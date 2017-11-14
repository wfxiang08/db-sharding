package logic

import (
	"github.com/fatih/color"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/wfxiang08/cyutils/utils/atomic2"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"github.com/wfxiang08/db-sharding/binlog"
	"github.com/wfxiang08/db-sharding/conf"
	"github.com/wfxiang08/db-sharding/models"
	"github.com/wfxiang08/db-sharding/mysql"
	"strconv"
	"strings"
	"sync"
)

//
// sourceConfig 代表一台mysql db
// 上面可能有多个db
//
func BinlogShard4SingleMachine(wg *sync.WaitGroup, originTable *OriginTable, dbConfig *conf.DatabaseConfig,
	dbHelper models.DBHelper,
	shardingAppliers ShardingAppliers,
	stopInput *atomic2.Bool,
	replicaServerId uint, binlogInfo string) {

	// binlog一次只处理一台机器
	_, hostname, port := dbConfig.GetDB(originTable.DbAlias)
	sourceConfig := &mysql.ConnectionConfig{
		Key:  mysql.InstanceKey{Hostname: hostname, Port: port},
		User: dbConfig.User, Password: dbConfig.Password,
	}

	binlogFile := ""
	binlogPos := int64(0)
	if len(binlogInfo) > 0 {
		// 格式: mysql-bin-changelog.024709:19464691
		items := strings.Split(binlogInfo, ":")
		binlogFile = items[0]
		binlogPos, _ = strconv.ParseInt(items[1], 10, 64)
	}

	wg.Add(1)
	// 初始化stream
	defer wg.Done()

	var eventsStreamer *EventsStreamer

	eventsStreamer = NewEventsStreamer(sourceConfig, "", MaxRetryNum, replicaServerId)

	if err := eventsStreamer.InitDBConnections(binlogFile, binlogPos); err != nil {
		log.PanicErrorf(err, "InitDBConnections failed")
	}

	eventsStreamer.AddListener(false, originTable.DatabasePattern, originTable.TablePattern, func(binlogEntry *binlog.BinlogEntry) error {

		event := binlogEntry.DmlEvent

		var shardingSQL *models.ShardingSQL
		// 将各种DML操作转换成为SQL
		switch event.DML {
		case binlog.InsertDML:
			shardingSQL = dbHelper.GetBuilder().Insert(event.NewColumnValues.AbstractValues())
		case binlog.UpdateDML:
			shardingSQL = dbHelper.GetBuilder().Update(event.NewColumnValues.AbstractValues(), event.WhereColumnValues.AbstractValues())
		case binlog.DeleteDML:
			shardingSQL = dbHelper.GetBuilder().Delete(event.WhereColumnValues.AbstractValues())
		}

		// 分配工作
		if shardingSQL != nil {
			log.Printf(color.MagentaString("Binlog Entry to shard%02d")+": %s", shardingSQL.ShardingIndex, shardingSQL.String())
			if dbHelper.ShardFilter(shardingSQL.ShardingIndex) {
				shardingAppliers.PushSQL(shardingSQL)
			}
		}
		return nil
	})

	log.Debugf("Beginning streaming")
	err := eventsStreamer.StreamEvents(func() bool {
		return stopInput.Get()
	})

	// 出错，就直接PanicAbort
	if err != nil {
		log.Panicf("Streaming events")
	} else {
		log.Debugf("Done streaming")
	}
}
