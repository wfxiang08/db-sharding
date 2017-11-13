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

func BinlogShard(wg *sync.WaitGroup, originTable string, sourceDBAlias string, dbConfig *conf.DatabaseConfig,
	dbHelper models.DBHelper,
	shardingAppliers []*ShardingApplier,
	stopInput *atomic2.Bool,
	startStreamingEvent chan bool,
	replicaServerId uint, binlogInfo string) {

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
	sourceDB, hostname, port := dbConfig.GetDB(sourceDBAlias)

	sourceConfig := &mysql.ConnectionConfig{
		Key:  mysql.InstanceKey{Hostname: hostname, Port: port},
		User: dbConfig.User, Password: dbConfig.Password,
	}

	eventsStreamer = NewEventsStreamer(sourceConfig, sourceDB, MaxRetryNum, replicaServerId)

	if err := eventsStreamer.InitDBConnections(binlogFile, binlogPos); err != nil {
		log.PanicErrorf(err, "InitDBConnections failed")
	}

	eventsStreamer.AddListener(false, sourceDB, originTable, func(binlogEntry *binlog.BinlogEntry) error {

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
				shardingAppliers[shardingSQL.ShardingIndex].PushSQL(shardingSQL)
			}
		}
		return nil
	})

	// 等待streaming events
	if startStreamingEvent != nil {
		<-startStreamingEvent
	}

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
