package logic

import (
	db_sql "database/sql"
	"fmt"
	"github.com/outbrain/golib/sqlutils"
	"github.com/wfxiang08/cyutils/utils/atomic2"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"github.com/wfxiang08/db-sharding/conf"
	"github.com/wfxiang08/db-sharding/mysql"
	"github.com/wfxiang08/db-sharding/sql"
	"strings"
	"time"
)

const (
	LagTableName        = "sharding_heartbeat"
	MaxLagInMillseconds = time.Millisecond * 1500
)

type ThrottlerNode struct {
	master *mysql.ConnectionConfig
	slave  *mysql.ConnectionConfig
	DbName string
	Lag    time.Duration

	masterDB *db_sql.DB
	slaveDB  *db_sql.DB

	pauseInput *atomic2.Bool
}

func StartThrottleCheck(dbConfig *conf.DatabaseConfig, masterAlias string, host2PauseInput map[string]*atomic2.Bool) []*ThrottlerNode {
	masterAliases := strings.Split(masterAlias, ";")
	// 获取master的配置和salve的配置
	var results []*ThrottlerNode
	for _, masterAlias := range masterAliases {
		dbName, hostname, _ := dbConfig.GetDB(masterAlias)
		pauseInput, ok := host2PauseInput[hostname]
		if !ok {
			log.Printf("Hostname skipped: %s", hostname)
			continue
		}

		master := dbConfig.AliasToConnectionConfig(masterAlias)
		slave := master.Duplicate()

		slave.Key.Hostname = dbConfig.Master2Slave[master.Key.Hostname]

		result := &ThrottlerNode{
			Lag:        0,
			master:     master,
			slave:      slave,
			DbName:     dbName,
			pauseInput: pauseInput,
		}
		result.init()
		// 更新db
		go result.updateHeartBeat()
		// 检查slave的变化
		go result.collectControlReplicasLag()

		results = append(results, result)
	}
	return results
}

func (this *ThrottlerNode) init() {
	// 获取master db
	dbUri := this.master.GetDBUri(this.DbName)
	db, _, err := sqlutils.GetDB(dbUri)
	if err != nil {
		log.PanicErrorf(err, "Get Master Db failed")
	}
	this.masterDB = db

	// 确保LagTableName存在
	query := fmt.Sprintf(`CREATE Table IF NOT EXISTS %s.%s (
			id int,
            value bigint,
			primary key(id)
		)`, sql.EscapeName(this.DbName), sql.EscapeName(LagTableName),
	)

	_, err = this.masterDB.Exec(query)
	if err != nil {
		log.PanicErrorf(err, "Create Status table failed")
		return
	}

	// 获取slave db
	dbUri = this.slave.GetDBUri(this.DbName)
	db, _, err = sqlutils.GetDB(dbUri)
	if err != nil {
		log.PanicErrorf(err, "Get Master Db failed")
	}
	this.slaveDB = db
}

func (this *ThrottlerNode) updateHeartBeat() {

	ticker := time.NewTicker(time.Millisecond * 500)
	for _ = range ticker.C {
		//log.Printf("updateHeartBeat")
		query := fmt.Sprintf(`replace into %s.%s (id, value) values (1, %d)`, sql.EscapeName(this.DbName), sql.EscapeName(LagTableName), time.Now().UnixNano())
		_, err := this.masterDB.Exec(query)
		if err != nil {
			log.ErrorErrorf(err, "Update Master Db failed")
		}
	}
}

func (this *ThrottlerNode) collectControlReplicasLag() {
	ticker := time.NewTicker(time.Millisecond * 500)
	replicationLagQuery := fmt.Sprintf(`select value from %s.%s where id = 1`,
		sql.EscapeName(this.DbName),
		sql.EscapeName(LagTableName),
	)
	time.Sleep(time.Second) // 保证table创建，数据ok

	var heartbeatValue int64
	for _ = range ticker.C {
		//log.Printf("collectControlReplicasLag")

		if err := this.slaveDB.QueryRow(replicationLagQuery).Scan(&heartbeatValue); err != nil {
			// 第一次可能没有数据
			log.ErrorErrorf(err, "replicationLagQuery failed")
			continue
		}
		t := time.Now().UnixNano() - heartbeatValue
		if t > 0 {
			this.Lag = time.Duration(t)

			paused := this.Lag > MaxLagInMillseconds
			if this.pauseInput.Get() != paused {
				log.Printf("%s paused: %v", this.master.Key.Hostname, paused)
				this.pauseInput.Set(paused)
			}
		}
	}
}
