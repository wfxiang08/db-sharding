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
	dbName string
	lag    time.Duration

	masterDB *db_sql.DB
	slaveDB  *db_sql.DB

	pauseInput *atomic2.Bool
}

func StartThrottleCheck(dbConfig *conf.DatabaseConfig, masterAlias string, host2PauseInput map[string]*atomic2.Bool) {
	masterAliases := strings.Split(masterAlias, ";")
	// 获取master的配置和salve的配置
	for _, masterAlias := range masterAliases {
		dbName, hostname, _ := dbConfig.GetDB(masterAlias)
		pauseInput, ok := host2PauseInput[hostname]
		if !ok {
			continue
		}

		master := dbConfig.AliasToConnectionConfig(masterAlias)
		slave := master.Duplicate()

		slave.Key.Hostname = dbConfig.Master2Slave[master.Key.Hostname]

		result := &ThrottlerNode{
			lag:        0,
			master:     master,
			slave:      slave,
			dbName:     dbName,
			pauseInput: pauseInput,
		}
		result.init()
		go result.updateHeartBeat()
		go result.collectControlReplicasLag()
	}
}

func (this *ThrottlerNode) init() {
	// 获取master db
	dbUri := this.master.GetDBUri(this.dbName)
	db, _, err := sqlutils.GetDB(dbUri)
	if err != nil {
		log.PanicErrorf(err, "Get Master Db failed")
	}
	this.masterDB = db

	// 确保LagTableName存在
	query := fmt.Sprintf(`Create Table IF NOT EXISTS %s.%s (
			id int,
            value bigint
			primary key(id),
		)`, sql.EscapeName(this.dbName), sql.EscapeName(LagTableName),
	)

	_, err = this.masterDB.Exec(query)
	if err != nil {
		log.PanicErrorf(err, "Create Status table failed")
		return
	}

	// 获取slave db
	dbUri = this.slave.GetDBUri(this.dbName)
	db, _, err = sqlutils.GetDB(dbUri)
	if err != nil {
		log.PanicErrorf(err, "Get Master Db failed")
	}
	this.slaveDB = db
}

func (this *ThrottlerNode) updateHeartBeat() {

	ticker := time.NewTicker(time.Millisecond * 500)
	for _ = range ticker.C {
		query := fmt.Sprintf(`replace into %s.%s (id, value) values (1, %d)`, sql.EscapeName(this.dbName), sql.EscapeName(LagTableName), time.Now().UnixNano())
		_, err := this.masterDB.Exec(query)
		if err != nil {
			log.ErrorErrorf(err, "Update Master Db failed")
		}
	}
}

func (this *ThrottlerNode) collectControlReplicasLag() {
	ticker := time.NewTicker(time.Millisecond * 500)
	replicationLagQuery := fmt.Sprintf(`select value from %s.%s where id = 1`,
		sql.EscapeName(this.dbName),
		sql.EscapeName(LagTableName),
	)
	var heartbeatValue int64
	for _ = range ticker.C {
		if err := this.slaveDB.QueryRow(replicationLagQuery).Scan(&heartbeatValue); err != nil {
			log.ErrorErrorf(err, "replicationLagQuery failed")
			continue
		}
		t := time.Now().UnixNano() - heartbeatValue
		if t > 0 {
			this.lag = time.Duration(t)

			this.pauseInput.Set(this.lag > MaxLagInMillseconds)
		}
	}
}
