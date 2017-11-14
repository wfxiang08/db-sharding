package logic

import (
	"database/sql"
	"github.com/fatih/color"
	"github.com/outbrain/golib/sqlutils"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"github.com/wfxiang08/db-sharding/conf"
)

func PrintBinlogPos(dbAliases []string, dbConfig *conf.DatabaseConfig) {
	for _, dbAliase := range dbAliases {
		uri := dbConfig.GetDBUri(dbAliase)
		if db, _, err := sqlutils.GetDB(uri); err != nil {
			log.ErrorErrorf(err, "GetBinlog Pos failed: %s --> %s", dbAliase, uri)
			continue
		} else {
			readCurrentBinlogCoordinates(db, dbAliase, dbConfig)
		}
	}

}

func readCurrentBinlogCoordinates(db *sql.DB, dbAliase string, dbConfig *conf.DatabaseConfig) {
	query := `show master status`
	sqlutils.QueryRowsMap(db, query, func(m sqlutils.RowMap) error {
		_, hostname, _ := dbConfig.GetDB(dbAliase)
		log.Printf(color.MagentaString("Binlog Info: %s ==> %s:%d"), hostname, m.GetString("File"), m.GetInt64("Position"))
		return nil
	})
}
