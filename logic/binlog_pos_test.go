package logic

import (
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"github.com/wfxiang08/db-sharding/conf"
	"testing"
)

// go test github.com/wfxiang08/db-sharding/logic -v -run "TestBinlogCoordinates$"
func TestBinlogCoordinates(t *testing.T) {
	config, err := conf.NewConfigWithFile("dbs.toml")
	if err != nil {
		log.ErrorErrorf(err, "NewConfigWithFile failed")
		return
	}

	PrintBinlogPos([]string{"local"}, config)
}
