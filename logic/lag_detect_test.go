package logic

import (
	"github.com/wfxiang08/cyutils/utils/atomic2"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"github.com/wfxiang08/db-sharding/conf"
)

import (
	"testing"
	"time"
)

// go test github.com/wfxiang08/db-sharding/logic -v -run "TestThrottleCheck$"
func TestThrottleCheck(t *testing.T) {
	config, err := conf.NewConfigWithFile("dbs.toml")
	if err != nil {
		log.ErrorErrorf(err, "NewConfigWithFile failed")
		return
	}

	StartThrottleCheck(config, "local", map[string]*atomic2.Bool{
		"localhost": &atomic2.Bool{},
	})

	for i := 0; i < 100; i++ {
		time.Sleep(time.Second)
	}

}
