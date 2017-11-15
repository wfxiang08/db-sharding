package logic

import (
	"fmt"
	"github.com/fatih/color"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/wfxiang08/cyutils/utils/atomic2"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"github.com/wfxiang08/db-sharding/conf"
	"github.com/wfxiang08/db-sharding/models"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const (
	MaxRetryNum           = 10
	MaxBinlogDelaySeconds = 5
)

var (
	TotalShardNum                = 32 // 默认是32, 如果每个DB内的table被拆分，则为32 * replication(被拆分数)
	BatchInsertSleepMilliseconds = 20
	BatchReadCount               = 2000
	BatchWriteCount              = 2000
)

// 原始的Table(每次只考虑单个的db/table, 或者单台机器上的一类tables)
type OriginTable struct {
	TablePattern    string // comment or comment*
	DatabasePattern string // shard_0 or shard_*
	DbAlias         string
}

func ShardingSetupLog(logPrefix string) {
	// 1. 解析Log相关的配置
	if len(logPrefix) > 0 {
		f, err := log.NewRollingFile(logPrefix, 3)
		if err != nil {
			log.PanicErrorf(err, "open rolling log file failed: %s", logPrefix)
		} else {
			// 不能放在子函数中
			// defer f.Close()
			// 最终退出是，文件也会自动关闭
			log.StdLog = log.New(f, "")
		}
	}

	// 默认是Debug模式
	log.SetLevel(log.LEVEL_DEBUG)
	log.SetFlags(log.Flags() | log.Lshortfile)
}

func ShardingWaitingClose(batchOnly bool, pauseInput *atomic2.Bool, stopInput *atomic2.Bool, shardingAppliers ShardingAppliers) {
	// 接受停止信号
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGTERM)

	for true {
		sig := <-c
		if sig == syscall.SIGHUP {
			if batchOnly {
				// 中止拷贝数据, 重启拷贝数据
				pauseInput.Set(!pauseInput.Get())
				if pauseInput.Get() {
					log.Printf(color.MagentaString("Pause input...."))
				} else {
					log.Printf(color.MagentaString("Resume input...."))
				}
			} else {
				// 打印binlog的位置:

			}
		} else {
			// 停止输入
			if stopInput.CompareAndSwap(false, true) {
				log.Printf(color.MagentaString("Stop input recordings"))
				go func() {
					time.Sleep((MaxBinlogDelaySeconds + 1) * time.Second)
					// 关闭数据输入
					for i := 0; i < TotalShardNum; i++ {
						shardingAppliers[i].Close()
					}
				}()
			}
		}
	}
}

func BuildAppliers(wg *sync.WaitGroup, cacheSize int, dbHelper models.DBHelper, dryRun bool,
	dbConfig *conf.DatabaseConfig, pauseInput *atomic2.Bool) (ShardingAppliers, map[string]*atomic2.Bool) {
	return BuildBatchAppliersWithRepliction(wg, 1, cacheSize, dbHelper, dryRun, dbConfig)
}

func BuildBatchAppliersWithRepliction(wg *sync.WaitGroup, replication int, cacheSize int, dbHelper models.DBHelper,
	dryRun bool, dbConfig *conf.DatabaseConfig) (ShardingAppliers, map[string]*atomic2.Bool) {

	hostname2Pause := make(map[string]*atomic2.Bool)
	var err error
	// 1. 准备消费者
	shardingAppliers := make([]*ShardingApplier, TotalShardNum)
	for i := 0; i < TotalShardNum; i++ {

		dbIndex := i / replication

		_, hostname, _ := dbConfig.GetDB(fmt.Sprintf("shard%d", dbIndex))
		pauseInput, ok := hostname2Pause[hostname]
		if !ok {
			pauseInput = &atomic2.Bool{}
			hostname2Pause[hostname] = pauseInput
		}

		shardingAppliers[i], err = NewShardingApplier(dbIndex, BatchWriteCount, cacheSize, dbConfig, dryRun,
			dbHelper.GetBuilder(), pauseInput)
		if err != nil {
			log.PanicErrorf(err, "NewShardingApplier failed")
		}

		wg.Add(1)
		// 启动消费者进程
		go shardingAppliers[i].Run(wg)
	}
	return ShardingAppliers(shardingAppliers), hostname2Pause
}
