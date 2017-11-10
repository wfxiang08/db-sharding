package logic

import (
	"conf"
	"github.com/fatih/color"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/wfxiang08/cyutils/utils/atomic2"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"models"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const (
	TotalShardNum         = 32
	BatchWriteCount       = 4000
	BatchReadCount        = 4000
	MaxRetryNum           = 10
	MaxBinlogDelaySeconds = 5
)

// 支持online将一个table拆分成为多个table
func ShardingProcess(originTable string, sourceDBAlias string, dbHelper models.DBHelper, selectedShardOnly bool,
	replicaServerId uint,
	eventOnly bool, batchOnly bool, dbConfigFile string,
	logPrefix string, dryRun bool, binlogInfo string) {

	// 1. 解析Log相关的配置
	if len(logPrefix) > 0 {
		f, err := log.NewRollingFile(logPrefix, 3)
		if err != nil {
			log.PanicErrorf(err, "open rolling log file failed: %s", logPrefix)
		} else {
			// 不能放在子函数中
			defer f.Close()
			log.StdLog = log.New(f, "")
		}
	}

	// 默认是Debug模式
	log.SetLevel(log.LEVEL_DEBUG)
	log.SetFlags(log.Flags() | log.Lshortfile)

	dbConfig, err := conf.NewConfigWithFile(dbConfigFile)
	if err != nil {
		log.ErrorErrorf(err, "NewConfigWithFile failed")
		return
	}

	var stopInput atomic2.Bool
	var pauseInput atomic2.Bool
	wg := &sync.WaitGroup{}

	// 1. 准备消费者
	shardingAppliers := buildAppliers(wg, dbHelper, dryRun, dbConfig)

	// 2. 设置数据源
	var startStreamingEvent chan bool

	// 如果不设置BatchOnly Mode, 那么启动binlog订阅
	if !batchOnly {
		if !eventOnly {
			// 在开启binlog模式下，如果只是eventOnly, 则不需要通过 startStreamingEvent 来同步
			startStreamingEvent = make(chan bool, 2) // 不要阻塞数据输入
		}

		BinlogShard(wg, originTable, sourceDBAlias, dbConfig, dbHelper, shardingAppliers, &stopInput, startStreamingEvent,
			replicaServerId, binlogInfo)

	}

	// 如果不支持同步binlog, 则启动Batch Shard流程
	if !eventOnly {
		BatchShard(wg, sourceDBAlias, dbConfig, dbHelper, shardingAppliers, batchOnly,
			&stopInput, &pauseInput, startStreamingEvent)

	}

	go func() {
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
				if !stopInput.Get() {
					log.Printf(color.MagentaString("Stop input recordings"))
					stopInput.Set(true)

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
	}()

	wg.Wait()
	log.Printf(color.MagentaString("Data sharding finished"))
}

func buildAppliers(wg *sync.WaitGroup, dbHelper models.DBHelper, dryRun bool, dbConfig *conf.DatabaseConfig) []*ShardingApplier {
	var err error
	// 1. 准备消费者
	shardingAppliers := make([]*ShardingApplier, TotalShardNum)
	for i := 0; i < TotalShardNum; i++ {
		shardingAppliers[i], err = NewShardingApplier(i, BatchWriteCount, dbConfig, dryRun, dbHelper.GetBuilder())
		if err != nil {
			log.PanicErrorf(err, "NewShardingApplier failed")
		}

		wg.Add(1)
		// 启动消费者进程
		go shardingAppliers[i].Run(wg)
	}
	return shardingAppliers
}
