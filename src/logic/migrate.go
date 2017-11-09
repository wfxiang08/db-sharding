package logic

import (
	"binlog"
	"conf"
	"github.com/fatih/color"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/wfxiang08/cyutils/utils"
	"github.com/wfxiang08/cyutils/utils/atomic2"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"models"
	"mysql"
	"os"
	"os/signal"
	"strconv"
	"strings"
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

// 具体实现
func ShardingProcess(originTable string, sourceDBAlias string, dbHelper models.DBHelper, selectedShardOnly bool,
	replicaServerId uint,
	eventOnly bool, dbConfigFile string,
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
	var wg sync.WaitGroup
	wg.Add(2)

	// 1. 准备消费者
	shardingAppliers := make([]*ShardingApplier, TotalShardNum)
	for i := 0; i < TotalShardNum; i++ {
		shardingAppliers[i], err = NewShardingApplier(i, BatchWriteCount, dbConfig, dryRun, dbHelper.GetBuilder())
		if err != nil {
			log.PanicErrorf(err, "NewShardingApplier failed")
		}

		wg.Add(1)
		// 启动消费者进程
		go shardingAppliers[i].Run(&wg)
	}

	// 3. 设置数据源
	sourceDB, hostname, port := dbConfig.GetDB(sourceDBAlias)

	startStreamingEvent := make(chan bool, 2) // 不要阻塞数据输入

	binlogFile := ""
	binlogPos := int64(0)

	if len(binlogInfo) > 0 {
		// mysql-bin-changelog.024709:19464691
		items := strings.Split(binlogInfo, ":")
		binlogFile = items[0]
		binlogPos, _ = strconv.ParseInt(items[1], 10, 64)
	}

	if selectedShardOnly {
		// 不激活EventsStreamer
		wg.Done()
	} else {
		var eventsStreamer *EventsStreamer
		// 初始化stream
		go func() {
			defer wg.Done()

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
			<-startStreamingEvent

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
		}()
	}

	// 异步遍历Events
	if !eventOnly {
		go func() {
			defer wg.Done()

			db, err := gorm.Open("mysql", dbConfig.GetDBUri(sourceDBAlias))
			if err != nil {
				log.ErrorErrorf(err, "Open database failed")
				return
			}
			db.DB().SetConnMaxLifetime(time.Hour * 4)
			db.DB().SetMaxOpenConns(2) // 设置最大的连接数（防止异常情况干死数据库)
			db.DB().SetMaxIdleConns(2)

			start := time.Now()
			totalRowsProcessed := int(0)
			for !stopInput.Get() {

				for pauseInput.Get() {
					log.Printf(color.BlueString("Pause") + ", sleep 1 second")
					time.Sleep(time.Second)
					break
				}
				if stopInput.Get() {
					break
				}

				recordCount := 0
				t0 := time.Now()
				for i := 0; i < MaxRetryNum; i++ {

					dbInfo, count := dbHelper.BatchRead(db)

					if dbInfo.Error != nil && i != MaxRetryNum-1 {
						log.ErrorErrorf(dbInfo.Error, "db record read failed")
						time.Sleep(1 * time.Second)
					} else if dbInfo.Error != nil {
						log.PanicErrorf(dbInfo.Error, "db record read failed")
					} else {
						// 正常返回
						recordCount = count
						break
					}
				}

				// 遍历
				if recordCount == 0 {
					break
				} else {
					dbHelper.BatchMerge()

					totalRowsProcessed += recordCount
					t1 := time.Now()
					time.Sleep(time.Microsecond * 10)

					log.Printf(color.GreenString("Rows Processed")+": %d, Elapsed: %.3fms, Total elapsed: %ds",
						totalRowsProcessed, utils.ElapsedMillSeconds(t0, t1),
						t1.Unix()-start.Unix())
				}
			}
			dbHelper.PrintSummary()

			// 数据读取完毕(按照升序排列)
			var shardingTransferWg sync.WaitGroup
			for i := 0; i < TotalShardNum; i++ {
				shardingTransferWg.Add(1)
				go func(shardIndex int) {
					defer shardingTransferWg.Done()

					// 排序:
					dbHelper.ShardSort(shardIndex)

					shardLen := dbHelper.GetShardLen(shardIndex)
					log.Printf("SHARDXX %d, total size: %d", shardIndex, shardLen)
					applier := shardingAppliers[shardIndex]
					applier.InsertIgnoreMode.Set(true)

					for j := 0; j < shardLen; j++ {
						if j%10000 == 0 {
							log.Printf(color.GreenString("Sharding %d")+" insert progress: %d/%d", shardIndex, j, shardLen)
						}

						sql := dbHelper.GetBuilder().InsertIgnore(dbHelper.GetShardItem(shardIndex, j, true))
						applier.PushSQL(sql)

					}

					dbHelper.ClearShard(shardIndex)

					// 如果没有启动binlog, 则可以关闭
					if selectedShardOnly {
						// 否则由手动关闭(不停订阅binlog)
						shardingAppliers[shardIndex].Close()
					} else {
						// 放弃批量执行优化，因为SQL可能开始有各种类型的操作，例如： delete, update, insert..., 而且剩下的工作量也不大了
						applier.InsertIgnoreMode.Set(false)
					}

					log.Printf(color.CyanString("Sharding %d")+" finished insert ignore", shardIndex)

				}(i)
			}
			shardingTransferWg.Wait()
			log.Printf("Block Transfer events finished")
			// 转移完毕之后，开始Streaming Events
			startStreamingEvent <- true
		}()
	} else {
		// 直接启动Streaming
		startStreamingEvent <- true
	}

	go func() {
		// 接受停止信号
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGHUP, syscall.SIGTERM)

		for true {
			sig := <-c
			if sig == syscall.SIGHUP {
				pauseInput.Set(!pauseInput.Get())
				if pauseInput.Get() {
					log.Printf(color.MagentaString("Pause input...."))
				} else {
					log.Printf(color.MagentaString("Resume input...."))
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
