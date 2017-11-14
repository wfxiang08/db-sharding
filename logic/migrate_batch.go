package logic

import (
	"github.com/fatih/color"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/wfxiang08/cyutils/utils"
	"github.com/wfxiang08/cyutils/utils/atomic2"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"github.com/wfxiang08/db-sharding/conf"
	"github.com/wfxiang08/db-sharding/models"
	"sync"
	"time"
)

// 实现原理:
// 1. 按照某个unique-key, 一般是id, 将数据全部读取到内存中，并做好sharding处理
// 2. 数据读取完毕之后，按照新的key进行排序;
//    如果没有按照新key排序的需求，则可以在读取的过程中就可以开始批量执行sharding的sql的操作(insert ignore)
// 3. 排序之后，再有序地将数据插入到各个shard中
//    MySQL中给数据按照primary-key的顺序插入效率最高，同时也需要考虑网络的round-trip, 因此最有效的方法就是:
//    1. 一次插入N(4000左右), 通过statement生成固定的SQL, 然后args一口气传递给mysql;
//    2. 不便于合并的请求，可以通过Transaction减少mysql端的io
//
func BatchReadDB(wg *sync.WaitGroup, tableName string, sourceDBAlias string, dbConfig *conf.DatabaseConfig,
	dbHelper models.DBHelper, shardingAppliers ShardingAppliers,
	stopInput *atomic2.Bool, pauseInput *atomic2.Bool) {

	wg.Add(1)
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

		// 暂停，直到状态改变
		for pauseInput.Get() {
			log.Printf(color.BlueString("Pause") + ", sleep 1 second")
			time.Sleep(time.Second)
		}

		if stopInput.Get() {
			break
		}

		recordCount := 0
		t0 := time.Now()
		for i := 0; i < MaxRetryNum; i++ {
			dbInfo, count := dbHelper.BatchRead(db, tableName, sourceDBAlias)

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
			dbHelper.BatchMerge(sourceDBAlias, shardingAppliers)

			totalRowsProcessed += recordCount
			t1 := time.Now()
			time.Sleep(time.Microsecond * 10)

			log.Printf(color.GreenString("Rows Processed - %s")+": %d, Elapsed: %.3fms, Total elapsed: %ds",
				sourceDBAlias,
				totalRowsProcessed, utils.ElapsedMillSeconds(t0, t1),
				t1.Unix()-start.Unix())
		}
	}

	log.Printf("Block Read events finished")

}

func ReorderAndApply(dbHelper models.DBHelper, shardingAppliers []*ShardingApplier) {
	dbHelper.PrintSummary()

	if dbHelper.NeedReOrder() {
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
				applier.batchInsertMode.Set(true)

				for j := 0; j < shardLen; j++ {
					if j%10000 == 0 {
						log.Printf(color.GreenString("Sharding %d")+" insert progress: %d/%d", shardIndex, j, shardLen)
					}

					sql := dbHelper.GetBuilder().InsertIgnore(dbHelper.GetShardItem(shardIndex, j, true))
					applier.PushSQL(sql)

				}

				dbHelper.ClearShard(shardIndex)

				// 否则由手动关闭(不停订阅binlog)
				shardingAppliers[shardIndex].Close()

				log.Printf(color.CyanString("Sharding %d")+" finished insert ignore", shardIndex)

			}(i)
		}
		shardingTransferWg.Wait()
	}
}
