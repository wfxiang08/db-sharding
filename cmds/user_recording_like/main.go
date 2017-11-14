package main

import (
	"flag"
	"github.com/fatih/color"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/wfxiang08/cyutils/utils/atomic2"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"github.com/wfxiang08/db-sharding/conf"
	"github.com/wfxiang08/db-sharding/logic"
	"github.com/wfxiang08/db-sharding/media_utils"
	"sync"
)

// 正确性:
// 1. R表示某一条Record, E表示events
// 2. 假设R读取时的状态和 E[i]对应
//    R和E[i]从两条不同的通路过来，因此不能保证时序
//    2.1 R < E[i], 则在R上会执行E[i-x], ..., E[i], ... 等操作，这个应该是安全的（一般情况下key是不会主动变化的, 或者在有限的时间窗口内可以保证）
//    	  同一个key可能被重复执行Events[i-x], ..., E[i-1], 这个确定是安全的
//    2.2 R > E[i]，则看到数据可能过期，并且错过不少events[风险]
//        R
//        E[i] E[i+1] E[i+x]
//        这个events的窗口期有多大呢? SELECT * FROM xxxx LIMIT 2000, 可能在1s左右
//
// ghost如何保证数据的一致性呢?
// insert into ... (select * from xxxx)
// R在读的这一刻就立马被写入新的表中，在一个事务内执行；不存在R过期，并且错误events的情况
//
// 批量拷贝 + Event模式分开处理
//
var (
	dbConfigFile    = flag.String("conf", "", "hosts config file")
	replicaServerId = flag.Uint("replica-server-id", 99900, "server id used by gh-ost process. Default: 99900")
	logPrefix       = flag.String("log", "", "log file prefix")
	dryRun          = flag.Bool("dry", false, "dry run")

	batchMode  = flag.Bool("batch-model", false, "batch mode or event mode") // 不处理binlog, 默认是先处理批处理数据，然后再考虑binlog
	binlogInfo = flag.String("bin", "", "binlog position")

	// 根据数据规模来选择
	// 如果数据量太大，可以考虑临时找一个大内存的云主机，完事之后再退
	cacheSize = flag.Int64("batch-cache", 20000000, "batch process init cache size")

	metaDir = flag.String("meta-dir", "", "binlog meta dir")
)

//
// go build github.com/wfxiang08/db-sharding/cmds/user_recording_like
//
func main() {
	flag.Parse()

	// 1. 设置日志
	logic.ShardingSetupLog(*logPrefix)

	originTableName := "user_recording_like"
	originTable := &logic.OriginTable{
		TablePattern:    originTableName,
		DatabasePattern: "*",
		DbAlias:         "final",
	}

	// 2. 解析config
	dbConfig, err := conf.NewConfigWithFile(*dbConfigFile)
	if err != nil {
		log.ErrorErrorf(err, "NewConfigWithFile failed")
		return
	}

	var stopInput atomic2.Bool
	var pauseInput atomic2.Bool
	wg := &sync.WaitGroup{}

	// 3. 创建DbHelper
	cacheSizeInt := *cacheSize
	if !*batchMode {
		cacheSizeInt = 0
	}
	dbHelper := NewDbHelperRecordingLike(cacheSizeInt, true)

	// 4. 准备消费者
	shardingAppliers := logic.BuildAppliers(wg, logic.BatchReadCount*10, dbHelper, *dryRun, dbConfig)

	// 5. 准备退出
	go logic.ShardingWaitingClose(*batchMode, &pauseInput, &stopInput, shardingAppliers)

	if *batchMode {
		// 一尺处理一个Table, 可以并发地处理多个Table
		logic.BatchReadDB(wg, originTableName, originTable.DbAlias, dbConfig, dbHelper,
			logic.ShardingAppliers(shardingAppliers),
			&stopInput, &pauseInput)

		// 批量Apply数据
		logic.ReorderAndApply(dbHelper, shardingAppliers)
		log.Printf(color.MagentaString("Data sharding finished"))

	} else {
		if len(*metaDir) == 0 || !media_utils.IsDir(*metaDir) {
			log.Panicf("Invalid meta-dir")
		}
		// 只处理binlog(一次只处理一台机器)
		logic.BinlogShard4SingleMachine(wg,
			originTable, dbConfig,
			dbHelper, shardingAppliers, &stopInput,
			*replicaServerId, *binlogInfo, *metaDir)

	}

	// 等外完成
	wg.Wait()
}
