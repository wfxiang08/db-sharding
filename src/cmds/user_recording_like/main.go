package main

import (
	"flag"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"logic"
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

	batchOnly  = flag.Bool("batch-only", false, "batch only") // 不处理binlog, 默认是先处理批处理数据，然后再考虑binlog
	eventOnly  = flag.Bool("event", false, "notrunk")         // 只处理binlog
	binlogInfo = flag.String("bin", "", "binlog position")
	cacheSize  = flag.Int64("batch-cache", 20000000, "batch process init cache size")
)

func main() {
	flag.Parse()

	originTable := "user_recording_like"
	sourceDBAlias := "final"

	cacheSizeInt := *cacheSize
	if *eventOnly {
		cacheSizeInt = 0
	}
	helper := NewDbHelperRecordingLike(originTable, cacheSizeInt)
	logic.ShardingProcess(originTable, sourceDBAlias, helper, false,
		*replicaServerId,
		*eventOnly, *batchOnly, *dbConfigFile,
		*logPrefix, *dryRun, *binlogInfo)
}
