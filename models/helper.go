package models

import "github.com/jinzhu/gorm"

//
// 1. 实现db的批量读，shard过滤，shard内数据重新按照不同的key进行排序等问题
//
type DBHelper interface {
	GetBuilder() ModelBuilder
	ShardFilter(shardIndex int) bool

	// 批量处理
	BatchProcess(db *gorm.DB, tableName string, sourceDBAlias string, sqlApplier SqlApplier) (*gorm.DB, int)

	NeedReOrder() bool

	PrintSummary()
	ShardSort(shard int)
	GetShardItem(shard int, index int, clear bool) interface{}
	ClearShard(shard int)
	GetShardLen(shard int) int
}
