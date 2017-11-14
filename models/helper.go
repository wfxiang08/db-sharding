package models

import "github.com/jinzhu/gorm"

//
// 1. 实现db的批量读，shard过滤，shard内数据重新按照不同的key进行排序等问题
//
type DBHelper interface {
	GetBuilder() ModelBuilder
	ShardFilter(shardIndex int) bool
	BatchRead(db *gorm.DB, tableName string, sourceDBAlias string) (*gorm.DB, int)
	BatchMerge(sqlApplier SqlApplier, tableName string, sourceDBAlias string)

	NeedReOrder() bool

	PrintSummary()
	ShardSort(shard int)
	GetShardItem(shard int, index int, clear bool) interface{}
	ClearShard(shard int)
	GetShardLen(shard int) int
}
