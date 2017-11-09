package models

import "github.com/jinzhu/gorm"

type ModelBuilder interface {
	Delete(where []interface{}) *ShardingSQL
	Update(args []interface{}, where []interface{}) *ShardingSQL
	Insert(args []interface{}) *ShardingSQL
	InsertIgnore(model interface{}) *ShardingSQL
	GetShardingIndex4Model(model interface{}) int
	GetBatchInsertSegment() string
}

type DBHelper interface {
	GetBuilder() ModelBuilder
	ShardFilter(shardIndex int) bool
	BatchRead(db *gorm.DB) (*gorm.DB, int)
	BatchMerge()
	PrintSummary()
	ShardSort(shard int)
	GetShardItem(shard int, index int, clear bool) interface{}
	ClearShard(shard int)
	GetShardLen(shard int) int
}
