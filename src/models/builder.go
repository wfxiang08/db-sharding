package models

//
// 1. 实现model(interface{})到SQL转换
// 2. 实现binlog的args, where到SQL转换
//
type ModelBuilder interface {
	Delete(where []interface{}) *ShardingSQL
	Update(args []interface{}, where []interface{}) *ShardingSQL
	Insert(args []interface{}) *ShardingSQL
	InsertIgnore(model interface{}) *ShardingSQL
	GetShardingIndex4Model(model interface{}) int
	GetBatchInsertSegment() string
}
