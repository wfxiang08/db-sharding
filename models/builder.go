package models

//
// 1. 实现model(interface{})到SQL转换
// 2. 实现binlog的args, where到SQL转换
//
type ModelBuilder interface {
	// 将binlog中的delete event转换成为SQL
	Delete(where []interface{}) *ShardingSQL
	// 将binlog中的update event转换成为SQL
	Update(args []interface{}, where []interface{}) *ShardingSQL
	// 将binlog中的insert event转换成为SQL
	Insert(args []interface{}) *ShardingSQL
	// 将批量读取到的model转换成为sql
	InsertIgnore(model interface{}) *ShardingSQL
	GetShardingIndex4Model(model interface{}) int
	GetBatchInsertSegment() string
}
