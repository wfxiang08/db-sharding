package models

import "database/sql/driver"

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

// Value is a value that drivers must be able to handle.
// It is either nil or an instance of one of these types:
//
//   int64
//   float64
//   bool
//   []byte
//   string
//   time.Time
// WARNING: 参考: go-sql-driver/mysql中 interpolateParams的实现，其他类型的数据被统一处理为空了
//
func Nullable2Value(v driver.Valuer) driver.Value {
	result, _ := v.Value()
	return result
}
