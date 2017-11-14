package main

import (
	"github.com/jinzhu/gorm"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"github.com/wfxiang08/db-sharding/logic"
	"github.com/wfxiang08/db-sharding/models"
	"sort"
)

type DbHelperRecordingLike struct {
	batchModels   []*UserRecordingLike
	shardedModels [][]*UserRecordingLike
	builder       models.ModelBuilder
	needReOrder   bool
	lastId        int64
}

// 目前每个Helper需要定制的内容:
// 1. 构造函数
// 2. ShardFilter
// 3. BatchRead(如果是以id为主键，则也可以直接拷贝)
//
func NewDbHelperRecordingLike(cacheSize int64, needReOrder bool) *DbHelperRecordingLike {

	result := &DbHelperRecordingLike{
		builder:       NewUserRecordingLikeBuild(logic.TotalShardNum),
		lastId:        0,
		shardedModels: make([][]*UserRecordingLike, logic.TotalShardNum),
		needReOrder:   needReOrder,
	}

	for i := 0; i < logic.TotalShardNum; i++ {
		result.shardedModels[i] = make([]*UserRecordingLike, 0, cacheSize)
	}

	return result
}

func (this *DbHelperRecordingLike) ShardFilter(shardIndex int) bool {
	if shardIndex != 5 {
		return true
	} else {
		return false
	}
}
func (this *DbHelperRecordingLike) BatchRead(db *gorm.DB, tableName string, dbAlias string) (*gorm.DB, int) {
	this.batchModels = nil
	dbInfo := db.Table(tableName).
		Where("id > ?", this.lastId).
		Order("id ASC").Limit(logic.BatchReadCount).Find(&this.batchModels)
	return dbInfo, len(this.batchModels)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 下面的代码可以拷贝
func (this *DbHelperRecordingLike) GetBuilder() models.ModelBuilder {
	return this.builder
}

// 需要重新排序
func (this *DbHelperRecordingLike) NeedReOrder() bool {
	return this.needReOrder
}

func (this *DbHelperRecordingLike) BatchMerge(dbAlias string, sqlApplier models.SqlApplier) {
	for _, model := range this.batchModels {
		shardIndex := this.builder.GetShardingIndex4Model(model)
		if this.ShardFilter(shardIndex) {
			// 机器人，数据直接扔掉
			if this.NeedReOrder() {
				// 先buffer, 在排序
				this.shardedModels[shardIndex] = append(this.shardedModels[shardIndex], model)
			} else {
				// 直接Apply
				sqlApplier.PushSQL(this.builder.InsertIgnore(model))
			}
		}
	}

	// 更新遍历状态
	lastItem := this.batchModels[len(this.batchModels)-1]
	this.lastId = lastItem.Id
}

func (this *DbHelperRecordingLike) ShardSort(shard int) {
	sort.Sort(UserRecordingLikes(this.shardedModels[shard]))
}

func (this *DbHelperRecordingLike) PrintSummary() {
	for i := 0; i < logic.TotalShardNum; i++ {
		log.Printf("SHARDXX %d, total size: %d", i, len(this.shardedModels[i]))
	}
}

func (this *DbHelperRecordingLike) GetShardItem(shard int, index int, clear bool) interface{} {
	result := this.shardedModels[shard][index]
	if clear {
		this.shardedModels[shard][index] = nil
	}
	return result
}

func (this *DbHelperRecordingLike) ClearShard(shard int) {
	this.shardedModels[shard] = nil
}

func (this *DbHelperRecordingLike) GetShardLen(shard int) int {
	return len(this.shardedModels[shard])
}
