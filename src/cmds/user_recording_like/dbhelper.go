package main

import (
	"github.com/jinzhu/gorm"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"logic"
	"models"
	"sort"
)

type DbHelperRecordingLike struct {
	batchModels   []*UserRecordingLike
	shardedModels [][]*UserRecordingLike
	builder       models.ModelBuilder
	originTable   string
	lastId        int64
}

// 目前每个Helper需要定制的内容:
// 1. 构造函数
// 2. ShardFilter
// 3. BatchRead(如果是以id为主键，则也可以直接拷贝)
//
func NewDbHelperRecordingLike(originTable string, cacheSize int64) *DbHelperRecordingLike {

	result := &DbHelperRecordingLike{
		builder:       NewUserRecordingLikeBuild(logic.TotalShardNum),
		lastId:        0,
		originTable:   originTable,
		shardedModels: make([][]*UserRecordingLike, logic.TotalShardNum),
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
func (this *DbHelperRecordingLike) BatchRead(db *gorm.DB) (*gorm.DB, int) {
	this.batchModels = nil
	dbInfo := db.Table(this.originTable).
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

func (this *DbHelperRecordingLike) BatchMerge() {
	for _, model := range this.batchModels {
		shardIndex := this.builder.GetShardingIndex4Model(model)
		if this.ShardFilter(shardIndex) {
			// 机器人，数据直接扔掉
			this.shardedModels[shardIndex] = append(this.shardedModels[shardIndex], model)
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
