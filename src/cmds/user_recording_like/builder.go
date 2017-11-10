package main

import (
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"models"
)

type UserRecordingLikeBuild struct {
	models.SMHashShard
}

func NewUserRecordingLikeBuild(shardNum int) *UserRecordingLikeBuild {
	result := &UserRecordingLikeBuild{}
	result.ShardNum = shardNum
	return result
}

func (this *UserRecordingLikeBuild) getShardingIndex(args []interface{}) int {
	shard, err := this.FindForKey(args[kIndexUser])
	if err != nil {
		log.ErrorErrorf(err, "unexpected sharding error")
	}
	return shard
}

func (this *UserRecordingLikeBuild) Delete(where []interface{}) *models.ShardingSQL {
	return &models.ShardingSQL{
		ShardingIndex: this.getShardingIndex(where),
		SQL:           kSQLUserRecordingDel,
		Args:          []interface{}{where[kIndexUser], where[kIndexRecordingId]},
	}
}

func (this *UserRecordingLikeBuild) Update(args []interface{}, where []interface{}) *models.ShardingSQL {
	return &models.ShardingSQL{
		ShardingIndex: this.getShardingIndex(args),
		SQL:           kSQLUserRecordingUpdate,
		Args: []interface{}{
			args[kIndexUser], args[kIndexRecordingId], args[kIndexCreatedOn],
			where[kIndexUser], where[kIndexRecordingId],
		},
	}
}

func (this *UserRecordingLikeBuild) Insert(args []interface{}) *models.ShardingSQL {
	return &models.ShardingSQL{
		ShardingIndex: this.getShardingIndex(args),
		SQL:           kSQLUserRecordingInsert,
		Args: []interface{}{
			args[kIndexUser], args[kIndexRecordingId], args[kIndexCreatedOn],
		},
	}
}

func (this *UserRecordingLikeBuild) InsertIgnore(model interface{}) *models.ShardingSQL {
	m := model.(*UserRecordingLike)
	shardId, _ := this.FindForKey(m.UserId)
	return &models.ShardingSQL{
		ShardingIndex: shardId,
		SQL:           kSQLUserRecordingInsertIgnore,
		Args: []interface{}{
			m.UserId, m.RecordingId, m.CreatedOn,
		},
	}
}

func (this *UserRecordingLikeBuild) GetShardingIndex4Model(model interface{}) int {
	m := model.(*UserRecordingLike)
	shardId, _ := this.FindForKey(m.UserId)
	return shardId
}

func (this *UserRecordingLikeBuild) GetBatchInsertSegment() string {
	return `(?, ?, ?)`
}
