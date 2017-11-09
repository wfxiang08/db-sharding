package models

import (
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
)

const (
	kSQLUserRecordingDel    = `delete from user_recording_like where user_id=? and recording_id=?`
	kSQLUserRecordingUpdate = `update user_recording_like SET user_id=?, recording_id=?, created_on = ? where user_id=? and recording_id=?`
	kSQLUserRecordingInsert = `replace into user_recording_like (user_id, recording_id, created_on) values (?, ?, ?)`

	kSQLUserRecordingInsertIgnore = `insert ignore into user_recording_like (user_id, recording_id, created_on) values (?, ?, ?)`

	kIndexUser        = 2
	kIndexRecordingId = 3
	kIndexCreatedOn   = 1
)

type UserRecordingLike struct {
	Id          int64
	UserId      int64 // 2
	RecordingId int64 // 3
	CreatedOn   int32 // 1
}

type UserRecordingLikes []*UserRecordingLike

func (p UserRecordingLikes) Len() int {
	return len(p)
}

// 按照 user_id, recording_id的升序排列
func (p UserRecordingLikes) Less(i, j int) bool {
	return p[i].UserId < p[j].UserId || (p[i].UserId == p[j].UserId && p[i].RecordingId < p[j].RecordingId)
}
func (p UserRecordingLikes) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

type UserRecordingLikeBuild struct {
	SMHashShard
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

func (this *UserRecordingLikeBuild) Delete(where []interface{}) *ShardingSQL {
	return &ShardingSQL{
		ShardingIndex: this.getShardingIndex(where),
		SQL:           kSQLUserRecordingDel,
		Args:          []interface{}{where[kIndexUser], where[kIndexRecordingId]},
	}
}

func (this *UserRecordingLikeBuild) Update(args []interface{}, where []interface{}) *ShardingSQL {
	return &ShardingSQL{
		ShardingIndex: this.getShardingIndex(args),
		SQL:           kSQLUserRecordingUpdate,
		Args: []interface{}{
			args[kIndexUser], args[kIndexRecordingId], args[kIndexCreatedOn],
			where[kIndexUser], where[kIndexRecordingId],
		},
	}
}

func (this *UserRecordingLikeBuild) Insert(args []interface{}) *ShardingSQL {
	return &ShardingSQL{
		ShardingIndex: this.getShardingIndex(args),
		SQL:           kSQLUserRecordingInsert,
		Args: []interface{}{
			args[kIndexUser], args[kIndexRecordingId], args[kIndexCreatedOn],
		},
	}
}

func (this *UserRecordingLikeBuild) InsertIgnore(model interface{}) *ShardingSQL {
	m := model.(*UserRecordingLike)
	shardId, _ := this.FindForKey(m.UserId)
	return &ShardingSQL{
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
