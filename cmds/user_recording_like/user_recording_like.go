package main

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
