package models

import (
	"encoding/json"
	"fmt"
)

type ShardingSQL struct {
	ShardingIndex int
	SQL           string
	Args          []interface{}
}

func (this *ShardingSQL) String() string {
	args, _ := json.Marshal(this.Args)
	return fmt.Sprintf("%s --> %s", this.SQL, string(args))
}

type SqlApplier interface {
	PushSQL(sql *ShardingSQL)
}
