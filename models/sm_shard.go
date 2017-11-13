package models

import (
	"fmt"
	"github.com/siddontang/go/hack"
	"hash/crc32"
	"strconv"
)

/*由分片ID找到分片，可用文件中的函数*/
type KeyError string

func NewKeyError(format string, args ...interface{}) KeyError {
	return KeyError(fmt.Sprintf(format, args...))
}

func HashValue(value interface{}) uint64 {
	switch val := value.(type) {
	case int:
		return uint64(val)
	case uint64:
		return uint64(val)
	case int64:
		return uint64(val)
	case string:
		if v, err := strconv.ParseUint(val, 10, 64); err != nil {
			return uint64(crc32.ChecksumIEEE(hack.Slice(val)))
		} else {
			return uint64(v)
		}
	case []byte:
		return uint64(crc32.ChecksumIEEE(val))
	}
	panic(NewKeyError("Unexpected key variable type %T", value))
}

// 我们自定义的Sharding算法
type SMHashShard struct {
	ShardNum int
	Location uint64
}

func NewSMHashShard(ShardNum int, Location uint64) *SMHashShard {
	return &SMHashShard{
		ShardNum: ShardNum,
		Location: Location,
	}
}

func (s *SMHashShard) FindForKey(key interface{}) (int, error) {
	h := HashValue(key)
	smShardNum := int((h>>48)&((1<<12)-1)) % s.ShardNum
	if s.Location < 2 {
		return smShardNum, nil
	} else {
		// tb0, ..., tbN-1    ---> shard0
		// tbN, ..., tb2N - 1 ---> shard1
		//
		return smShardNum * int(h%uint64(s.Location)), nil
	}
}
func SMShard(h uint64) int {
	return int((h >> 48) & ((1 << 12) - 1))
}
