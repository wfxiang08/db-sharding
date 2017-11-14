package models

import (
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"testing"
)

// go test github.com/wfxiang08/db-sharding/models -v -run "TestSMSharding$"
func TestSMSharding(t *testing.T) {

	shard1 := NewSMHashShard(32, 1)
	shard1_10 := NewSMHashShard(32, 10)
	k1, _ := shard1.FindForKey(6755399444017774)
	k1_10, _ := shard1_10.FindForKey(6755399444017774)

	log.Printf("shard1_1: %d shard1_10: %d", k1, k1_10)

	k1, _ = shard1.FindForKey(6755399441094662)
	k1_10, _ = shard1_10.FindForKey(6755399441094662)

	log.Printf("shard1_1: %d shard1_10: %d", k1, k1_10)

}
