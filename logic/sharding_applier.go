package logic

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/fatih/color"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/wfxiang08/cyutils/utils"
	"github.com/wfxiang08/cyutils/utils/atomic2"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"github.com/wfxiang08/db-sharding/conf"
	"github.com/wfxiang08/db-sharding/models"
	"strings"
	"sync"
	"time"
)

// 接受Event
type ShardingApplier struct {
	shardingIndex   int
	sqlsBuffered    []*models.ShardingSQL
	sqls            chan *models.ShardingSQL
	batchInsertSize int
	maxRetries      int
	db              *sql.DB

	totalPushed   int
	totalExecuted int
	dryRun        bool

	isClosed atomic2.Bool

	batchInsertMode atomic2.Bool
	builder         models.ModelBuilder
	pauseInput      *atomic2.Bool
}

type ShardingAppliers []*ShardingApplier

// 自动处理Sharding的逻辑
func (this ShardingAppliers) PushSQL(sql *models.ShardingSQL) {
	this[sql.ShardingIndex].PushSQL(sql)
}

// 批量修改模式
func (this ShardingAppliers) SetBatchInsertMode(batchInsert bool) {
	for _, applier := range this {
		applier.batchInsertMode.Set(batchInsert)
	}
}

func NewShardingApplier(shardingIndex, batchSize int, cacheSize int, config *conf.DatabaseConfig, dryRun bool,
	builder models.ModelBuilder, pauseInput *atomic2.Bool) (*ShardingApplier, error) {
	result := &ShardingApplier{
		shardingIndex:   shardingIndex,
		sqlsBuffered:    make([]*models.ShardingSQL, 0, batchSize),
		sqls:            make(chan *models.ShardingSQL, cacheSize), // 多保留一些数据，保证各个shard能并发跑起来
		batchInsertSize: batchSize,
		maxRetries:      10,
		dryRun:          dryRun,
		builder:         builder,
		pauseInput:      pauseInput,
	}

	result.isClosed.Set(false)
	result.batchInsertMode.Set(false)

	var err error
	result.db, err = sql.Open("mysql", config.GetDBUri(fmt.Sprintf("shard%d", shardingIndex)))
	// 控制一下最大的连接数
	result.db.SetMaxOpenConns(2)
	result.db.SetMaxIdleConns(2)
	if err != nil {
		return nil, err
	} else {
		return result, nil
	}
}

func (this *ShardingApplier) Close() {
	if this.isClosed.CompareAndSwap(false, true) {
		// 表示没有数据了
		close(this.sqls)
	}
}

// 添加到队列末尾
func (this *ShardingApplier) PushSQL(sql *models.ShardingSQL) {
	if sql != nil {
		this.sqls <- sql
		this.totalPushed++
	}
}

// retryOperation attempts up to `count` attempts at running given function,
// exiting as soon as it returns with non-error.
func (this *ShardingApplier) retryOperation(operation func() error, notFatalHint ...bool) (err error) {
	maxRetries := int(this.maxRetries)
	for i := 0; i < maxRetries; i++ {
		if i != 0 {
			// sleep after previous iteration
			// 如果遇到异常，最好等待一段时间，否则retry也是失败
			time.Sleep(1 * time.Second)
		}
		err = operation()
		if err == nil {
			return nil
		} else {
			log.ErrorErrorf(err, color.RedString("Shard %d")+" failed, and retry", this.shardingIndex)
		}
		// there's an error. Let's try again.
	}

	// 直接报错，中断执行
	if len(notFatalHint) == 0 {
		log.PanicErrorf(err, "retryOperation failed for shard: %d", this.shardingIndex)
	}
	return err
}

func (this *ShardingApplier) Run(wg *sync.WaitGroup) {
	defer wg.Done()

	for true {

		// 暂停，直到状态改变
		for this.pauseInput.Get() {
			log.Printf(color.BlueString("Throttle Pause")+", sleep 1 second for shard: %d", this.shardingIndex)
			time.Sleep(time.Second)
		}

		timeout := false
		channelClosed := false
		select {
		case shardingSQL, ok := <-this.sqls:
			if ok {
				this.sqlsBuffered = append(this.sqlsBuffered, shardingSQL)
			} else {
				// 关闭了，则直接结束
				channelClosed = true
				timeout = true
			}
		case <-time.After(time.Second * 2):
			timeout = true
		}

		if len(this.sqlsBuffered) >= this.batchInsertSize || (len(this.sqlsBuffered) > 0 && timeout) {
			// 有数据，或timeout
			batchSQL := func() error {
				if this.batchInsertMode.Get() {
					// 如何处理批量插入的问题呢?
					insertSqls := make([]string, len(this.sqlsBuffered))
					argsAll := make([]interface{}, 0, len(this.sqlsBuffered)*len(this.sqlsBuffered[0].Args))
					for idx, shardingSQL := range this.sqlsBuffered {
						if idx == 0 {
							insertSqls[idx] = shardingSQL.SQL
						} else {
							insertSqls[idx] = this.builder.GetBatchInsertSegment()
						}
						argsAll = append(argsAll, shardingSQL.Args...)
					}

					// 执行效果
					_, err := this.db.Exec(strings.Join(insertSqls, ", "), argsAll...)
					return err

				} else {
					// 处理一批数据
					tx, err := this.db.Begin()
					if err != nil {
						return err
					}

					rollback := func(err error) error {
						tx.Rollback()
						return err
					}

					for _, shardingSQL := range this.sqlsBuffered {
						// 将参数展开
						// 会不会应为网络round trip很大呢?
						_, err := tx.Exec(shardingSQL.SQL, shardingSQL.Args...)
						if err != nil {
							return rollback(err)
						}
					}
					err = tx.Commit()
					return err
				}
			}

			// log.Printf("Batch update shard: %d", this.shardingIndex)
			if this.dryRun {
				data, _ := json.Marshal(this.sqlsBuffered[0].Args)
				log.Printf("SQL: %s, Args: %s", this.sqlsBuffered[0].SQL, string(data))
			} else {
				t0 := time.Now()
				// 运行SQL
				err := this.retryOperation(batchSQL)
				t1 := time.Now()
				log.Printf(color.CyanString("Shard: %02d")+", sql executed size: %d, elapsed: %.3fms", this.shardingIndex,
					len(this.sqlsBuffered), utils.ElapsedMillSeconds(t0, t1))

				if err != nil {
					log.PanicErrorf(err, color.RedString("Shard: %d")+", sql executed failed", this.shardingIndex)
					return
				}
			}

			if len(this.sqlsBuffered) >= this.batchInsertSize {
				time.Sleep(time.Millisecond * time.Duration(BatchInsertSleepMilliseconds)) // sleep 20ms
			}

			this.totalExecuted += len(this.sqlsBuffered)
			this.sqlsBuffered = this.sqlsBuffered[0:0]

			log.Printf(color.GreenString("Shard: %02d - apply progress: %.2f%%")+", total_executed: %d/%d", this.shardingIndex,
				float64(this.totalExecuted)/float64(this.totalPushed)*100,
				this.totalExecuted, this.totalPushed)

		} else {
			// 没有数据，要么退出，要么继续等待
			if channelClosed {
				break
			}
		}
	}
}
