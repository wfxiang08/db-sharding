/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package binlog

import (
	"fmt"
	"sync"

	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"

	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"github.com/wfxiang08/db-sharding/mysql"
	"github.com/wfxiang08/db-sharding/sql"
)

type GoMySQLReader struct {
	connectionConfig         *mysql.ConnectionConfig
	binlogSyncer             *replication.BinlogSyncer
	binlogStreamer           *replication.BinlogStreamer
	currentCoordinates       mysql.BinlogCoordinates
	currentCoordinatesMutex  *sync.Mutex
	LastAppliedRowsEventHint mysql.BinlogCoordinates // binlog的坐标
}

func NewGoMySQLReader(connectionConfig *mysql.ConnectionConfig, serverId uint) (binlogReader *GoMySQLReader, err error) {
	binlogReader = &GoMySQLReader{
		connectionConfig:        connectionConfig,
		currentCoordinates:      mysql.BinlogCoordinates{},
		currentCoordinatesMutex: &sync.Mutex{},
		binlogSyncer:            nil,
		binlogStreamer:          nil,
	}

	binlogSyncerConfig := &replication.BinlogSyncerConfig{
		ServerID: uint32(serverId),
		Flavor:   "mysql",
		Host:     connectionConfig.Key.Hostname,
		Port:     uint16(connectionConfig.Key.Port),
		User:     connectionConfig.User,
		Password: connectionConfig.Password,
	}
	binlogReader.binlogSyncer = replication.NewBinlogSyncer(binlogSyncerConfig)

	return binlogReader, err
}

// ConnectBinlogStreamer
func (this *GoMySQLReader) ConnectBinlogStreamer(coordinates mysql.BinlogCoordinates) (err error) {
	if coordinates.IsEmpty() {
		return fmt.Errorf("Emptry coordinates at ConnectBinlogStreamer()")
	}

	this.currentCoordinates = coordinates
	log.Infof("Connecting binlog streamer at %+v", this.currentCoordinates)
	// Start sync with sepcified binlog file and position
	this.binlogStreamer, err = this.binlogSyncer.StartSync(gomysql.Position{this.currentCoordinates.LogFile, uint32(this.currentCoordinates.LogPos)})

	return err
}

func (this *GoMySQLReader) GetCurrentBinlogCoordinates() *mysql.BinlogCoordinates {
	this.currentCoordinatesMutex.Lock()
	defer this.currentCoordinatesMutex.Unlock()
	returnCoordinates := this.currentCoordinates
	return &returnCoordinates
}

// StreamEvents
// 处理Row Event
// BinlogEvent 中特殊的Event ==> rowsEvent
//
func (this *GoMySQLReader) handleRowsEvent(ev *replication.BinlogEvent, rowsEvent *replication.RowsEvent,
	entriesChannel chan<- *BinlogEntry) error {

	if this.currentCoordinates.SmallerThanOrEquals(&this.LastAppliedRowsEventHint) {
		log.Debugf("Skipping handled query at %+v", this.currentCoordinates)
		return nil
	}

	dml := ToEventDML(ev.Header.EventType.String())
	if dml == NotDML {
		return fmt.Errorf("Unknown DML type: %s", ev.Header.EventType.String())
	}

	for i, row := range rowsEvent.Rows {
		// UpdateDML 两个Row一组数据，不处理奇数组数据
		if dml == UpdateDML && i%2 == 1 {
			// An update has two rows (WHERE+SET)
			// We do both at the same time
			continue
		}

		// 封装binlogEntry
		// pos, schema, table, dml
		binlogEntry := NewBinlogEntryAt(this.currentCoordinates)
		binlogEntry.Timestamp = int64(ev.Header.Timestamp)
		binlogEntry.DmlEvent = NewBinlogDMLEvent(
			string(rowsEvent.Table.Schema),
			string(rowsEvent.Table.Table),
			dml,
		)

		// Insert    --> NewColumnValues
		// UpdateDML --> WhereColumnValues & NewColumnValues
		// DeleteDML --> WhereColumnValues
		switch dml {
		case InsertDML:
			{
				binlogEntry.DmlEvent.NewColumnValues = sql.ToColumnValues(row)
			}
		case UpdateDML:
			{
				binlogEntry.DmlEvent.WhereColumnValues = sql.ToColumnValues(row)
				binlogEntry.DmlEvent.NewColumnValues = sql.ToColumnValues(rowsEvent.Rows[i+1])
			}
		case DeleteDML:
			{
				binlogEntry.DmlEvent.WhereColumnValues = sql.ToColumnValues(row)
			}
		}
		// The channel will do the throttling. Whoever is reding from the channel
		// decides whether action is taken sycnhronously (meaning we wait before
		// next iteration) or asynchronously (we keep pushing more events)
		// In reality, reads will be synchronous
		entriesChannel <- binlogEntry
	}

	// 记录执行过的RowEvent的地址
	this.LastAppliedRowsEventHint = this.currentCoordinates
	return nil
}

// StreamEvents
func (this *GoMySQLReader) StreamEvents(canStopStreaming func() bool, entriesChannel chan<- *BinlogEntry) error {
	if canStopStreaming() {
		return nil
	}
	for {
		// 任何时候都可以中断
		if canStopStreaming() {
			break
		}

		// 获取event
		ev, err := this.binlogStreamer.GetEvent(context.Background())
		if err != nil {
			return err
		}
		// 更新LogPos
		func() {
			this.currentCoordinatesMutex.Lock()
			defer this.currentCoordinatesMutex.Unlock()
			this.currentCoordinates.LogPos = int64(ev.Header.LogPos)
		}()

		// 如果是binlog文件rotate, 则更新LogFile
		if rotateEvent, ok := ev.Event.(*replication.RotateEvent); ok {
			func() {
				this.currentCoordinatesMutex.Lock()
				defer this.currentCoordinatesMutex.Unlock()
				this.currentCoordinates.LogFile = string(rotateEvent.NextLogName)
			}()

			// 如果binlog同步挂了，可以通过该日志定位
			log.Infof("XXX: rotate to next log pos: %s:4", rotateEvent.NextLogName)

		} else if rowsEvent, ok := ev.Event.(*replication.RowsEvent); ok {
			// 普通的rowsEvent如何处理呢?
			if err := this.handleRowsEvent(ev, rowsEvent, entriesChannel); err != nil {
				return err
			}
		} else if _, ok := ev.Event.(*replication.QueryEvent); ok {
			// 修改表结构
			// @1, @2, .., @N 是和当前的表结构对应的，如果表结构变化了，那么@1 <--> column name之间的映射关系可能需要调整
			// TODO:
			// gh-ost只所以不关系，是因为这个脚本只是短暂地工作，可以认为表结构的在ost过程中不会变化
			// sharding也是类似的逻辑，可以认为在sharding过程中表结构相对稳定, 如果处理cache, 则可能需要关注这方面的逻辑
		}
	}
	log.Debugf("done streaming events")

	return nil
}

func (this *GoMySQLReader) Close() error {
	// Historically there was a:
	//   this.binlogSyncer.Close()
	// here. A new go-mysql version closes the binlog syncer connection independently.
	// I will go against the sacred rules of comments and just leave this here.
	// This is the year 2017. Let's see what year these comments get deleted.
	return nil
}
