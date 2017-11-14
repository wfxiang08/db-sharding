/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	gosql "database/sql"
	"fmt"
	"github.com/outbrain/golib/sqlutils"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"github.com/wfxiang08/db-sharding/binlog"
	"github.com/wfxiang08/db-sharding/mysql"
	"strings"
	"sync"
	"time"
)

type BinlogEventListener struct {
	async        bool
	databaseName string
	tableName    string

	databaseNameLower string
	tableNameLower    string

	dbPattern    bool
	tablePattern bool

	onDmlEvent func(binlogEntry *binlog.BinlogEntry) error
}

func (l *BinlogEventListener) init() {
	l.databaseNameLower = strings.ToLower(l.databaseName)
	l.tableNameLower = strings.ToLower(l.tableName)
	if strings.HasSuffix(l.databaseNameLower, "*") {
		l.dbPattern = true
		l.databaseNameLower = l.databaseNameLower[0 : len(l.databaseNameLower)-1]
	}
	if strings.HasSuffix(l.tableNameLower, "*") {
		l.tablePattern = true
		l.tableNameLower = l.tableNameLower[0 : len(l.tableNameLower)-1]
	}
}

// dbPattern            false             true
// databaseNameLower    dbname			  dbname_prefix*
//
func (l *BinlogEventListener) Match(db string, table string) bool {
	db = strings.ToLower(db)
	// 匹配db
	// prefix*
	// full_match
	if l.dbPattern {
		if !strings.HasPrefix(db, l.databaseNameLower) {
			return false
		}
	} else if db != l.databaseNameLower {
		return false
	}

	table = strings.ToLower(table)
	if l.tablePattern {
		return strings.HasPrefix(table, l.tableNameLower)
	} else {
		return table == l.tableNameLower
	}
}

const (
	EventsChannelBufferSize       = 1
	ReconnectStreamerSleepSeconds = 5
)

// 将来自MySQL binlog转换成为BinlogEvent, 交给BinlogEventListener来处理
type EventsStreamer struct {
	connectionConfig         *mysql.ConnectionConfig
	DatabaseName             string
	db                       *gosql.DB
	maxRetry                 int64
	initialBinlogCoordinates *mysql.BinlogCoordinates
	listeners                [](*BinlogEventListener)
	listenersMutex           *sync.Mutex
	eventsChannel            chan *binlog.BinlogEntry
	binlogReader             *binlog.GoMySQLReader
	serverId                 uint
	metaDir                  string
	masterInfo               *MasterInfo
}

func NewEventsStreamer(connectionConfig *mysql.ConnectionConfig, maxRetry int64, serverId uint, metaDir string) *EventsStreamer {
	return &EventsStreamer{
		connectionConfig: connectionConfig,
		maxRetry:         maxRetry,
		listeners:        [](*BinlogEventListener){},
		listenersMutex:   &sync.Mutex{},
		eventsChannel:    make(chan *binlog.BinlogEntry, EventsChannelBufferSize),
		serverId:         serverId,
		metaDir:          metaDir,
	}
}

// AddListener registers a new listener for binlog events, on a per-table basis
func (this *EventsStreamer) AddListener(
	async bool, databaseName string, tableName string,
	onDmlEvent func(binlogEntry *binlog.BinlogEntry) error) (err error) {

	this.listenersMutex.Lock()
	defer this.listenersMutex.Unlock()

	if databaseName == "" {
		return fmt.Errorf("Empty database name in AddListener")
	}
	if tableName == "" {
		return fmt.Errorf("Empty table name in AddListener")
	}
	listener := &BinlogEventListener{
		async:        async,
		databaseName: databaseName,
		tableName:    tableName,
		onDmlEvent:   onDmlEvent,
	}
	listener.init()

	this.listeners = append(this.listeners, listener)
	return nil
}

// notifyListeners will notify relevant listeners with given DML event. Only
// listeners registered for changes on the table on which the DML operates are notified.
func (this *EventsStreamer) notifyListeners(binlogEntry *binlog.BinlogEntry) {
	this.listenersMutex.Lock()
	defer this.listenersMutex.Unlock()

	binlogEvent := binlogEntry.DmlEvent
	// 如何通知listeners呢?
	for _, listener := range this.listeners {
		listener := listener
		// DB和Table一致，可以做一个预处理, 把listener的names都统一为小写
		// 所有的db, 或者满足条件的db
		if !listener.Match(binlogEvent.DatabaseName, binlogEvent.TableName) {
			continue
		}

		// 同步和异步的区别?
		// Dml vs. DDL
		if listener.async {
			go func() {
				listener.onDmlEvent(binlogEntry)
			}()
		} else {
			listener.onDmlEvent(binlogEntry)
		}
	}
}

func (this *EventsStreamer) InitDBConnections(binlogFile string, binlogPos int64) (err error) {

	// 1. Connection + DB 构成完整的Uri
	EventsStreamerUri := this.connectionConfig.GetDBUri(this.DatabaseName)
	if this.db, _, err = sqlutils.GetDB(EventsStreamerUri); err != nil {
		return err
	}

	this.masterInfo, _ = LoadMasterInfo(this.metaDir, this.connectionConfig.Key)
	if this.masterInfo == nil {
		log.Panicf("MasterInfo not found....")
	}

	// 获取当前的binlog的位置
	// 如果没有有效的信息
	if len(binlogFile) == 0 && len(this.masterInfo.Name) == 0 {
		if err := this.readCurrentBinlogCoordinates(); err != nil {
			return err
		}
	} else {
		if len(binlogFile) > 0 {
			this.initialBinlogCoordinates = &mysql.BinlogCoordinates{
				LogFile: binlogFile,
				LogPos:  binlogPos,
			}
		} else {
			this.initialBinlogCoordinates = this.masterInfo.Position()
		}
		log.Printf("Get initial binlog coordinates from input: %s", this.initialBinlogCoordinates.String())
	}

	// 初始化binlog read的初始位置
	if err := this.initBinlogReader(this.initialBinlogCoordinates); err != nil {
		return err
	}

	return nil
}

// initBinlogReader creates and connects the reader: we hook up to a MySQL server as a replica
func (this *EventsStreamer) initBinlogReader(binlogCoordinates *mysql.BinlogCoordinates) error {
	// binlog reader
	goMySQLReader, err := binlog.NewGoMySQLReader(this.connectionConfig, this.serverId)
	if err != nil {
		return err
	}
	// 设置起始read的位置
	// 如果binlog出现问题，如何处理呢?
	if err := goMySQLReader.ConnectBinlogStreamer(*binlogCoordinates); err != nil {
		log.ErrorErrorf(err, "ConnectBinlogStreamer failed")
		return err
	}

	// 创建完毕
	this.binlogReader = goMySQLReader
	return nil
}

func (this *EventsStreamer) GetCurrentBinlogCoordinates() *mysql.BinlogCoordinates {
	return this.binlogReader.GetCurrentBinlogCoordinates()
}

func (this *EventsStreamer) GetReconnectBinlogCoordinates() *mysql.BinlogCoordinates {
	return &mysql.BinlogCoordinates{LogFile: this.GetCurrentBinlogCoordinates().LogFile, LogPos: 4}
}

// readCurrentBinlogCoordinates reads master status from hooked server
func (this *EventsStreamer) readCurrentBinlogCoordinates() error {
	query := `show master status`
	foundMasterStatus := false

	// 如何处理一些特殊的请求?
	// 除了使用gorm, 该如何使用其他的开始模式呢?
	// 如何实现rows, row to map？
	err := sqlutils.QueryRowsMap(this.db, query, func(m sqlutils.RowMap) error {
		// 看来这个不支持gtid模式？
		this.initialBinlogCoordinates = &mysql.BinlogCoordinates{
			LogFile: m.GetString("File"),
			LogPos:  m.GetInt64("Position"),
		}
		foundMasterStatus = true

		return nil
	})
	if err != nil {
		return err
	}
	if !foundMasterStatus {
		return fmt.Errorf("Got no results from SHOW MASTER STATUS. Bailing out")
	}
	log.Debugf("Streamer binlog coordinates: %+v", *this.initialBinlogCoordinates)
	return nil
}

// StreamEvents will begin streaming events. It will be blocking, so should be
// executed by a goroutine
func (this *EventsStreamer) StreamEvents(canStopStreaming func() bool) error {
	go func() {
		for binlogEntry := range this.eventsChannel {
			if binlogEntry.DmlEvent != nil {
				this.notifyListeners(binlogEntry)
			}
		}
	}()
	// The next should block and execute forever, unless there's a serious error
	var successiveFailures int64
	var lastAppliedRowsEventHint mysql.BinlogCoordinates
	for {
		// 第一步: Streaming
		//        如果失败，则等待5s
		if err := this.binlogReader.StreamEvents(func() bool {
			// 保存binlog的读取信息
			this.masterInfo.Save(&this.binlogReader.LastAppliedRowsEventHint)
			return canStopStreaming()

		}, this.eventsChannel); err != nil {
			log.Infof("StreamEvents encountered unexpected error: %+v", err)

			time.Sleep(ReconnectStreamerSleepSeconds * time.Second)

			// See if there's retry overflow
			// 失败提示? 如果连续N次在同一个地方失败，则退出
			if this.binlogReader.LastAppliedRowsEventHint.Equals(&lastAppliedRowsEventHint) {
				successiveFailures += 1
			} else {
				successiveFailures = 0
			}
			if successiveFailures > this.maxRetry {
				return fmt.Errorf("%d successive failures in streamer reconnect at coordinates %+v", successiveFailures, this.GetReconnectBinlogCoordinates())
			}

			// Reposition at same binlog file.
			lastAppliedRowsEventHint = this.binlogReader.LastAppliedRowsEventHint
			log.Infof("Reconnecting... Will resume at %+v", lastAppliedRowsEventHint)

			// 获取之前的binlogReader的binlog-coordinate
			// 重新初始化binlog reader？
			if err := this.initBinlogReader(this.GetReconnectBinlogCoordinates()); err != nil {
				return err
			}
			this.binlogReader.LastAppliedRowsEventHint = lastAppliedRowsEventHint
		}
	}
}

func (this *EventsStreamer) Close() (err error) {
	err = this.binlogReader.Close()
	log.Infof("Closed streamer connection. err=%+v", err)
	return err
}
