package logic

import (
	"bytes"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/siddontang/go/ioutil2"
	"github.com/wfxiang08/cyutils/utils/errors"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"github.com/wfxiang08/db-sharding/mysql"
	"os"
	"path"
	"sync"
	"time"
)

type MasterInfo struct {
	sync.RWMutex

	Name string `toml:"bin_name"`
	Pos  int64  `toml:"bin_pos"`

	filePath     string
	lastSaveTime time.Time
}

func LoadMasterInfo(dataDir string, key mysql.InstanceKey) (*MasterInfo, error) {
	var m MasterInfo

	if len(dataDir) == 0 {
		return &m, nil
	}

	m.filePath = path.Join(dataDir, fmt.Sprintf("%s_%d.info", key.Hostname, key.Port))
	m.lastSaveTime = time.Now()

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, errors.Trace(err)
	}

	f, err := os.Open(m.filePath)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return nil, errors.Trace(err)
	} else if os.IsNotExist(errors.Cause(err)) {
		return &m, nil
	}
	defer f.Close()

	_, err = toml.DecodeReader(f, &m)
	return &m, errors.Trace(err)
}

func (m *MasterInfo) Save(pos *mysql.BinlogCoordinates) error {
	log.Infof("save position %s", pos)

	m.Lock()
	defer m.Unlock()

	m.Name = pos.LogFile
	m.Pos = pos.LogPos

	if len(m.filePath) == 0 {
		return nil
	}

	// 1s保存一次数据
	n := time.Now()
	if n.Sub(m.lastSaveTime) < time.Second {
		return nil
	}

	m.lastSaveTime = n
	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)

	e.Encode(m)

	var err error
	if err = ioutil2.WriteFileAtomic(m.filePath, buf.Bytes(), 0644); err != nil {
		log.Errorf("canal save master info to file %s err %v", m.filePath, err)
	}

	return errors.Trace(err)
}

func (m *MasterInfo) Position() *mysql.BinlogCoordinates {
	m.RLock()
	defer m.RUnlock()

	return &mysql.BinlogCoordinates{
		LogFile: m.Name,
		LogPos:  m.Pos,
	}
}

func (m *MasterInfo) Close() error {
	pos := m.Position()

	return m.Save(pos)
}
