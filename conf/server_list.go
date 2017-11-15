package conf

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/fatih/color"
	"github.com/juju/errors"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"github.com/wfxiang08/db-sharding/mysql"
	"io/ioutil"
	"strconv"
	"strings"
)

type DatabaseConfig struct {
	Databases          []string   `toml:"dbs"`
	User               string     `toml:"user"`
	Password           string     `toml:"password"`
	SlaveMasterMapping [][]string `toml:"slave_master_mapping"`
	Master2Slave       map[string]string
}

func NewConfigWithFile(name string) (*DatabaseConfig, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(string(data))
}

func NewConfig(data string) (*DatabaseConfig, error) {
	var c DatabaseConfig
	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	c.Master2Slave = make(map[string]string)
	for _, mapping := range c.SlaveMasterMapping {
		c.Master2Slave[mapping[1]] = mapping[0]
	}

	return &c, nil
}

func (c *DatabaseConfig) GetDBUri(alias string) string {
	dbName, hostname, port := c.GetDB(alias)
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?interpolateParams=true&autocommit=true&charset=utf8mb4,utf8,latin1", c.User, c.Password, hostname, port, dbName)
}
func (c *DatabaseConfig) AliasToConnectionConfig(dbAlias string) *mysql.ConnectionConfig {
	// binlog一次只处理一台机器
	_, hostname, port := c.GetDB(dbAlias)
	sourceConfig := &mysql.ConnectionConfig{
		Key:  mysql.InstanceKey{Hostname: hostname, Port: port},
		User: c.User, Password: c.Password,
	}
	return sourceConfig
}

func (c *DatabaseConfig) GetDB(alias string) (dbName string, hostname string, port int) {
	for _, db := range c.Databases {
		// db格式:
		// alias:db@host@port
		if strings.HasPrefix(db, "#") {
			continue
		}
		fields := strings.Split(db, ":")
		if len(fields) != 2 {
			fmt.Printf(color.RedString("Invalid db config found: %s\n"), db)
			continue
		} else if fields[0] == alias {
			items := strings.Split(fields[1], "@")
			if len(items) < 2 {
				log.Panicf(color.RedString("Invalid db config found: %s\n"), db)
			} else {
				dbName = items[0]
				hostname = items[1]
				if len(items) > 2 {
					p, err := strconv.ParseInt(items[2], 10, 64)
					if err != nil {
						log.Panicf(color.RedString("Invalid db config found: %s\n"), db)
					}
					port = int(p)
				} else {
					port = 3306
				}
				return dbName, hostname, port
			}
		} else {
			continue
		}
	}

	log.Panicf(color.RedString("No db found for alias: %s\n"), alias)
	return
}
