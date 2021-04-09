package adapter

import (
	"fmt"
	"time"

	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"

	log "github.com/sirupsen/logrus"
)

type DatabaseInfo struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	DbName   string `json:"db_name"`
}
type Database struct {
	dbInfo      *DatabaseInfo
	canal       *canal.Canal
	lastPosName string
	lastPos     uint32
}

func NewDatabase() *Database {
	return &Database{
		dbInfo: &DatabaseInfo{},
	}
}

func (database *Database) Connect(info *SourceInfo) error {

	log.WithFields(log.Fields{
		"host":     info.Host,
		"port":     info.Port,
		"username": info.Username,
		"dbname":   info.DBName,
	}).Info("Connecting to database")

	targetTables := make([]string, 0, len(info.Tables))
	for tableName, _ := range info.Tables {
		targetTables = append(targetTables, tableName)
	}

	connStr := fmt.Sprintf(
		"%s:%d",
		info.Host,
		info.Port,
	)

	// configure canal cfg
	cfg := canal.NewDefaultConfig()
	cfg.Addr = connStr
	cfg.User = info.Username
	cfg.Password = info.Password
	cfg.Dump.TableDB = info.DBName
	cfg.Dump.Tables = targetTables

	// Open database
	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.Error(err)
		return err
	}

	database.canal = c

	return nil
}

func (database *Database) GetConnection() *canal.Canal {
	return database.canal
}

func (database *Database) StartCDC(tables []string, initialLoad bool, fn func(*CDCEvent)) error {

	c := database.GetConnection()

	for {
		log.Info("Start Watch Event.")
		h := &binlogHandler{
			fn:    fn,
			canal: c,
		}
		c.SetEventHandler(h)
		if !initialLoad && database.lastPos == 0 {
			pos, _ := c.GetMasterPos()
			database.lastPosName = pos.Name
			database.lastPos = pos.Pos
		}
		pos := mysql.Position{
			Name: database.lastPosName,
			Pos:  database.lastPos,
		}

		err := c.RunFrom(pos)
		if err != nil {
			log.Error(err)
			<-time.After(1 * time.Second)
			return err
		}
	}

	return nil
}
