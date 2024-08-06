package adapter

import (
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"

	_ "github.com/go-sql-driver/mysql"
	initMysql "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

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
	source      *Source
	dbInfo      *DatabaseInfo
	tableInfo   map[string]tableInfo
	canal       *canal.Canal
	db          *sqlx.DB
	lastPosName string
	lastPos     uint32
	stopping    bool
}
type tableInfo struct {
	initialLoaded bool
}

func NewDatabase() *Database {
	return &Database{
		dbInfo:    &DatabaseInfo{},
		tableInfo: make(map[string]tableInfo, 0),
		stopping:  false,
	}
}

func (database *Database) Connect(source *Source) error {

	info := source.info
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

	loc, err := time.LoadLocation("UTC")
	if err != nil {
		log.Fatal("load location error", err)
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
	cfg.ParseTime = true
	cfg.TimestampStringLocation = loc
	cfg.Dump.TableDB = info.DBName
	cfg.Dump.Tables = targetTables

	// Open cannal
	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	database.canal = c

	// Open database
	config := initMysql.Config{
		User:                 info.Username,
		Passwd:               info.Password,
		Addr:                 connStr,
		Net:                  "tcp",
		DBName:               info.DBName,
		AllowNativePasswords: true,
		Loc:                  loc,
		ParseTime:            true,
		//Params:               params,
	}

	initConnStr := config.FormatDSN()
	db, err := sqlx.Open("mysql", initConnStr)
	if err != nil {
		log.Fatal(err)
		return nil
	}

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	db.SetConnMaxIdleTime(1 * time.Minute)
	db.SetConnMaxLifetime(4 * time.Minute)

	database.db = db

	database.source = source

	return nil
}

func (database *Database) Uninit() {
	database.stopping = true
	database.canal.Close()
}

func (database *Database) GetCanalConnection() *canal.Canal {
	return database.canal
}

func (database *Database) WatchEvents(tables []string, initialLoad bool, fn func(*CDCEvent)) error {

	c := database.GetCanalConnection()
	for {
		if database.stopping {
			time.Sleep(time.Second)
			return nil
		}
		log.Info("Start Watch Event.")
		h := &binlogHandler{
			fn:    fn,
			canal: c,
		}
		c.SetEventHandler(h)
		//if !initialLoad && database.lastPos == 0 {
		if database.lastPos == 0 {
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
			if database.stopping {
				return nil
			}

			log.Error(err)
			<-time.After(1 * time.Second)
			return err
		}
	}

	return nil
}

func (database *Database) DoInitialLoad(sourceName string, tables []string, fn func(*CDCEvent)) error {
	for _, tableName := range tables {
		//get tableInfo
		tableInfo := database.tableInfo[tableName]

		// if scn not equal 0 than don't do it.
		if tableInfo.initialLoaded {
			continue
		}

		i := uint32(0)
		// query
		// begin transation
		tx, err := database.db.Beginx()
		if err != nil {
			log.Error(err)
			tx.Rollback()
			continue
		}
		defer tx.Rollback()

		rows, err := tx.Queryx(fmt.Sprintf("SELECT * FROM %s", tableName))
		if err != nil {
			tx.Rollback()
			log.Error(err)
			continue
		}
		defer rows.Close()
		for rows.Next() {
			// parse data
			event := make(map[string]interface{}, 0)
			err := rows.MapScan(event)
			if err != nil {
				log.Error("mapScan: ", err)
				tx.Rollback()
				continue
			}

			i += 1
			//Prepare CDC event
			e := database.processSnapshotEvent(tableName, event)
			e.PosName = tableName
			e.Pos = i

			fn(e)

		}
		if err := rows.Err(); err != nil {
			if database.stopping {
				return nil
			}
			log.Error("Initialization Error: ", err)
			//time.Sleep(time.Duration(interval) * time.Second)
		}

		rows.Close()

		tx.Commit()

		initialLoadStatusCol := fmt.Sprintf("%s-%s-initialload", sourceName, tableName)
		err = database.source.store.PutInt64("status", []byte(initialLoadStatusCol), 1)
		if err != nil {
			log.Error(err)
			return err
		}
		log.Info(tableName, " initialLoad done.")

	}
	return nil
}

func (database *Database) StartCDC(tables []string, initialLoad bool, fn func(*CDCEvent)) error {

	if initialLoad {
		//database.DoInitialLoad(sourceName, tables, fn, initialLoadBatchSize, interval)
		database.DoInitialLoad(database.source.name, tables, fn)
	}

	go database.WatchEvents(tables, initialLoad, fn)

	return nil
}
