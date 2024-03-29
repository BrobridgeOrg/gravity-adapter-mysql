package adapter

import (
	"fmt"
	"sync"

	//"sync/atomic"
	"time"
	"unsafe"

	"github.com/BrobridgeOrg/broton"
	"github.com/spf13/viper"

	gravity_adapter "github.com/BrobridgeOrg/gravity-sdk/adapter"
	parallel_chunked_flow "github.com/cfsghost/parallel-chunked-flow"
	log "github.com/sirupsen/logrus"
)

var counter uint64

type Packet struct {
	EventName string
	Payload   []byte
}

type Source struct {
	adapter   *Adapter
	info      *SourceInfo
	store     *broton.Store
	database  *Database
	connector *gravity_adapter.AdapterConnector
	incoming  chan *CDCEvent
	name      string
	parser    *parallel_chunked_flow.ParallelChunkedFlow
	tables    map[string]SourceTable
	mu        sync.Mutex
}

type Request struct {
	Pos     uint32
	PosName string
	Req     *Packet
	Table   string
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return &Request{}
	},
}

func StrToBytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func NewSource(adapter *Adapter, name string, sourceInfo *SourceInfo) *Source {

	// required channel
	if len(sourceInfo.Host) == 0 {
		log.WithFields(log.Fields{
			"source": name,
		}).Error("Required host")

		return nil
	}

	if len(sourceInfo.DBName) == 0 {
		log.WithFields(log.Fields{
			"source": name,
		}).Error("Required dbname")

		return nil
	}

	// Prepare table configs
	tables := make(map[string]SourceTable, len(sourceInfo.Tables))
	for tableName, config := range sourceInfo.Tables {
		tables[tableName] = config
	}

	source := &Source{
		adapter:  adapter,
		info:     sourceInfo,
		store:    nil,
		database: NewDatabase(),
		incoming: make(chan *CDCEvent, 204800),
		name:     name,
		tables:   tables,
	}

	// Initialize parapllel chunked flow
	pcfOpts := parallel_chunked_flow.Options{
		BufferSize: 204800,
		ChunkSize:  512,
		ChunkCount: 512,
		Handler: func(data interface{}, output func(interface{})) {
			/*
				id := atomic.AddUint64((*uint64)(&counter), 1)
				if id%100 == 0 {
					log.Info(id)
				}
			*/

			req := source.prepareRequest(data.(*CDCEvent))
			if req == nil {
				log.Error("req in nil")
				return
			}

			output(req)
		},
	}

	source.parser = parallel_chunked_flow.NewParallelChunkedFlow(&pcfOpts)

	return source
}

func (source *Source) parseEventName(event *CDCEvent) string {

	eventName := ""

	// determine event name
	tableInfo, ok := source.tables[event.Table]
	if !ok {
		return eventName
	}

	switch event.Operation {
	case InsertOperation:
		eventName = tableInfo.Events.Create
	case UpdateOperation:
		eventName = tableInfo.Events.Update
	case DeleteOperation:
		eventName = tableInfo.Events.Delete
	case SnapshotOperation:
		eventName = tableInfo.Events.Snapshot
	default:
		return eventName
	}

	return eventName
}

func (source *Source) Init() error {

	if viper.GetBool("store.enabled") {

		// Initializing store
		log.WithFields(log.Fields{
			"store": "adapter-" + source.name,
		}).Info("Initializing store for adapter")
		store, err := source.adapter.storeMgr.GetStore("adapter-" + source.name)
		if err != nil {
			return err
		}

		source.store = store

		var lastPos uint64 = 0
		var lastPosName string = ""

		// Register columns
		columns := []string{"status"}
		err = source.store.RegisterColumns(columns)
		if err != nil {
			log.Error(err)
			return err
		}

		// Getting last Position Name
		lastPosName, err = source.store.GetString("status", []byte("POSNAME"))
		if err != nil {
			log.Error(err)
			return err
		}

		// Getting last Position
		lastPos, err = source.store.GetUint64("status", []byte("POS"))
		if err != nil {
			log.Error(err)
			return err
		}

		source.database.lastPosName = lastPosName
		source.database.lastPos = uint32(lastPos)

	}

	// Initializing gravity adapter connector
	source.connector = source.adapter.app.GetAdapterConnector()

	// Connect to database
	err := source.database.Connect(source.info)
	if err != nil {
		return err
	}

	go source.eventReceiver()
	go source.requestHandler()

	// Getting tables
	tables := make([]string, 0, len(source.tables))
	for tableName, _ := range source.tables {
		tables = append(tables, tableName)
	}

	log.WithFields(log.Fields{
		"tables": tables,
	}).Info("Preparing to watch tables")

	err = source.database.StartCDC(tables, source.info.InitialLoad, func(event *CDCEvent) {

		eventName := source.parseEventName(event)

		// filter
		if eventName == "" {
			return
		}

		source.incoming <- event
	})
	if err != nil {
		return err
	}

	return nil
}

func (source *Source) eventReceiver() {

	log.WithFields(log.Fields{
		"source":      source.name,
		"client_name": source.adapter.clientName + "-" + source.name,
	}).Info("Initializing workers ...")

	for {
		select {
		case msg := <-source.incoming:
			source.parser.Push(msg)
		}
	}
}

func (source *Source) requestHandler() {

	for {
		select {
		case req := <-source.parser.Output():
			// TODO: retry
			if req == nil {
				log.Error("req in nil")
				break
			}
			source.HandleRequest(req.(*Request))
			requestPool.Put(req)
		}
	}
}

func (source *Source) prepareRequest(event *CDCEvent) *Request {

	source.mu.Lock()
	// determine event name
	eventName := source.parseEventName(event)
	if eventName == "" {
		return nil
	}

	// Prepare payload
	data := make(map[string]interface{}, len(event.Before)+len(event.After))
	for k, v := range event.Before {

		data[k] = v.Data
	}

	for k, v := range event.After {

		data[k] = v.Data
	}

	payload, err := json.Marshal(data)
	if err != nil {
		log.Error(err)
		return nil
	}

	// Preparing request
	request := requestPool.Get().(*Request)
	request.PosName = event.PosName
	request.Pos = event.Pos
	request.Table = event.Table

	request.Req = &Packet{
		EventName: eventName,
		Payload:   payload,
	}

	source.mu.Unlock()
	return request
}

func (source *Source) HandleRequest(request *Request) {

	for {
		// Using new SDK to re-implement this part
		meta := make(map[string]interface{})
		meta["Msg-Id"] = fmt.Sprintf("%s-%s-%s", source.name, request.Table, request.Pos)
		err := source.connector.Publish(request.Req.EventName, request.Req.Payload, meta)
		if err != nil {
			log.Error("Failed to get publish Request:", err)
			time.Sleep(time.Second)
			//return
			continue
		}

		/*
			id := atomic.AddUint64((*uint64)(&counter), 1)
			if id%1000 == 0 {
				log.Info(id)
			}
		*/
		break
	}

	<-source.connector.PublishComplete()

	if source.store == nil {
		return
	}

	for {
		err := source.store.PutUint64("status", []byte("POS"), uint64(request.Pos))
		if err != nil {
			log.Error("Failed to update Position")
			time.Sleep(time.Second)
			continue
		}

		err = source.store.PutString("status", []byte("POSNAME"), request.PosName)
		if err != nil {
			log.Error("Failed to update Position Name")
			time.Sleep(time.Second)
			continue
		}
		break
	}
}
