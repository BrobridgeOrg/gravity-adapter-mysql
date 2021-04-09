package adapter

import (
	//"fmt"
	parser "git.brobridge.com/gravity/gravity-adapter-mysql/pkg/adapter/service/parser"
	"github.com/siddontang/go-mysql/canal"
	//"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/schema"
	//	log "github.com/sirupsen/logrus"
	"time"
)

type binlogHandler struct {
	canal.DummyEventHandler // Dummy handler from external lib
	fn                      func(*CDCEvent)
	canal                   *canal.Canal
}

func (h *binlogHandler) OnRow(e *canal.RowsEvent) error {

	columns := []string{}
	var colType []int
	for _, column := range e.Table.Columns {
		columns = append(columns, column.Name)
		colType = append(colType, column.Type)
	}

	// prepare Before/After Value
	beforeValue := make(map[string]*parser.Value)
	afterValue := make(map[string]*parser.Value)

	for i, row := range e.Rows[0] {
		rowData := row

		switch colType[i] {
		case schema.TYPE_TIMESTAMP, schema.TYPE_DATETIME, schema.TYPE_DATE, schema.TYPE_TIME:
			t, _ := time.Parse("2006-01-02 15:04:05.999999", row.(string))
			rowData = t
		}

		beforeValue[columns[i]] = &parser.Value{
			Data: rowData,
		}
	}

	if len(e.Rows) == 2 {
		for i, row := range e.Rows[1] {
			rowData := row

			switch colType[i] {
			case schema.TYPE_TIMESTAMP, schema.TYPE_DATETIME, schema.TYPE_DATE, schema.TYPE_TIME:
				t, _ := time.Parse("2006-01-02 15:04:05.999999", row.(string))
				rowData = t
			}

			afterValue[columns[i]] = &parser.Value{
				Data: rowData,
			}
		}
	}

	// Prepare CDC event
	result := CDCEvent{}
	switch e.Action {
	case canal.DeleteAction:
		result = CDCEvent{
			Operation: DeleteOperation,
			Table:     e.Table.Name,
			After:     afterValue,
			Before:    beforeValue,
		}

	case canal.UpdateAction:
		result = CDCEvent{
			Operation: UpdateOperation,
			Table:     e.Table.Name,
			After:     afterValue,
			Before:    beforeValue,
		}

	case canal.InsertAction:
		result = CDCEvent{
			Operation: InsertOperation,
			Table:     e.Table.Name,
			After:     afterValue,
			Before:    beforeValue,
		}

	}

	if e.Header == nil {
		result = CDCEvent{
			Operation: SnapshotOperation,
			Table:     e.Table.Name,
			After:     afterValue,
			Before:    beforeValue,
		}

	}

	//send data
	pos, _ := h.canal.GetMasterPos()
	result.PosName = pos.Name
	result.Pos = pos.Pos
	h.fn(&result)

	return nil
}
