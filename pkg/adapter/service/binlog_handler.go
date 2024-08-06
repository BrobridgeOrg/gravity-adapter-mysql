package adapter

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/google/uuid"

	"time"
)

type binlogHandler struct {
	canal.DummyEventHandler // Dummy handler from external lib
	fn                      func(*CDCEvent)
	canal                   *canal.Canal
}

func (h *binlogHandler) joinPKs(e *canal.RowsEvent, row []interface{}) string {
	eventPKs := ""
	pks, err := e.Table.GetPKValues(row)
	if err != nil {
		eventPKs = uuid.New().String()
	} else {
		eventPKsArray := make([]string, len(pks))
		for i, pk := range pks {
			eventPKsArray[i] = fmt.Sprintf("%v", pk)
		}
		eventPKs = strings.Join(eventPKsArray, "-")
	}

	return eventPKs

}

func (h *binlogHandler) convertValue(v interface{}) interface{} {
	switch value := v.(type) {
	case []byte:
		return string(value)
	case time.Time:
		return value.UTC()
	default:
		return value
	}
}

func (h *binlogHandler) OnRow(e *canal.RowsEvent) error {

	columns := []string{}
	for _, column := range e.Table.Columns {
		columns = append(columns, column.Name)
	}

	// prepare Before/After Value
	beforeValue := make(map[string]interface{})
	afterValue := make(map[string]interface{})
	result := CDCEvent{}

	for i, row := range e.Rows {

		if e.Header == nil {
			result = CDCEvent{}
			for seq, rowData := range row {
				afterValue[columns[seq]] = rowData
			}

			result = CDCEvent{
				Operation: SnapshotOperation,
				Table:     e.Table.Name,
				After:     afterValue,
				Before:    beforeValue,
			}

			pos, _ := h.canal.GetMasterPos()
			result.PosName = pos.Name
			result.Pos = pos.Pos
			result.EventPKs = h.joinPKs(e, row)
			h.fn(&result)

			timer := time.NewTimer(50 * time.Microsecond)
			<-timer.C

			continue
		}

		// Prepare CDC event
		switch e.Action {
		case canal.DeleteAction:
			result = CDCEvent{}
			for seq, rowData := range row {
				beforeValue[columns[seq]] = h.convertValue(rowData)
			}
			result = CDCEvent{
				Operation: DeleteOperation,
				Table:     e.Table.Name,
				After:     afterValue,
				Before:    beforeValue,
			}

			break

		case canal.UpdateAction:
			if i%2 == 0 {
				result = CDCEvent{}
				for seq, rowData := range row {
					beforeValue[columns[seq]] = h.convertValue(rowData)
				}
				result = CDCEvent{
					Operation: UpdateOperation,
					Table:     e.Table.Name,
					Before:    beforeValue,
				}
				continue
			} else if i%2 != 0 {
				for seq, rowData := range row {
					afterValue[columns[seq]] = h.convertValue(rowData)
				}

				result = CDCEvent{
					Operation: UpdateOperation,
					Table:     e.Table.Name,
					Before:    beforeValue,
					After:     afterValue,
				}
			}

			break

		case canal.InsertAction:
			result = CDCEvent{}
			for seq, rowData := range row {
				afterValue[columns[seq]] = h.convertValue(rowData)
			}

			result = CDCEvent{
				Operation: InsertOperation,
				Table:     e.Table.Name,
				After:     afterValue,
				Before:    beforeValue,
			}

			break
		}

		pos, _ := h.canal.GetMasterPos()
		result.PosName = pos.Name
		result.Pos = pos.Pos
		result.EventPKs = h.joinPKs(e, row)
		h.fn(&result)

	}

	return nil
}
