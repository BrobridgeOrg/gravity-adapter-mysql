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
	updateEvent := make(map[string]*CDCEvent, 0)

	for i, row := range e.Rows {

		pos, _ := h.canal.GetMasterPos()
		if e.Header == nil {
			afterValue := make(map[string]interface{}, len(row))
			result := cdcEventPool.Get().(*CDCEvent)
			for seq, rowData := range row {
				afterValue[columns[seq]] = rowData
			}

			/*
				result = CDCEvent{
					Operation: SnapshotOperation,
					Table:     e.Table.Name,
					After:     afterValue,
					Before:    beforeValue,
				}
			*/
			result.Operation = SnapshotOperation
			result.Table = e.Table.Name
			result.After = afterValue
			result.Before = nil

			result.PosName = pos.Name
			result.Pos = pos.Pos
			result.EventPKs = h.joinPKs(e, row)
			h.fn(result)

			timer := time.NewTimer(50 * time.Microsecond)
			<-timer.C

			continue
		}

		// Prepare CDC event
		switch e.Action {
		case canal.DeleteAction:
			beforeValue := make(map[string]interface{}, len(row))
			result := cdcEventPool.Get().(*CDCEvent)
			for seq, rowData := range row {
				beforeValue[columns[seq]] = h.convertValue(rowData)
			}
			/*
				result = CDCEvent{
					Operation: DeleteOperation,
					Table:     e.Table.Name,
					After:     afterValue,
					Before:    beforeValue,
				}
			*/
			result.Operation = DeleteOperation
			result.Table = e.Table.Name
			result.After = nil
			result.Before = beforeValue

			result.PosName = pos.Name
			result.Pos = pos.Pos
			result.EventPKs = h.joinPKs(e, row)
			h.fn(result)
			break

		case canal.UpdateAction:
			updateKey := fmt.Sprintf("%s-%d", pos.Name, pos.Pos)
			if i%2 == 0 {
				beforeValue := make(map[string]interface{}, len(row))
				result := cdcEventPool.Get().(*CDCEvent)
				for seq, rowData := range row {
					beforeValue[columns[seq]] = h.convertValue(rowData)
				}
				/*
					result = CDCEvent{
						Operation: UpdateOperation,
						Table:     e.Table.Name,
						Before:    beforeValue,
					}
				*/
				result.Operation = UpdateOperation
				result.Table = e.Table.Name
				result.Before = beforeValue
				updateEvent[updateKey] = result
				continue
			} else if i%2 != 0 {
				afterValue := make(map[string]interface{}, len(row))
				result := updateEvent[updateKey]
				for seq, rowData := range row {
					afterValue[columns[seq]] = h.convertValue(rowData)
				}

				/*
					result = CDCEvent{
						Operation: UpdateOperation,
						Table:     e.Table.Name,
						Before:    beforeValue,
						After:     afterValue,
					}
				*/
				result.Operation = UpdateOperation
				result.Table = e.Table.Name
				result.After = afterValue
				result.PosName = pos.Name
				result.Pos = pos.Pos
				result.EventPKs = h.joinPKs(e, row)
				h.fn(result)
				delete(updateEvent, updateKey)
			}

			break

		case canal.InsertAction:
			afterValue := make(map[string]interface{}, len(row))
			result := cdcEventPool.Get().(*CDCEvent)
			for seq, rowData := range row {
				afterValue[columns[seq]] = h.convertValue(rowData)
			}

			/*
				result = CDCEvent{
					Operation: InsertOperation,
					Table:     e.Table.Name,
					After:     afterValue,
					Before:    beforeValue,
				}
			*/
			result.Operation = InsertOperation
			result.Table = e.Table.Name
			result.After = afterValue
			result.Before = nil

			result.PosName = pos.Name
			result.Pos = pos.Pos
			result.EventPKs = h.joinPKs(e, row)
			h.fn(result)
			break
		}

	}

	return nil
}
