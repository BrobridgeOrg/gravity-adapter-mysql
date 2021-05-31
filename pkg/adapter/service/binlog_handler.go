package adapter

import (
	//"fmt"
	parser "git.brobridge.com/gravity/gravity-adapter-mysql/pkg/adapter/service/parser"
	"github.com/go-mysql-org/go-mysql/canal"
	"sync"
	//"github.com/siddontang/go-mysql/mysql"
	//"github.com/siddontang/go-mysql/schema"
	//log "github.com/sirupsen/logrus"
	"time"
)

type binlogHandler struct {
	canal.DummyEventHandler // Dummy handler from external lib
	fn                      func(*CDCEvent)
	canal                   *canal.Canal
	mu                      sync.Mutex
}

func (h *binlogHandler) OnRow(e *canal.RowsEvent) error {

	columns := []string{}
	for _, column := range e.Table.Columns {
		columns = append(columns, column.Name)
	}

	// prepare Before/After Value
	beforeValue := make(map[string]*parser.Value)
	afterValue := make(map[string]*parser.Value)
	result := CDCEvent{}

	for i, row := range e.Rows {

		h.mu.Lock()

		// Prepare CDC event
		switch e.Action {
		case canal.DeleteAction:
			result = CDCEvent{}
			for seq, rowData := range row {
				beforeValue[columns[seq]] = &parser.Value{
					Data: rowData,
				}
			}
			result = CDCEvent{
				Operation: DeleteOperation,
				Table:     e.Table.Name,
				After:     afterValue,
				Before:    beforeValue,
			}

			pos, _ := h.canal.GetMasterPos()
			result.PosName = pos.Name
			result.Pos = pos.Pos
			h.fn(&result)

			timer := time.NewTimer(50 * time.Microsecond)
			<-timer.C
			break

		case canal.UpdateAction:
			if i%2 == 0 {
				result = CDCEvent{}
				for seq, rowData := range row {
					beforeValue[columns[seq]] = &parser.Value{
						Data: rowData,
					}
				}
				result = CDCEvent{
					Operation: UpdateOperation,
					Table:     e.Table.Name,
					Before:    beforeValue,
				}
			} else if i%2 != 0 {
				for seq, rowData := range row {
					afterValue[columns[seq]] = &parser.Value{
						Data: rowData,
					}
				}

				result = CDCEvent{
					Operation: UpdateOperation,
					Table:     e.Table.Name,
					Before:    beforeValue,
					After:     afterValue,
				}
				pos, _ := h.canal.GetMasterPos()
				result.PosName = pos.Name
				result.Pos = pos.Pos
				h.fn(&result)

				timer := time.NewTimer(50 * time.Microsecond)
				<-timer.C

			}

			break

		case canal.InsertAction:
			result = CDCEvent{}
			for seq, rowData := range row {
				afterValue[columns[seq]] = &parser.Value{
					Data: rowData,
				}
			}

			result = CDCEvent{
				Operation: InsertOperation,
				Table:     e.Table.Name,
				After:     afterValue,
				Before:    beforeValue,
			}

			pos, _ := h.canal.GetMasterPos()
			result.PosName = pos.Name
			result.Pos = pos.Pos
			h.fn(&result)

			timer := time.NewTimer(50 * time.Microsecond)
			<-timer.C
			break

		}

		if e.Header == nil {
			result = CDCEvent{}
			for seq, rowData := range row {
				afterValue[columns[seq]] = &parser.Value{
					Data: rowData,
				}
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
			h.fn(&result)

			timer := time.NewTimer(50 * time.Microsecond)
			<-timer.C
		}

		h.mu.Unlock()

	}

	return nil
}
