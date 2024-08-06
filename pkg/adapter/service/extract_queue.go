package adapter

type OperationType int8

const (
	InsertOperation = OperationType(iota + 1)
	UpdateOperation
	DeleteOperation
	SnapshotOperation
)

type CDCEvent struct {
	Pos       uint32
	PosName   string
	Operation OperationType
	Table     string
	After     map[string]interface{}
	Before    map[string]interface{}
	EventPKs  string
}

func (database *Database) convertValue(v interface{}) interface{} {

	switch value := v.(type) {
	case []byte:
		return string(value)

	default:
		return value
	}

	return v
}

func (database *Database) processSnapshotEvent(tableName string, eventPayload map[string]interface{}) *CDCEvent {
	afterValue := make(map[string]interface{})

	for key, value := range eventPayload {
		afterValue[key] = database.convertValue(value)
	}

	result := CDCEvent{
		Operation: SnapshotOperation,
		Table:     tableName,
		After:     afterValue,
	}
	return &result

}
