package adapter

import (
	//"errors"

	parser "git.brobridge.com/gravity/gravity-adapter-mysql/pkg/adapter/service/parser"
)

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
	After     map[string]*parser.Value
	Before    map[string]*parser.Value
}
