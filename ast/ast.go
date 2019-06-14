// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package ast

type FieldType int

const (
	NIL FieldType = iota
	INT
	STRING
)

var fieldTypeNames = map[FieldType]string{
	NIL:    "",
	INT:    "INT",
	STRING: "STRING",
}

func (t FieldType) String() string {
	return fieldTypeNames[t]
}

type FieldTag int

const (
	UNTAGGED FieldTag = iota
	PRIMARY
	UNIQUE
	INDEX
)

var fieldTagNames = map[FieldTag]string{
	UNTAGGED: "",
	PRIMARY:  "PRIMARY",
	UNIQUE:   "UNIQUE",
	INDEX:    "INDEX",
}

func (t FieldTag) String() string {
	return fieldTagNames[t]
}

type Field struct {
	FieldName string
	FieldType
	FieldTag
}

type CreateTableStmt struct {
	TableName string
	Fields    []Field
}

type DropTableStmt struct {
	TableName string
}

type CmpOp int

const (
	LT CmpOp = iota
	LE
	EQ
	GE
	GT
	NE
	BETWEEN
)

type WhereClause struct {
	FieldName string
	CmpOp
	Value    interface{}
	ValueOpt interface{} // for BETWEEN
}

type SelectStmt struct {
	TableName  string
	FieldNames []string
	Where      *WhereClause
}

type InsertStmt struct {
	TableName string
	Values    []interface{}
}

type DeleteStmt struct {
	TableName string
	Where     *WhereClause
}

type UpdatePair struct {
	FieldName string
	Value     interface{}
}

type UpdateStmt struct {
	TableName   string
	UpdatePairs []UpdatePair
	Where       *WhereClause
}

type BeginStmt struct{}

type RollbackStmt struct{}

type CommitStmt struct{}

type ShowStmt struct {
	ShowTables bool
	TableName  string
}

type Stmts []interface{}
