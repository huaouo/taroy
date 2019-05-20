// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package executor

import (
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/huaouo/taroy/ast"
	"github.com/huaouo/taroy/rpc"
	"github.com/vmihailenco/msgpack"
	"log"
	"time"
)

const (
	tablePrefix      = "@TP@"
	queryOkPattern   = "Query OK, %d rows affected (%.2f sec)"
	rowsInSetPattern = "%d row(s) in set (%.2f sec)"
	emptySet         = "Empty set (%.2f sec)"
)

var (
	internalErrorMessage = &rpc.ResultSet{Message: "Internal error"}
)

type Txn struct {
	kvTxn *badger.Txn
	s     *seq
}

func (txn *Txn) Execute(plan interface{}) *rpc.ResultSet {
	switch plan.(type) {
	case ast.CreateTableStmt:
		return txn.createTable(plan.(*ast.CreateTableStmt))
	case ast.DropTableStmt:
		return txn.dropTable(plan.(*ast.DropTableStmt))
	case ast.ShowStmt:
		return txn.show(plan.(*ast.ShowStmt))
	}

	log.Println("Plan not implemented")
	return internalErrorMessage
}

func (txn *Txn) Commit() error {
	ts, err := txn.s.nextTs()
	if err != nil {
		return err
	}
	return txn.kvTxn.CommitAt(ts, nil)
}

// Table Metadata Structure:
//   Key: tablePrefix + TableName
//   Value: []ast.Field
func (txn *Txn) createTable(stmt *ast.CreateTableStmt) *rpc.ResultSet {
	start := time.Now()
	tableNameBytes := []byte(tablePrefix + stmt.TableName)
	switch _, err := txn.kvTxn.Get(tableNameBytes); err {
	case nil:
		return &rpc.ResultSet{
			Message: fmt.Sprintf("Table '%s' already exists", stmt.TableName),
		}
	case badger.ErrKeyNotFound:
		break
	default:
		log.Printf("Check Table Error: %v\n", err)
		return internalErrorMessage
	}

	fieldBytes, err := msgpack.Marshal(stmt.Fields)
	if err != nil {
		log.Printf("Marshal error: %v\n", err)
		return internalErrorMessage
	}
	err = txn.kvTxn.Set(tableNameBytes, fieldBytes)
	if err != nil {
		log.Printf("Write error: %v\n", err)
		return internalErrorMessage
	}
	return &rpc.ResultSet{
		Message: fmt.Sprintf(queryOkPattern, 0, time.Since(start).Seconds()),
	}
}

// Drop table metadata and table contents.
// Table Content Structure:
//   Key: TableName + "@" + ...
//   Value: ...
func (txn *Txn) dropTable(stmt *ast.DropTableStmt) *rpc.ResultSet {
	start := time.Now()
	tableNameBytes := []byte(tablePrefix + stmt.TableName)
	switch _, err := txn.kvTxn.Get(tableNameBytes); err {
	case nil:
		break
	case badger.ErrKeyNotFound:
		return &rpc.ResultSet{
			Message: fmt.Sprintf("Unknown table '%s'", stmt.TableName),
		}
	default:
		log.Printf("Check Table Error: %v\n", err)
		return internalErrorMessage
	}
	err := txn.kvTxn.Delete(tableNameBytes)
	if err != nil {
		log.Printf("Delete error: %v\n", err)
		return internalErrorMessage
	}

	it := txn.kvTxn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	tableContentPrefixBytes := []byte(stmt.TableName + "@")
	for it.Seek(tableContentPrefixBytes); it.ValidForPrefix(tableContentPrefixBytes); it.Next() {
		k := it.Item().Key()
		err := txn.kvTxn.Delete(k)
		if err != nil {
			log.Printf("Delete error: %v\n", err)
			return internalErrorMessage
		}
	}
	return &rpc.ResultSet{
		Message: fmt.Sprintf(queryOkPattern, 0, time.Since(start).Seconds()),
	}
}

// If stmt.ShowTables == true:
//   Scan keys prefixed by tablePrefix, return collection of all table names
// Else:
//   Return table def.
func (txn *Txn) show(stmt *ast.ShowStmt) *rpc.ResultSet {
	start := time.Now()
	if stmt.ShowTables {
		it := txn.kvTxn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		results := &rpc.ResultSet{}
		results.Table = append(results.Table,
			&rpc.Row{Fields: []*rpc.Value{{Val: &rpc.Value_StrVal{StrVal: "Tables"}}}})
		tablePrefixBytes := []byte(tablePrefix)
		for it.Seek(tablePrefixBytes); it.ValidForPrefix(tablePrefixBytes); it.Next() {
			tableName := string(it.Item().Key())[len(tablePrefix):]
			results.Table = append(results.Table,
				&rpc.Row{Fields: []*rpc.Value{{Val: &rpc.Value_StrVal{StrVal: tableName}}}})
		}
		if len(results.Table) == 1 {
			results.Table = nil
			results.Message = fmt.Sprintf(emptySet, time.Since(start).Seconds())
		} else {
			results.Message = fmt.Sprintf(rowsInSetPattern, len(results.Table)-1, time.Since(start).Seconds())
		}
		return results
	} else {
		it := txn.kvTxn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		tableNameBytes := []byte(tablePrefix + stmt.TableName)
		it.Seek(tableNameBytes)
		if string(it.Item().Key())[len(tablePrefix):] != stmt.TableName {
			return &rpc.ResultSet{
				Message: fmt.Sprintf("Table '%s' doesn't exist", stmt.TableName),
			}
		}
		results := &rpc.ResultSet{}
		results.Table = append(results.Table,
			&rpc.Row{Fields: []*rpc.Value{
				{Val: &rpc.Value_StrVal{StrVal: "Field"}},
				{Val: &rpc.Value_StrVal{StrVal: "Type"}},
				{Val: &rpc.Value_StrVal{StrVal: "Tag"}},
			}})
		fieldBytes, err := it.Item().Value()
		if err != nil {
			log.Printf("Read error: %v\n", err)
			return internalErrorMessage
		}
		var fields []ast.Field
		err = msgpack.Unmarshal(fieldBytes, &fields)
		if err != nil {
			log.Printf("Unmarshal error: %v\n", err)
			return internalErrorMessage
		}

		for _, f := range fields {
			results.Table = append(results.Table,
				&rpc.Row{Fields: []*rpc.Value{
					{Val: &rpc.Value_StrVal{StrVal: f.FieldName}},
					{Val: &rpc.Value_StrVal{StrVal: f.FieldType.String()}},
					{Val: &rpc.Value_StrVal{StrVal: f.FieldTag.String()}},
				}})
		}
		results.Message = fmt.Sprintf(rowsInSetPattern, len(results.Table)-1, time.Since(start).Seconds())
		return results
	}
}
