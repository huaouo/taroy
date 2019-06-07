// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package executor

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/huaouo/taroy/ast"
	"github.com/huaouo/taroy/rpc"
	"github.com/vmihailenco/msgpack"
	"log"
	"time"
)

const (
	tablePrefix = "@TP@"

	queryOkPattern       = "Query OK, %d row(s) affected (%.2f sec)"
	rowsInSetPattern     = "%d row(s) in set (%.2f sec)"
	emptySetPattern      = "Empty set (%.2f sec)"
	tableNotExistPattern = "Table '%s' doesn't exist"
)

var internalError = errors.New("internal error")
var internalErrorMessage = &rpc.ResultSet{
	Message:  "Internal error",
	FailFlag: true,
}

func marshal(v interface{}) ([]byte, error) {
	bytes, err := msgpack.Marshal(v)
	if err != nil {
		log.Printf("Marshal error: %v\n", err)
	}
	return bytes, err
}

func unmarshal(data []byte, v interface{}) error {
	err := msgpack.Unmarshal(data, v)
	if err != nil {
		log.Printf("Unmarshal error: %v\n", err)
	}
	return err
}

type Txn struct {
	kvTxn        *badger.Txn
	readTs       uint64
	s            *seq
	modifiedKeys map[string]bool
}

func (txn *Txn) getKv(key string) ([]byte, error) {
	item, err := txn.kvTxn.Get([]byte(key))
	if err != nil {
		log.Printf("Get error: %v\n", err)
		return nil, err
	}
	return item.ValueCopy(nil)
}

func (txn *Txn) setKv(key string, value []byte) error {
	err := txn.kvTxn.Set([]byte(key), value)
	if err == nil {
		txn.modifiedKeys[key] = true
	} else {
		log.Printf("Set error: %v\n", err)
	}
	return err
}

func (txn *Txn) deleteKv(key string) error {
	err := txn.kvTxn.Delete([]byte(key))
	if err == nil {
		txn.modifiedKeys[key] = true
	} else {
		log.Printf("Delete error: %v\n", err)
	}
	return err
}

func (txn *Txn) isKeyExists(key string) (bool, error) {
	switch _, err := txn.getKv(key); err {
	case nil:
		return true, nil
	case badger.ErrKeyNotFound:
		return false, nil
	default:
		return false, internalError
	}
}

func (txn *Txn) Valid() bool {
	return txn.kvTxn != nil
}

func (txn *Txn) Execute(plan interface{}) *rpc.ResultSet {
	switch plan.(type) {
	case ast.CreateTableStmt:
		return txn.createTable(plan.(ast.CreateTableStmt))
	case ast.DropTableStmt:
		return txn.dropTable(plan.(ast.DropTableStmt))
	case ast.ShowStmt:
		return txn.show(plan.(ast.ShowStmt))
	}

	log.Println("Plan not implemented")
	return internalErrorMessage
}

func (txn *Txn) Rollback() {
	txn.kvTxn.Discard()
	txn.kvTxn = nil
}

func (txn *Txn) Commit() *rpc.ResultSet {
	start := time.Now()
	db := GetDb()
	db.Lock()
	defer db.Unlock()
	defer txn.Rollback()

	commitFailureMessage := &rpc.ResultSet{
		Message:  fmt.Sprintf("Commit failed, rollback automatically"),
		FailFlag: true,
	}
	for k := range txn.modifiedKeys {
		if ts, err := db.getLatestVersion(k); err != nil && ts > txn.readTs {
			return commitFailureMessage
		}
	}

	ts, err := txn.s.nextTs()
	if err != nil {
		return commitFailureMessage
	}
	err = txn.kvTxn.CommitAt(ts, nil)
	if err != nil {
		return commitFailureMessage
	}
	return &rpc.ResultSet{
		Message: fmt.Sprintf(queryOkPattern, 0, time.Since(start).Seconds()),
	}
}

// Table Metadata Structure:
//   Key: tablePrefix + TableName
//   Value: []ast.Field
func (txn *Txn) createTable(stmt ast.CreateTableStmt) *rpc.ResultSet {
	start := time.Now()
	tableName := tablePrefix + stmt.TableName
	exist, err := txn.isKeyExists(tableName)
	if err != nil {
		return internalErrorMessage
	} else if exist {
		return &rpc.ResultSet{
			Message:  fmt.Sprintf("Table '%s' already exists", stmt.TableName),
			FailFlag: true,
		}
	}

	fieldBytes, err := marshal(stmt.Fields)
	if err != nil {
		return internalErrorMessage
	}
	err = txn.setKv(tableName, fieldBytes)
	if err != nil {
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
func (txn *Txn) dropTable(stmt ast.DropTableStmt) *rpc.ResultSet {
	start := time.Now()
	tableName := tablePrefix + stmt.TableName
	exist, err := txn.isKeyExists(tableName)
	if err != nil {
		return internalErrorMessage
	} else if !exist {
		return &rpc.ResultSet{
			Message:  fmt.Sprintf(tableNotExistPattern, stmt.TableName),
			FailFlag: true,
		}
	}
	err = txn.deleteKv(tableName)
	if err != nil {
		return internalErrorMessage
	}

	it := txn.kvTxn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	tableContentPrefixBytes := []byte(stmt.TableName + "@")
	for it.Seek(tableContentPrefixBytes); it.ValidForPrefix(tableContentPrefixBytes); it.Next() {
		k := it.Item().Key()
		err := txn.deleteKv(string(k))
		if err != nil {
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
func (txn *Txn) show(stmt ast.ShowStmt) *rpc.ResultSet {
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
			results.Message = fmt.Sprintf(emptySetPattern, time.Since(start).Seconds())
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
				Message:  fmt.Sprintf(tableNotExistPattern, stmt.TableName),
				FailFlag: true,
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
			return internalErrorMessage
		}
		var fields []ast.Field
		err = unmarshal(fieldBytes, &fields)
		if err != nil {
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
