// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package executor

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/huaouo/taroy/ast"
	"github.com/huaouo/taroy/rpc"
	"github.com/vmihailenco/msgpack"
	"log"
	"math"
	"time"
)

const (
	tablePrefix              = "@TP@"
	tablePrimaryNumberPrefix = "@TaP@"

	queryOkPattern       = "Query OK, %d row(s) affected (%.2f sec)"
	rowsInSetPattern     = "%d row(s) in set (%.2f sec)"
	emptySetPattern      = "Empty set (%.2f sec)"
	tableNotExistPattern = "Table '%s' doesn't exist"

	multiplePrimaryKey = -1
	noPrimaryKey       = 255
)

var internalError = errors.New("internal error")
var internalErrorMessage = &rpc.ResultSet{
	Message:  "Internal error",
	FailFlag: true,
}

func msgpackMarshal(v interface{}) ([]byte, error) {
	bytes, err := msgpack.Marshal(v)
	if err != nil {
		log.Printf("Marshal error: %v\n", err)
	}
	return bytes, err
}

func msgpackUnmarshal(data []byte, v interface{}) error {
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

func (txn *Txn) getKv(key []byte) ([]byte, error) {
	item, err := txn.kvTxn.Get(key)
	if err != nil {
		log.Printf("Get error: %v\n", err)
		return nil, err
	}
	return item.ValueCopy(nil)
}

func (txn *Txn) setKv(key []byte, value []byte) error {
	err := txn.kvTxn.Set(key, value)
	if err == nil {
		// TODO: do it without creating a new object
		txn.modifiedKeys[string(key)] = true
	} else {
		log.Printf("Set error: %v\n", err)
	}
	return err
}

func (txn *Txn) deleteKv(key []byte) error {
	err := txn.kvTxn.Delete(key)
	if err == nil {
		// TODO: do it without creating a new object
		txn.modifiedKeys[string(key)] = true
	} else {
		log.Printf("Delete error: %v\n", err)
	}
	return err
}

func (txn *Txn) isKeyExists(key []byte) (bool, error) {
	switch _, err := txn.kvTxn.Get(key); err {
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
	case ast.InsertStmt:
		return txn.insert(plan.(ast.InsertStmt))
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

	ts, err := txn.s.getNext()
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

func getPrimaryKeyIndex(fields []ast.Field) int {
	index := noPrimaryKey
	for i, f := range fields {
		if f.FieldTag == ast.PRIMARY {
			if index != noPrimaryKey {
				return multiplePrimaryKey
			}
			index = i
		}
	}
	return index
}

func getKeyIndicesExceptPrimaryByTag(fields []ast.Field, tag ast.FieldTag) []int {
	var indices []int
	for i, f := range fields {
		if f.FieldTag == tag {
			indices = append(indices, i)
		}
	}
	return indices
}

func getBytes(strs ...interface{}) []byte {
	var bytes []byte
	for _, str := range strs {
		switch str.(type) {
		case string:
			bytes = append(bytes, []byte(str.(string))...)
		case []byte:
			bytes = append(bytes, str.([]byte)...)
		case byte:
			bytes = append(bytes, str.(byte))
		case rune:
			bytes = append(bytes, byte(str.(rune)))
		}
	}
	return bytes
}

// Table Metadata Structure:
//   Key: tablePrefix + TableName
//   Value: []ast.Field
func (txn *Txn) createTable(stmt ast.CreateTableStmt) *rpc.ResultSet {
	start := time.Now()

	if len(stmt.Fields) > math.MaxUint8-1 {
		return &rpc.ResultSet{
			// 255 is used to denote an additional primary key when there's
			// no primary key specified.
			Message:  "Number of fields should be less than 255",
			FailFlag: true,
		}
	}

	if getPrimaryKeyIndex(stmt.Fields) == multiplePrimaryKey {
		return &rpc.ResultSet{
			Message:  "Multiple primary key defined",
			FailFlag: true,
		}
	}

	tableMetadataKey := getBytes(tablePrefix, stmt.TableName)
	exist, err := txn.isKeyExists(tableMetadataKey)
	if err != nil {
		return internalErrorMessage
	} else if exist {
		return &rpc.ResultSet{
			Message:  fmt.Sprintf("Table '%s' already exists", stmt.TableName),
			FailFlag: true,
		}
	}

	fieldBytes, err := msgpackMarshal(stmt.Fields)
	if err != nil {
		return internalErrorMessage
	}
	err = txn.setKv(tableMetadataKey, fieldBytes)
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
	tableMetadataKey := getBytes(tablePrefix, stmt.TableName)
	exist, err := txn.isKeyExists(tableMetadataKey)
	if err != nil {
		return internalErrorMessage
	} else if !exist {
		return &rpc.ResultSet{
			Message:  fmt.Sprintf(tableNotExistPattern, stmt.TableName),
			FailFlag: true,
		}
	}
	err = txn.deleteKv(tableMetadataKey)
	if err != nil {
		return internalErrorMessage
	}

	primaryNumberKey := getBytes(tablePrimaryNumberPrefix, stmt.TableName)
	exist, err = txn.isKeyExists(primaryNumberKey)
	if err != nil {
		return internalErrorMessage
	} else if exist {
		err = txn.deleteKv(primaryNumberKey)
		if err != nil {
			return internalErrorMessage
		}
	}

	it := txn.kvTxn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	tableContentPrefixBytes := []byte(stmt.TableName + "@")
	for it.Seek(tableContentPrefixBytes); it.ValidForPrefix(tableContentPrefixBytes); it.Next() {
		k := it.Item().Key()
		err := txn.deleteKv(k)
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
		err = msgpackUnmarshal(fieldBytes, &fields)
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

func uint64ToBytesBE(val uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, val)
	return buf
}

type pendingWrite struct {
	key []byte
	val []byte
}

func i64ToU64(i int64) uint64 {
	var u uint64
	if i >= 0 {
		u = uint64(i) + math.MaxInt64 + 1
	} else {
		u = uint64(i + math.MaxInt64 + 1)
	}
	return u
}

func u64ToI64(u uint64) int64 {
	var i int64
	if u <= math.MaxInt64 {
		i = int64(u) - math.MaxInt64 - 1
	} else {
		i = int64(u - math.MaxInt64 - 1)
	}
	return i
}

func (txn *Txn) insert(stmt ast.InsertStmt) *rpc.ResultSet {
	start := time.Now()

	tableMetadataKey := getBytes(tablePrefix, stmt.TableName)
	exist, err := txn.isKeyExists(tableMetadataKey)
	if err != nil {
		return internalErrorMessage
	} else if !exist {
		return &rpc.ResultSet{
			Message:  fmt.Sprintf(tableNotExistPattern, stmt.TableName),
			FailFlag: true,
		}
	}

	fieldBytes, err := txn.getKv(tableMetadataKey)
	if err != nil {
		return internalErrorMessage
	}
	var fields []ast.Field
	err = msgpackUnmarshal(fieldBytes, &fields)
	if err != nil {
		return internalErrorMessage
	}
	if len(fields) != len(stmt.Values) {
		return &rpc.ResultSet{
			Message:  "Column count doesn't match",
			FailFlag: true,
		}
	}

	var pendingWrites []pendingWrite

	// Handle primary key
	primaryIndex := getPrimaryKeyIndex(fields)
	var primaryKeyPart []byte
	if primaryIndex == noPrimaryKey {
		primarySeq := getSeq(tablePrimaryNumberPrefix + stmt.TableName)
		next, err := primarySeq.getNext()
		if err != nil {
			return internalErrorMessage
		}
		primaryKeyPart = uint64ToBytesBE(next)
	} else {
		switch v := stmt.Values[primaryIndex]; v.(type) {
		case string:
			primaryKeyPart = []byte(v.(string))
		case int64:
			primaryKeyPart = uint64ToBytesBE(i64ToU64(v.(int64)))
		}
	}
	primaryKey := getBytes(stmt.TableName, byte('@'), byte(primaryIndex), byte('@'), primaryKeyPart)
	exist, err = txn.isKeyExists(primaryKey)
	if err != nil {
		return internalErrorMessage
	} else if exist {
		return &rpc.ResultSet{
			Message:  fmt.Sprint("Duplicate entry for key 'PRIMARY'"),
			FailFlag: true,
		}
	}
	primaryContent, _ := msgpackMarshal(stmt.Values)
	pendingWrites = append(pendingWrites, pendingWrite{key: primaryKey, val: primaryContent})

	// Handle unique key
	uniqueKeyIndices := getKeyIndicesExceptPrimaryByTag(fields, ast.UNIQUE)
	for _, i := range uniqueKeyIndices {
		var uniqueKeyPart []byte
		switch v := stmt.Values[i]; v.(type) {
		case string:
			uniqueKeyPart = []byte(v.(string))
		case int64:
			uniqueKeyPart = uint64ToBytesBE(i64ToU64(v.(int64)))
		}
		uniqueKey := getBytes(stmt.TableName, byte('@'), byte(i), byte('@'), uniqueKeyPart)
		exist, err = txn.isKeyExists(uniqueKey)
		if err != nil {
			return internalErrorMessage
		} else if exist {
			return &rpc.ResultSet{
				Message:  fmt.Sprint("Duplicate entry for key 'UNIQUE'"),
				FailFlag: true,
			}
		}
		pendingWrites = append(pendingWrites, pendingWrite{key: uniqueKey, val: primaryKeyPart})
	}

	// Handle index key
	indexKeyIndices := getKeyIndicesExceptPrimaryByTag(fields, ast.INDEX)
	for _, i := range indexKeyIndices {
		var indexKeyPart []byte
		switch v := stmt.Values[i]; v.(type) {
		case string:
			indexKeyPart = []byte(v.(string))
		case int64:
			indexKeyPart = uint64ToBytesBE(i64ToU64(v.(int64)))
		}
		indexKeyPart = append(indexKeyPart, byte('@'))
		indexKeyPart = append(indexKeyPart, primaryKeyPart...)

		indexKey := getBytes(stmt.TableName, byte('@'), byte(i), byte('@'), indexKeyPart)
		pendingWrites = append(pendingWrites, pendingWrite{key: indexKey, val: []byte{}})
	}

	for _, w := range pendingWrites {
		err := txn.setKv(w.key, w.val)
		if err != nil {
			return internalErrorMessage
		}
	}

	return &rpc.ResultSet{
		Message: fmt.Sprintf(queryOkPattern, 1, time.Since(start).Seconds()),
	}
}
