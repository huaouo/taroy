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
var InternalErrorMessage = &rpc.ResultSet{
	Message:  "Internal error, all changes within this transaction have been discarded",
	FailFlag: true,
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

func (txn *Txn) Execute(stmt interface{}) *rpc.ResultSet {
	switch stmt.(type) {
	case ast.CreateTableStmt:
		return txn.createTable(stmt.(ast.CreateTableStmt))
	case ast.DropTableStmt:
		return txn.dropTable(stmt.(ast.DropTableStmt))
	case ast.ShowStmt:
		return txn.show(stmt.(ast.ShowStmt))
	case ast.InsertStmt:
		return txn.insert(stmt.(ast.InsertStmt))
	case ast.SelectStmt:
		return txn.sel(stmt.(ast.SelectStmt))
	case ast.DeleteStmt:
		return txn.del(stmt.(ast.DeleteStmt))
		//case ast.UpdateStmt:
		//	return txn.upd(stmt.(ast.UpdateStmt))
	}

	log.Println("Plan not implemented")
	return InternalErrorMessage
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

func getKeyIndicesExceptPrimaryByTag(fields []ast.Field, tag ast.FieldTag) []int {
	var indices []int
	for i, f := range fields {
		if f.FieldTag == tag {
			indices = append(indices, i)
		}
	}
	return indices
}

func (txn *Txn) getFields(tableName string) ([]ast.Field, error) {
	fieldBytes, err := txn.getKv([]byte(tablePrefix + tableName))
	if err != nil {
		return nil, err
	}
	var fields []ast.Field
	err = msgpackUnmarshal(fieldBytes, &fields)
	if err != nil {
		return nil, err
	}
	return fields, nil
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

	fieldNameMap := make(map[string]bool)
	for _, f := range stmt.Fields {
		if _, ok := fieldNameMap[f.FieldName]; ok {
			return &rpc.ResultSet{
				Message:  "Duplicate column name",
				FailFlag: true,
			}
		} else {
			fieldNameMap[f.FieldName] = true
		}
	}

	if getPrimaryKeyIndex(stmt.Fields) == multiplePrimaryKey {
		return &rpc.ResultSet{
			Message:  "Multiple primary key defined",
			FailFlag: true,
		}
	}

	tableMetadataKey := concatBytes(tablePrefix, stmt.TableName)
	exist, err := txn.isKeyExists(tableMetadataKey)
	if err != nil {
		return InternalErrorMessage
	} else if exist {
		return &rpc.ResultSet{
			Message:  fmt.Sprintf("Table '%s' already exists", stmt.TableName),
			FailFlag: true,
		}
	}

	fieldBytes, err := msgpackMarshal(stmt.Fields)
	if err != nil {
		return InternalErrorMessage
	}
	err = txn.setKv(tableMetadataKey, fieldBytes)
	if err != nil {
		return InternalErrorMessage
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
	tableMetadataKey := concatBytes(tablePrefix, stmt.TableName)
	exist, err := txn.isKeyExists(tableMetadataKey)
	if err != nil {
		return InternalErrorMessage
	} else if !exist {
		return &rpc.ResultSet{
			Message:  fmt.Sprintf(tableNotExistPattern, stmt.TableName),
			FailFlag: true,
		}
	}
	err = txn.deleteKv(tableMetadataKey)
	if err != nil {
		return InternalErrorMessage
	}

	primaryNumberKey := concatBytes(tablePrimaryNumberPrefix, stmt.TableName)
	exist, err = txn.isKeyExists(primaryNumberKey)
	if err != nil {
		return InternalErrorMessage
	} else if exist {
		err = txn.deleteKv(primaryNumberKey)
		if err != nil {
			return InternalErrorMessage
		}
	}

	it := txn.kvTxn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	tableContentPrefixBytes := []byte(stmt.TableName + "@")
	for it.Seek(tableContentPrefixBytes); it.ValidForPrefix(tableContentPrefixBytes); it.Next() {
		k := it.Item().Key()
		err := txn.deleteKv(k)
		if err != nil {
			return InternalErrorMessage
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
			return InternalErrorMessage
		}
		var fields []ast.Field
		err = msgpackUnmarshal(fieldBytes, &fields)
		if err != nil {
			return InternalErrorMessage
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

type pendingWrite struct {
	key []byte
	val []byte
}

// TODO: code clean up & add comments
func (txn *Txn) insert(stmt ast.InsertStmt) *rpc.ResultSet {
	start := time.Now()

	exist, err := txn.isKeyExists(concatBytes(tablePrefix, stmt.TableName))
	if err != nil {
		return InternalErrorMessage
	} else if !exist {
		return &rpc.ResultSet{
			Message:  fmt.Sprintf(tableNotExistPattern, stmt.TableName),
			FailFlag: true,
		}
	}

	fields, err := txn.getFields(stmt.TableName)
	if err != nil {
		return InternalErrorMessage
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
			return InternalErrorMessage
		}
		primaryKeyPart = packUint64(next)
	} else {
		primaryKeyPart = getValueBytes(stmt.Values[primaryIndex])
	}
	primaryKey := concatBytes(stmt.TableName, byte('@'), byte(primaryIndex), byte('@'), primaryKeyPart)
	exist, err = txn.isKeyExists(primaryKey)
	if err != nil {
		return InternalErrorMessage
	} else if exist {
		return &rpc.ResultSet{
			Message:  fmt.Sprint("Duplicate entry for 'PRIMARY' key"),
			FailFlag: true,
		}
	}
	primaryContent, _ := msgpackMarshal(stmt.Values)
	pendingWrites = append(pendingWrites, pendingWrite{key: primaryKey, val: primaryContent})

	// Handle unique key
	uniqueKeyIndices := getKeyIndicesExceptPrimaryByTag(fields, ast.UNIQUE)
	for _, i := range uniqueKeyIndices {
		uniqueKeyPart := getValueBytes(stmt.Values[i])
		uniqueKey := concatBytes(stmt.TableName, byte('@'), byte(i), byte('@'), uniqueKeyPart)
		exist, err = txn.isKeyExists(uniqueKey)
		if err != nil {
			return InternalErrorMessage
		} else if exist {
			return &rpc.ResultSet{
				Message:  fmt.Sprint("Duplicate entry for 'UNIQUE' key"),
				FailFlag: true,
			}
		}
		pendingWrites = append(pendingWrites, pendingWrite{key: uniqueKey, val: primaryKeyPart})
	}

	// Handle index key
	indexKeyIndices := getKeyIndicesExceptPrimaryByTag(fields, ast.INDEX)
	for _, i := range indexKeyIndices {
		indexKeyPart := concatBytes(getValueBytes(stmt.Values[i]), byte('@'), primaryKeyPart)
		indexKey := concatBytes(stmt.TableName, byte('@'), byte(i), byte('@'), indexKeyPart)
		pendingWrites = append(pendingWrites, pendingWrite{key: indexKey, val: []byte{}})
	}

	for _, w := range pendingWrites {
		err := txn.setKv(w.key, w.val)
		if err != nil {
			return InternalErrorMessage
		}
	}

	return &rpc.ResultSet{
		Message: fmt.Sprintf(queryOkPattern, 1, time.Since(start).Seconds()),
	}
}

func (txn *Txn) sel(stmt ast.SelectStmt) *rpc.ResultSet {
	start := time.Now()
	exist, err := txn.isKeyExists([]byte(tablePrefix + stmt.TableName))
	if err != nil {
		return InternalErrorMessage
	} else if !exist {
		return &rpc.ResultSet{
			Message:  fmt.Sprintf(tableNotExistPattern, stmt.TableName),
			FailFlag: true,
		}
	}

	var fieldIndices []int
	fields, err := txn.getFields(stmt.TableName)
	if err != nil {
		return InternalErrorMessage
	}
outer:
	for _, fieldName := range stmt.FieldNames {
		if fieldName == "*" {
			for i := 0; i < len(fields); i++ {
				fieldIndices = append(fieldIndices, i)
			}
		} else {
			for i, realField := range fields {
				if realField.FieldName == fieldName {
					fieldIndices = append(fieldIndices, i)
					continue outer
				}
			}
			return &rpc.ResultSet{
				Message:  "Unknown column",
				FailFlag: true,
			}
		}
	}

	header := &rpc.Row{}
	for _, i := range fieldIndices {
		header.Fields = append(header.Fields, &rpc.Value{Val: &rpc.Value_StrVal{StrVal: fields[i].FieldName}})
	}
	var results rpc.ResultSet
	results.Table = append(results.Table, header)

	pIt, err := newPlanIterator(txn, stmt.TableName, stmt.Where)
	switch err {
	case nil:
	case errInvalidField:
		return &rpc.ResultSet{
			Message:  "Unknown column",
			FailFlag: true,
		}
	case errFieldTypeMismatch:
		return &rpc.ResultSet{
			Message:  "Column type mismatch",
			FailFlag: true,
		}
	default:
		return InternalErrorMessage
	}
	defer pIt.close()

	for pIt.valid() {
		tuple, err := pIt.value()
		if err != nil {
			return InternalErrorMessage
		}

		row := &rpc.Row{}
		var val *rpc.Value
		for _, i := range fieldIndices {
			switch fields[i].FieldType {
			case ast.INT:
				val = &rpc.Value{Val: &rpc.Value_IntVal{IntVal: tuple[i].(int64)}}
			case ast.STRING:
				val = &rpc.Value{Val: &rpc.Value_StrVal{StrVal: tuple[i].(string)}}
			}
			row.Fields = append(row.Fields, val)
		}
		results.Table = append(results.Table, row)
		pIt.next()
	}
	if resultCnt := len(results.Table) - 1; resultCnt == 0 {
		results.Table = nil
		results.Message = fmt.Sprintf(emptySetPattern, time.Since(start).Seconds())
	} else {
		results.Message = fmt.Sprintf(rowsInSetPattern, len(results.Table)-1, time.Since(start).Seconds())
	}
	return &results
}

//func (txn *Txn) upd(stmt ast.UpdateStmt) *rpc.ResultSet {
//	start := time.Now()
//	exist, err := txn.isKeyExists([]byte(tablePrefix + stmt.TableName))
//	if err != nil {
//		return InternalErrorMessage
//	} else if !exist {
//		return &rpc.ResultSet{
//			Message:  fmt.Sprintf(tableNotExistPattern, stmt.TableName),
//			FailFlag: true,
//		}
//	}
//
//	fields, err := txn.getFields(stmt.TableName)
//	updateMap := make(map[int]interface{})
//nextPair:
//	for _, p := range stmt.UpdatePairs {
//		for i, f := range fields {
//			if p.FieldName == f.FieldName {
//				if getValueType(p.Value) != f.FieldType {
//					return &rpc.ResultSet{
//						Message:  "Unknown type mismatch in field list",
//						FailFlag: true,
//					}
//				}
//				updateMap[i] = p.Value
//				continue nextPair
//			}
//		}
//		return &rpc.ResultSet{
//			Message:  "Unknown column in field list",
//			FailFlag: true,
//		}
//	}
//	pIt, err := newPlanIterator(txn, stmt.TableName, stmt.Where)
//	switch err {
//	case nil:
//	case errInvalidField:
//		return &rpc.ResultSet{
//			Message:  "Unknown column",
//			FailFlag: true,
//		}
//	case errFieldTypeMismatch:
//		return &rpc.ResultSet{
//			Message:  "Column type mismatch",
//			FailFlag: true,
//		}
//	default:
//		return InternalErrorMessage
//	}
//	defer pIt.close()
//
//	updateCnt := 0
//	for pIt.valid() {
//		tuple, err := pIt.value()
//		if err != nil {
//			return InternalErrorMessage
//		}
//		for i := 0; i < len(tuple); i++ {
//			if v, ok := updateMap[i]; ok {
//				tuple[i] = v
//			}
//		}
//		err = pIt.update(tuple)
//		if err != nil {
//			return InternalErrorMessage
//		}
//		pIt.next()
//		updateCnt++
//	}
//	return &rpc.ResultSet{
//		Message: fmt.Sprintf(queryOkPattern, updateCnt, time.Since(start).Seconds()),
//	}
//}

func (txn *Txn) del(stmt ast.DeleteStmt) *rpc.ResultSet {
	start := time.Now()
	exist, err := txn.isKeyExists([]byte(tablePrefix + stmt.TableName))
	if err != nil {
		return InternalErrorMessage
	} else if !exist {
		return &rpc.ResultSet{
			Message:  fmt.Sprintf(tableNotExistPattern, stmt.TableName),
			FailFlag: true,
		}
	}

	pIt, err := newPlanIterator(txn, stmt.TableName, stmt.Where)
	switch err {
	case nil:
	case errInvalidField:
		return &rpc.ResultSet{
			Message:  "Unknown column",
			FailFlag: true,
		}
	case errFieldTypeMismatch:
		return &rpc.ResultSet{
			Message:  "Column type mismatch",
			FailFlag: true,
		}
	default:
		return InternalErrorMessage
	}
	defer pIt.close()

	deleteCnt := 0
	for pIt.valid() {
		err := pIt.delete()
		if err != nil {
			return InternalErrorMessage
		}
		deleteCnt++
		pIt.next()
	}
	return &rpc.ResultSet{
		Message: fmt.Sprintf(queryOkPattern, deleteCnt, time.Since(start).Seconds()),
	}
}
