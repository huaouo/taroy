// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package executor

import (
	"bytes"
	"github.com/dgraph-io/badger"
	"github.com/huaouo/taroy/ast"
	"github.com/pkg/errors"
)

var (
	errInvalidField        = errors.New("invalid field")
	errFieldTypeMismatch   = errors.New("field type mismatch")
	errInvalidPlanIterator = errors.New("invalid plan iterator")
	errDuplicatePrimaryKey = errors.New("duplicate primary key")
	errDuplicateUniqueKey  = errors.New("duplicate unique key")
)

type planIterator struct {
	kvIt           *badger.Iterator
	txn            *Txn
	tag            ast.FieldTag
	cmpOp          ast.CmpOp
	tableName      string
	fieldIndex     int
	primaryIndex   int
	prefix         []byte
	valueBytes     []byte
	valueOptBytes  []byte
	fields         []ast.Field
	pendingDeletes map[string]bool
	pendingWrites  map[string]string
}

func newPlanIterator(txn *Txn, tableName string, where *ast.WhereClause) (*planIterator, error) {
	fieldBytes, err := txn.getKv([]byte(tablePrefix + tableName))
	if err != nil {
		return nil, err
	}
	var fields []ast.Field
	err = msgpackUnmarshal(fieldBytes, &fields)
	if err != nil {
		return nil, err
	}

	primaryIndex := getPrimaryKeyIndex(fields)
	if where == nil {
		kvIt := txn.kvTxn.NewIterator(badger.DefaultIteratorOptions)
		prefix := concatBytes(tableName, byte('@'), byte(primaryIndex), byte('@'))
		kvIt.Seek(prefix)
		return &planIterator{
			kvIt:           kvIt,
			txn:            txn,
			tag:            ast.PRIMARY,
			tableName:      tableName,
			primaryIndex:   primaryIndex,
			fields:         fields,
			prefix:         prefix,
			pendingDeletes: make(map[string]bool),
			pendingWrites:  make(map[string]string),
		}, nil
	}

	// check existence of field
	fieldIndex := -1
	for i, f := range fields {
		if f.FieldName == where.FieldName {
			fieldIndex = i
		}
	}
	if fieldIndex == -1 {
		return nil, errInvalidField
	}
	field := fields[fieldIndex]

	// check type of Value and ValueOpt
	valueType := getValueType(where.Value)
	valueOptType := getValueType(where.ValueOpt)
	if valueType != field.FieldType ||
		where.CmpOp == ast.BETWEEN && valueType != valueOptType {
		return nil, errFieldTypeMismatch
	}

	kvIt := txn.kvTxn.NewIterator(badger.DefaultIteratorOptions)
	var findIndex int
	if field.FieldTag == ast.UNTAGGED {
		findIndex = getPrimaryKeyIndex(fields)
	} else {
		findIndex = fieldIndex
	}
	prefix := concatBytes(tableName, byte('@'), byte(findIndex), byte('@'))
	valueBytes := getValueBytes(where.Value)
	valueOptBytes := getValueBytes(where.ValueOpt)
	exPrefix := concatBytes(prefix, valueBytes)
	if field.FieldTag == ast.UNTAGGED {
		kvIt.Seek(prefix)
	} else {
		switch where.CmpOp {
		case ast.GE, ast.EQ, ast.BETWEEN:
			kvIt.Seek(exPrefix)
		case ast.GT:
			kvIt.Seek(exPrefix)
			if kvIt.Valid() {
				if field.FieldTag == ast.INDEX {
					targetValueBytes := extractIndexKey(extractPartKey(kvIt.Item().Key()))
					if bytes.Equal(targetValueBytes, valueBytes) {
						kvIt.Next()
					}
				} else {
					if bytes.Equal(kvIt.Item().Key(), exPrefix) {
						kvIt.Next()
					}
				}
			}
		default:
			kvIt.Seek(prefix)
		}
	}

	return &planIterator{
		kvIt:           kvIt,
		txn:            txn,
		tag:            field.FieldTag,
		cmpOp:          where.CmpOp,
		tableName:      tableName,
		fieldIndex:     fieldIndex,
		primaryIndex:   primaryIndex,
		fields:         fields,
		prefix:         prefix,
		valueBytes:     valueBytes,
		valueOptBytes:  valueOptBytes,
		pendingDeletes: make(map[string]bool),
		pendingWrites:  make(map[string]string),
	}, nil
}

func (pIt *planIterator) getCurrentComparingBytes() ([]byte, error) {
	if !pIt.kvIt.ValidForPrefix(pIt.prefix) {
		return nil, errInvalidPlanIterator
	}

	keyBytes := pIt.kvIt.Item().Key()
	partKey := extractPartKey(keyBytes)
	if pIt.tag == ast.UNTAGGED {
		tupleBytes, err := pIt.kvIt.Item().Value()
		if err != nil {
			return nil, errInvalidPlanIterator
		}
		var tuple []interface{}
		err = msgpackUnmarshal(tupleBytes, &tuple)
		if err != nil {
			return nil, errInvalidPlanIterator
		}
		partKey = getValueBytes(tuple[pIt.fieldIndex])
	} else if pIt.tag == ast.INDEX {
		partKey = extractIndexKey(partKey)
	}
	return partKey, nil
}

func (pIt *planIterator) compareWhile(curResult int, f func(result int) bool) bool {
	for f(curResult) {
		pIt.next()
		if !pIt.kvIt.ValidForPrefix(pIt.prefix) {
			return false
		}
		partKey, err := pIt.getCurrentComparingBytes()
		if err != nil {
			return false
		}
		curResult = bytes.Compare(partKey, pIt.valueBytes)
	}
	return true
}

func (pIt *planIterator) valid() bool {
	if !pIt.kvIt.ValidForPrefix(pIt.prefix) {
		return false
	} else if pIt.valueBytes == nil && pIt.valueOptBytes == nil {
		return true
	}

	partKey, err := pIt.getCurrentComparingBytes()
	if err != nil {
		return false
	}

	compareResult := bytes.Compare(partKey, pIt.valueBytes)
	if pIt.tag == ast.UNTAGGED {
		switch pIt.cmpOp {
		case ast.BETWEEN:
			compareResultOpt := bytes.Compare(partKey, pIt.valueOptBytes)
			for compareResult < 0 || compareResultOpt > 0 {
				pIt.next()
				if !pIt.kvIt.ValidForPrefix(pIt.prefix) {
					return false
				}
				partKey, err = pIt.getCurrentComparingBytes()
				if err != nil {
					return false
				}
				compareResult = bytes.Compare(partKey, pIt.valueBytes)
				compareResultOpt = bytes.Compare(partKey, pIt.valueOptBytes)
			}
			return true
		case ast.GT:
			return pIt.compareWhile(compareResult, func(result int) bool {
				return result != 1
			})
		case ast.GE:
			return pIt.compareWhile(compareResult, func(result int) bool {
				return result < 0
			})
		}
	}

	if pIt.cmpOp == ast.BETWEEN {
		return bytes.Compare(partKey, pIt.valueOptBytes) <= 0
	}
	switch pIt.cmpOp {
	case ast.LT:
		return compareResult == -1
	case ast.LE:
		return compareResult <= 0
	case ast.EQ:
		if pIt.tag != ast.UNTAGGED {
			return compareResult == 0
		}

		return pIt.compareWhile(compareResult, func(result int) bool {
			return result != 0
		})
	case ast.NE:
		return pIt.compareWhile(compareResult, func(result int) bool {
			return result == 0
		})
	}
	return true
}

func (pIt *planIterator) value() ([]interface{}, error) {
	if !pIt.valid() {
		return nil, errInvalidPlanIterator
	}

	var (
		err        error
		tupleBytes []byte
	)
	switch pIt.tag {
	case ast.UNTAGGED, ast.PRIMARY:
		tupleBytes, err = pIt.kvIt.Item().Value()
		if err != nil {
			return nil, err
		}
	case ast.UNIQUE:
		primaryKeyPart, err := pIt.kvIt.Item().Value()
		if err != nil {
			return nil, err
		}
		tupleBytes, err = pIt.txn.getKv(concatBytes(pIt.tableName, byte('@'), byte(pIt.primaryIndex), byte('@'), primaryKeyPart))
		if err != nil {
			return nil, err
		}
	case ast.INDEX:
		uniqueKey := pIt.kvIt.Item().Key()
		partKey := extractPartKey(uniqueKey)
		indexValue := extractIndexValue(partKey)
		tupleBytes, err = pIt.txn.getKv(concatBytes(pIt.tableName, byte('@'), byte(pIt.primaryIndex), byte('@'), indexValue))
		if err != nil {
			return nil, err
		}
	}
	var tuple []interface{}
	err = msgpackUnmarshal(tupleBytes, &tuple)
	if err != nil {
		return nil, err
	}
	return tuple, nil
}

func (pIt *planIterator) update(tuple []interface{}) error {
	curTuple, err := pIt.value()
	if err != nil {
		return err
	}
	tupleBytes, err := msgpackMarshal(tuple)
	if err != nil {
		return err
	}
	var (
		primaryPartKey    []byte
		curPrimaryPartKey []byte
	)
	if pIt.primaryIndex == 255 {
		switch pIt.tag {
		case ast.PRIMARY, ast.UNTAGGED:
			primaryPartKey = extractPartKey(pIt.kvIt.Item().Key())
		case ast.UNIQUE:
			var err error
			primaryPartKey, err = pIt.kvIt.Item().Value()
			if err != nil {
				return err
			}
		case ast.INDEX:
			primaryPartKey = extractIndexValue(extractPartKey(pIt.kvIt.Item().Key()))
		}
		curPrimaryPartKey = primaryPartKey

		writeKey := concatBytes(pIt.tableName, byte('@'), byte(255), byte('@'), primaryPartKey)
		if _, ok := pIt.pendingWrites[string(writeKey)]; ok {
			return errDuplicatePrimaryKey
		}
		pIt.pendingWrites[string(writeKey)] = string(tupleBytes)
	} else {
		i := pIt.primaryIndex
		curPrimaryPartKey = getValueBytes(curTuple[i])
		primaryPartKey = getValueBytes(tuple[i])

		primaryKey := concatBytes(pIt.tableName, byte('@'), byte(i),
			byte('@'), primaryPartKey)
		if exist, err := pIt.txn.isKeyExists(primaryKey); err != nil {
			return err
		} else if exist && tuple[i] != curTuple[i] {
			return errDuplicatePrimaryKey
		}

		deleteKey := concatBytes(pIt.tableName, byte('@'), byte(i), byte('@'),
			curPrimaryPartKey)
		pIt.pendingDeletes[string(deleteKey)] = true

		if _, ok := pIt.pendingWrites[string(primaryKey)]; ok {
			return errDuplicatePrimaryKey
		}
		pIt.pendingWrites[string(primaryKey)] = string(tupleBytes)
	}

	for i, f := range pIt.fields {
		//if tuple[i] != curTuple[i] {
		switch f.FieldTag {
		case ast.UNIQUE:
			uniqueKey := concatBytes(pIt.tableName, byte('@'), byte(i), byte('@'),
				getValueBytes(tuple[i]))
			if exist, err := pIt.txn.isKeyExists(uniqueKey); err != nil {
				return err
			} else if exist && tuple[i] != curTuple[i] {
				return errDuplicateUniqueKey
			}
			deleteKey := concatBytes(pIt.tableName, byte('@'), byte(i), byte('@'),
				getValueBytes(curTuple[i]))
			pIt.pendingDeletes[string(deleteKey)] = true
			if _, ok := pIt.pendingWrites[string(uniqueKey)]; ok {
				return errDuplicateUniqueKey
			}
			pIt.pendingWrites[string(uniqueKey)] = string(primaryPartKey)
		case ast.INDEX:
			deleteKey := concatBytes(pIt.tableName, byte('@'), byte(i), byte('@'),
				getValueBytes(curTuple[i]), byte('@'), curPrimaryPartKey)
			pIt.pendingDeletes[string(deleteKey)] = true
			writeKey := concatBytes(pIt.pendingWrites, concatBytes(pIt.tableName,
				byte('@'), byte(i), byte('@'), getValueBytes(tuple[i]), byte('@'),
				primaryPartKey))
			pIt.pendingWrites[string(writeKey)] = string([]byte{})
		}
		//}
	}
	return nil
}

func (pIt *planIterator) doUpdate() error {
	for k, _ := range pIt.pendingDeletes {
		err := pIt.txn.deleteKv([]byte(k))
		if err != nil {
			return err
		}
	}
	for k, v := range pIt.pendingWrites {
		err := pIt.txn.setKv([]byte(k), []byte(v))
		if err != nil {
			return err
		}
	}

	pIt.pendingDeletes = make(map[string]bool)
	pIt.pendingWrites = make(map[string]string)
	return nil
}

func (pIt *planIterator) delete() error {
	var primaryPartKey []byte
	switch pIt.tag {
	case ast.PRIMARY, ast.UNTAGGED:
		primaryPartKey = extractPartKey(pIt.kvIt.Item().Key())
	case ast.UNIQUE:
		var err error
		primaryPartKey, err = pIt.kvIt.Item().Value()
		if err != nil {
			return err
		}
	case ast.INDEX:
		primaryPartKey = extractIndexKey(extractPartKey(pIt.kvIt.Item().Key()))
	}
	tuple, err := pIt.value()
	if err != nil {
		return err
	}
	fields := pIt.fields

	for i, f := range fields {
		var err error
		switch f.FieldTag {
		case ast.UNIQUE:
			err = pIt.txn.deleteKv(concatBytes(pIt.tableName, byte('@'),
				byte(i), byte('@'), getValueBytes(tuple[i])))
		case ast.INDEX:
			err = pIt.txn.deleteKv(concatBytes(pIt.tableName, byte('@'),
				byte(i), byte('@'), getValueBytes(tuple[i]), byte('@'), primaryPartKey))
		}
		if err != nil {
			return err
		}
	}

	return pIt.txn.deleteKv(concatBytes(pIt.tableName, byte('@'),
		byte(pIt.primaryIndex), byte('@'), primaryPartKey))
}

func (pIt *planIterator) next() {
	pIt.kvIt.Next()
}

func (pIt *planIterator) close() {
	pIt.kvIt.Close()
}
