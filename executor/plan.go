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
)

type planIterator struct {
	kvIt          *badger.Iterator
	txn           *Txn
	tag           ast.FieldTag
	cmpOp         ast.CmpOp
	tableName     string
	fieldIndex    int
	primaryIndex  int
	prefix        []byte
	valueBytes    []byte
	valueOptBytes []byte
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
			kvIt:         kvIt,
			txn:          txn,
			tag:          ast.PRIMARY,
			tableName:    tableName,
			primaryIndex: primaryIndex,
			prefix:       prefix,
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
	if field.FieldTag != ast.UNTAGGED {
		switch where.CmpOp {
		case ast.GE, ast.EQ, ast.BETWEEN:
			kvIt.Seek(exPrefix)
		case ast.GT:
			kvIt.Seek(exPrefix)
			if kvIt.Valid() {
				kvIt.Next()
			}
		}
	}

	return &planIterator{
		kvIt:          kvIt,
		txn:           txn,
		tag:           field.FieldTag,
		cmpOp:         where.CmpOp,
		tableName:     tableName,
		fieldIndex:    fieldIndex,
		primaryIndex:  primaryIndex,
		prefix:        prefix,
		valueBytes:    valueBytes,
		valueOptBytes: valueOptBytes,
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

	if pIt.cmpOp == ast.BETWEEN {
		return bytes.Compare(partKey, pIt.valueOptBytes) <= 0
	}
	compareResult := bytes.Compare(partKey, pIt.valueBytes)
	switch pIt.cmpOp {
	case ast.LT:
		return compareResult == -1
	case ast.LE:
		return compareResult <= 0
	case ast.EQ:
		return compareResult == 0
	case ast.NE:
		for compareResult == 0 {
			pIt.next()
			if !pIt.kvIt.ValidForPrefix(pIt.prefix) {
				return false
			}
			partKey, err = pIt.getCurrentComparingBytes()
			if err != nil {
				return false
			}
			compareResult = bytes.Compare(partKey, pIt.valueBytes)
		}
		return true
	}
	return false
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
	}
	var tuple []interface{}
	err = msgpackUnmarshal(tupleBytes, &tuple)
	if err != nil {
		return nil, err
	}
	return tuple, nil
}

func (pIt *planIterator) update(tuple []interface{}) {

}

func (pIt *planIterator) delete() {

}

func (pIt *planIterator) next() {
	pIt.kvIt.Next()
}

func (pIt *planIterator) close() {
	pIt.kvIt.Close()
}
