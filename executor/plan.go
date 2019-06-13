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
	fields        []ast.Field
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
			fields:       fields,
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
		kvIt:          kvIt,
		txn:           txn,
		tag:           field.FieldTag,
		cmpOp:         where.CmpOp,
		tableName:     tableName,
		fieldIndex:    fieldIndex,
		primaryIndex:  primaryIndex,
		fields:        fields,
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

//func (pIt *planIterator) update(tuple []interface{}) error {
//	fields := pIt.fields
//	tupleBytes, _ := msgpackMarshal(tuple)
//	curKey := pIt.kvIt.Item().Key()
//	switch pIt. {
//
//	}
//}

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
