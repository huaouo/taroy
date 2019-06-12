// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package executor

import (
	"encoding/binary"
	"github.com/huaouo/taroy/ast"
	"github.com/vmihailenco/msgpack"
	"log"
	"math"
)

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

func concatBytes(elem ...interface{}) []byte {
	var bytes []byte
	for _, e := range elem {
		switch e.(type) {
		case string:
			bytes = append(bytes, []byte(e.(string))...)
		case []byte:
			bytes = append(bytes, e.([]byte)...)
		case byte:
			bytes = append(bytes, e.(byte))
		case rune:
			bytes = append(bytes, byte(e.(rune)))
		}
	}
	return bytes
}

func packUint64(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

func packInt64(i int64) []byte {
	var u uint64
	if i >= 0 {
		u = uint64(i) + math.MaxInt64 + 1
	} else {
		u = uint64(i + math.MaxInt64 + 1)
	}
	return packUint64(u)
}

func unpackUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func unpackInt64(b []byte) int64 {
	u := unpackUint64(b)
	var i int64
	if u <= math.MaxInt64 {
		i = int64(u) - math.MaxInt64 - 1
	} else {
		i = int64(u - math.MaxInt64 - 1)
	}
	return i
}

func getValueType(value interface{}) ast.FieldType {
	switch value.(type) {
	case int64:
		return ast.INT
	case string:
		return ast.STRING
	default:
		return ast.NIL
	}
}

func getValueBytes(value interface{}) []byte {
	switch value.(type) {
	case int64:
		return packInt64(value.(int64))
	case string:
		return []byte(value.(string))
	default:
		return nil
	}
}

func extractPartKey(keyBytes []byte) []byte {
	const splitByte = byte('@')
	splitByteCnt := 0
	for i, c := range keyBytes {
		if c == splitByte {
			splitByteCnt++
		}
		if splitByteCnt == 2 {
			return concatBytes(keyBytes[i+1:])
		}
	}
	return []byte{}
}

func extractIndexKey(partKeyBytes []byte) []byte {
	const splitByte = byte('@')
	for i, c := range partKeyBytes {
		if c == splitByte {
			return concatBytes(partKeyBytes[:i])
		}
	}
	return []byte{}
}

func extractIndexValue(partKeyBytes []byte) []byte {
	const splitByte = byte('@')
	for i, c := range partKeyBytes {
		if c == splitByte {
			return concatBytes(partKeyBytes[i+1:])
		}
	}
	return []byte{}
}
