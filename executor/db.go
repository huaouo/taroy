// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package executor

import (
	"flag"
	"github.com/dgraph-io/badger"
	"log"
	"path/filepath"
	"sync"
)

const timeStampKey = "@TS@"

var once sync.Once
var db *Db

type Db struct {
	kv *badger.ManagedDB
	s  *seq
}

func GetDb() *Db {
	once.Do(func() {
		opts := badger.DefaultOptions
		opts.Dir = filepath.Join(flag.Lookup("dir").Value.String(), "db")
		opts.ValueDir = opts.Dir
		managedDb, err := badger.OpenManaged(opts)
		if err != nil {
			log.Fatalf("Cannot open DB file: %v\n", err)
		}

		db = &Db{
			kv: managedDb,
			s: &seq{
				kv:     managedDb,
				key:    []byte(timeStampKey),
				next:   0,
				leased: 0,
			},
		}
	})
	return db
}

func (db *Db) NewTxn() (*Txn, error) {
	ts, err := db.s.nextTs()
	if err != nil {
		return nil, err
	}
	return &Txn{
		kvTxn: db.kv.NewTransactionAt(ts, true),
		s:     db.s,
	}, nil
}
