// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package executor

import (
	"flag"
	"github.com/dgraph-io/badger"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
)

const timeStampKey = "@TS@"

var once sync.Once
var db *Db

type Db struct {
	kv *badger.ManagedDB
	s  *seq
	sync.Mutex
}

func GetDb() *Db {
	once.Do(func() {
		opts := badger.DefaultOptions
		opts.SyncWrites = true
		opts.Dir = filepath.Join(flag.Lookup("dir").Value.String(), "db")
		opts.ValueDir = opts.Dir
		if _, err := os.Stat(filepath.Join(opts.Dir, "LOCK")); err == nil {
			opts.Truncate = true
			err = os.Remove(filepath.Join(opts.Dir, "LOCK"))
			if err != nil {
				log.Fatal("Cannot unlock db")
			}
		}
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
	ts, err := db.s.getNext()
	if err != nil {
		return nil, err
	}
	return &Txn{
		kvTxn:        db.kv.NewTransactionAt(ts, true),
		s:            db.s,
		readTs:       ts,
		modifiedKeys: make(map[string]bool),
	}, nil
}

func (db *Db) getLatestVersion(key string) (uint64, error) {
	txn := db.kv.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	itOpts := badger.DefaultIteratorOptions
	itOpts.AllVersions = true
	it := txn.NewIterator(itOpts)
	defer it.Close()
	it.Seek([]byte(key))
	if !it.Valid() || string(it.Item().Key()) != key {
		return 0, badger.ErrKeyNotFound
	}
	return it.Item().Version(), nil
}

func (db *Db) Close() error {
	for _, s := range seqMap {
		_ = s.release()
	}
	_ = db.s.release()
	err := db.kv.Close()
	return err
}
