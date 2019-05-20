// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package executor

import (
	"github.com/dgraph-io/badger"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"sync"
	"testing"
)

const dbDirForTest = "db"

var dbForTest *Db

func openDbForTest() *Db {
	opts := badger.DefaultOptions
	opts.Dir = dbDirForTest
	opts.ValueDir = opts.Dir
	managedDb, err := badger.OpenManaged(opts)
	if err != nil {
		log.Fatalf("Cannot open DB file: %v\n", err)
	}

	dbForTest = &Db{
		kv: managedDb,
		s: &seq{
			kv:     managedDb,
			key:    []byte(timeStampKey),
			next:   0,
			leased: 0,
		},
	}
	return dbForTest
}

func removeWrapper() {
	err := os.RemoveAll(dbDirForTest)
	if err != nil {
		panic(err)
	}
}

func closeDbForTest() {
	defer removeWrapper()
	err := dbForTest.kv.Close()
	if err != nil {
		panic(err)
	}
}

func TestLease(t *testing.T) {
	s := openDbForTest().s
	defer closeDbForTest()
	numOfTs := 10000
	numOfGoRoutine := 10
	ch := make(chan uint64, numOfTs)
	var wg sync.WaitGroup
	wg.Add(numOfGoRoutine)

	var mutex sync.Mutex
	f := func() {
		defer wg.Done()
		for i := 0; i < numOfTs/numOfGoRoutine; i++ {
			mutex.Lock()
			ts, err := s.nextTs()
			assert.Nil(t, err)
			ch <- ts
			mutex.Unlock()
		}
	}

	for i := 0; i < numOfGoRoutine; i++ {
		go f()
	}
	wg.Wait()
	close(ch)

	prev, ok := <-ch
	assert.True(t, ok)

	for cur, ok := <-ch; ok; cur, ok = <-ch {
		assert.Equal(t, prev+1, cur)
		prev = cur
	}
}

func TestRelease(t *testing.T) {
	s := openDbForTest().s
	defer closeDbForTest()
	numOfTs := 10000
	numOfGoRoutine := 10
	ch := make(chan uint64, numOfTs)
	var wg sync.WaitGroup
	wg.Add(numOfGoRoutine)

	var mutex sync.Mutex
	f := func() {
		defer wg.Done()
		for i := 0; i < numOfTs/numOfGoRoutine; i++ {
			mutex.Lock()
			ts, err := s.nextTs()
			if i%3 == 0 {
				err = s.release()
				assert.Nil(t, err)
			}
			assert.Nil(t, err)
			ch <- ts
			mutex.Unlock()
		}
	}

	for i := 0; i < numOfGoRoutine; i++ {
		go f()
	}
	wg.Wait()
	close(ch)

	prev, ok := <-ch
	assert.True(t, ok)

	for cur, ok := <-ch; ok; cur, ok = <-ch {
		assert.Equal(t, prev+1, cur)
		prev = cur
	}
}

func TestLose(t *testing.T) {
	s := openDbForTest().s
	defer closeDbForTest()
	numOfTs := 10000
	numOfGoRoutine := 10
	ch := make(chan uint64, numOfTs)
	var wg sync.WaitGroup
	wg.Add(numOfGoRoutine)

	var mutex sync.Mutex
	f := func() {
		defer wg.Done()
		for i := 0; i < numOfTs/numOfGoRoutine; i++ {
			mutex.Lock()
			ts, err := s.nextTs()
			if i%3 == 0 {
				// Reset seq
				s.next = 0
				s.leased = 0
				assert.Nil(t, err)
			}
			assert.Nil(t, err)
			ch <- ts
			mutex.Unlock()
		}
	}

	for i := 0; i < numOfGoRoutine; i++ {
		go f()
	}
	wg.Wait()
	close(ch)

	prev, ok := <-ch
	assert.True(t, ok)

	for cur, ok := <-ch; ok; cur, ok = <-ch {
		assert.True(t, cur > prev)
		prev = cur
	}
}
