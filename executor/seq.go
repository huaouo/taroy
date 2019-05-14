// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package executor

import (
	"encoding/binary"
	"github.com/dgraph-io/badger"
	"sync"
)

const bandwidth = 128

type seq struct {
	sync.Mutex
	kv     *badger.ManagedDB
	key    []byte
	next   uint64
	leased uint64
}

func (s *seq) updateLease() error {
	kvTxn := s.kv.NewTransactionAt(1, true)
	defer kvTxn.Discard()
	item, err := kvTxn.Get(s.key)
	if err == badger.ErrKeyNotFound {
		s.next = 0
	} else if err != nil {
		return err
	} else {
		val, err := item.Value()
		if err != nil {
			return err
		}
		s.next = binary.LittleEndian.Uint64(val)
	}

	lease := s.next + bandwidth
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], lease)
	err = kvTxn.Set(s.key, buf[:])
	if err != nil {
		return err
	}
	err = kvTxn.CommitAt(1, nil)
	if err != nil {
		return err
	}
	s.leased = lease
	return nil
}

func (s *seq) nextTs() (uint64, error) {
	s.Lock()
	defer s.Unlock()
	if s.next >= s.leased {
		if err := s.updateLease(); err != nil {
			return 0, err
		}
	}
	val := s.next
	s.next++
	return val, nil
}

func (s *seq) release() error {
	s.Lock()
	defer s.Unlock()
	kvTxn := s.kv.NewTransactionAt(1, true)
	defer kvTxn.Discard()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], s.next)
	err := kvTxn.Set(s.key, buf[:])
	if err != nil {
		return nil
	}
	err = kvTxn.CommitAt(1, nil)
	if err != nil {
		return err
	}
	s.leased = s.next
	return nil
}
