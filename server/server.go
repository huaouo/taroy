// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package main

import (
	"context"
	"fmt"
	"github.com/huaouo/taroy/ast"
	"github.com/huaouo/taroy/executor"
	"github.com/huaouo/taroy/parser"
	"github.com/huaouo/taroy/rpc"
	"google.golang.org/grpc/peer"
	"log"
	"sync"
	"time"
)

const queryOkWithZeroRowAffectedPattern = "Query OK, 0 row(s) affected (%.2f sec)"

type server struct {
	txnMap          sync.Map // map[net.Addr]*executor.Txn
	mutexMap        sync.Map // map[net.Addr]*sync.Mutex
	latestHeartbeat sync.Map // map[net.Addr]time.Time
}

func (s *server) Execute(ctx context.Context, sql *rpc.RawSQL) (*rpc.ResultSet, error) {
	p, _ := peer.FromContext(ctx)
	mutex, mutexOk := s.mutexMap.Load(p.Addr)
	if !mutexOk {
		mutex = &sync.Mutex{}
		s.mutexMap.Store(p.Addr, mutex)
	}
	// Response of network checking
	if sql.Sql == "" {
		if _, ok := s.latestHeartbeat.Load(p.Addr); !ok {
			log.Printf("Client from %s connected", p.Addr)
		}
		s.latestHeartbeat.Store(p.Addr, time.Now())
		return &rpc.ResultSet{}, nil
	}

	mutex.(*sync.Mutex).Lock()
	defer mutex.(*sync.Mutex).Unlock()
	stmts, err := parser.Parse(sql.Sql)
	if err != nil {
		return &rpc.ResultSet{
			Message:  "Parse error: " + err.Error(),
			FailFlag: true,
		}, nil
	}

	var resultSet *rpc.ResultSet
	for _, stmt := range *stmts {
		start := time.Now()
		var txn *executor.Txn
		tx, ok := s.txnMap.Load(p.Addr)
		if ok {
			txn = tx.(*executor.Txn)
		}
		switch stmt.(type) {
		case ast.BeginStmt:
			if txn == nil {
				txn, err = executor.GetDb().NewTxn()
				if err != nil {
					resultSet = &rpc.ResultSet{
						Message:  "Failed to create new transaction",
						FailFlag: true,
					}
					break
				}
				s.txnMap.Store(p.Addr, txn)
				resultSet = &rpc.ResultSet{
					Message: fmt.Sprintf(queryOkWithZeroRowAffectedPattern, time.Since(start).Seconds()),
				}
			}
		case ast.CommitStmt:
			if txn == nil {
				resultSet = &rpc.ResultSet{
					Message: fmt.Sprintf(queryOkWithZeroRowAffectedPattern, time.Since(start).Seconds()),
				}
				break
			}
			resultSet = txn.Commit()
			s.txnMap.Delete(p.Addr)
		case ast.RollbackStmt:
			if txn != nil {
				txn.Rollback()
			}
			resultSet = &rpc.ResultSet{
				Message: fmt.Sprintf(queryOkWithZeroRowAffectedPattern, time.Since(start).Seconds()),
			}
			s.txnMap.Delete(p.Addr)
		default:
			if txn == nil {
				tmpTxn, err := executor.GetDb().NewTxn()
				if err != nil {
					resultSet = &rpc.ResultSet{
						Message:  "Failed to create new transaction",
						FailFlag: true,
					}
					break
				}
				resultSet = tmpTxn.Execute(stmt)
				commitResult := tmpTxn.Commit()
				if commitResult.FailFlag {
					resultSet = commitResult
				}
			} else {
				resultSet = txn.Execute(stmt)
			}
		}

		if resultSet != nil && resultSet.FailFlag {
			break
		}
	}
	return resultSet, nil
}

// Should be called with go CleanUpTimeoutConnections()
func (s *server) CleanUpTimeoutConnections() {
	for {
		time.Sleep(time.Second * 5)
		s.latestHeartbeat.Range(func(key, value interface{}) bool {
			ts, _ := value.(time.Time)
			if time.Since(ts).Seconds() > 10 {
				mut, _ := s.mutexMap.Load(key)
				mutex := mut.(*sync.Mutex)
				mutex.Lock()
				tx, txOk := s.txnMap.Load(key)
				if txOk {
					txn, _ := tx.(*executor.Txn)
					txn.Rollback()
				}

				s.txnMap.Delete(key)
				s.mutexMap.Delete(key)
				s.latestHeartbeat.Delete(key)
				mutex.Unlock()
			}
			return true
		})
	}
}
