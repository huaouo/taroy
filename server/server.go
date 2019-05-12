// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package main

import (
	"context"
	"github.com/huaouo/taroy/parser"
	"github.com/huaouo/taroy/rpc"
	"google.golang.org/grpc/peer"
	"log"
	"sync"
	"time"
)

type server struct {
	latestHeartbeat sync.Map // map[net.Addr]time.Time
}

func (s *server) Execute(ctx context.Context, sql *rpc.RawSQL) (*rpc.ResultSet, error) {
	// Response of network checking
	if sql.Sql == "" {
		p, _ := peer.FromContext(ctx)
		if _, ok := s.latestHeartbeat.Load(p.Addr); !ok {
			log.Printf("Client from %s connected", p.Addr)
		}
		s.latestHeartbeat.Store(p.Addr, time.Now())
		return &rpc.ResultSet{}, nil
	}

	log.Println("SQL:", sql.Sql)
	_, err := parser.Parse(sql.Sql)
	if err != nil {
		return &rpc.ResultSet{
			Message: "Error: " + err.Error(),
		}, nil
	}
	return &rpc.ResultSet{}, nil
}
