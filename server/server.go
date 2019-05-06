// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package main

import (
	"context"
	"github.com/huaouo/taroy/rpc"
	"log"
)

type server struct{}

func (s *server) Execute(ctx context.Context, sql *rpc.RawSQL) (*rpc.ResultSet, error) {
	log.Println(sql)
	return &rpc.ResultSet{}, nil
}
