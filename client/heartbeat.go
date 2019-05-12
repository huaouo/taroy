// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package main

import (
	"context"
	"github.com/huaouo/taroy/rpc"
	"google.golang.org/grpc"
	"log"
	"time"
)

func heartbeat(ctx context.Context, conn *grpc.ClientConn) {
	c := rpc.NewDBMSClient(conn)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		time.Sleep(time.Second * 5)
		_, err := c.Execute(ctx, &rpc.RawSQL{})
		if err != nil {
			log.Fatalf("Lost connection with server: %s\n", err)
			return
		}
	}
}
