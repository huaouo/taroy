// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/huaouo/taroy/rpc"
	"google.golang.org/grpc"
	"log"
	"os"
)

var host = flag.String("host", "localhost", "server host")
var port = flag.String("port", "1214", "server port (tcp)")
var help = flag.Bool("help", false, "print help information")

func main() {
	flag.Parse()
	if *help {
		_, _ = fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		return
	}

	conn, err := grpc.Dial(*host+":"+*port, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Cannot connect to server: %v\n", err)
	}
	defer conn.Close()
	c := rpc.NewDBMSClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("Welcome to taroyDB! Type :q to exit.")
	for {
		if !clientLoop(ctx, c) {
			break
		}
	}
	fmt.Println("Bye!")
}
