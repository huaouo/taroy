// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package main

import (
	"flag"
	"fmt"
	"github.com/huaouo/taroy/executor"
	"github.com/huaouo/taroy/rpc"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

var port = flag.String("port", "1214", "specify a tcp port to listen on")
var help = flag.Bool("help", false, "print help information")
var _ = flag.String("dir", ".", "specify path to DB folder")

func main() {
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for range sigChan {
			db := executor.GetDb()
			log.Println("Exit ...")
			err := db.Close()
			if err == nil {
				os.Exit(0)
			} else {
				os.Exit(-1)
			}
		}
	}()

	flag.Parse()
	if *help {
		_, _ = fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		return
	}

	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("Failed to listen: %v\n", err)
	}
	log.Printf("Start to listen on :%s (tcp)\n", *port)

	// init db object
	_ = executor.GetDb()

	s := grpc.NewServer()
	defer s.Stop()

	serverImpl := &server{}
	go serverImpl.CleanUpTimeoutConnections()
	rpc.RegisterDBMSServer(s, serverImpl)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v\n", err)
	}
}
