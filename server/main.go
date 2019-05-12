// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package main

import (
	"flag"
	"fmt"
	"github.com/huaouo/taroy/rpc"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
)

var port = flag.String("port", "1214", "specify a tcp port to listen on")
var help = flag.Bool("help", false, "print help information")

func main() {
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

	s := grpc.NewServer()
	defer s.Stop()
	rpc.RegisterDBMSServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v\n", err)
	}
}
