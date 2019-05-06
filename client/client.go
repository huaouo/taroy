// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/huaouo/taroy/rpc"
	"os"
	"strings"
)

var reader = bufio.NewReader(os.Stdin)

func readLine() (string, error) {
	var builder strings.Builder

	line, prefixed, err := reader.ReadLine()
	if err != nil {
		return "", err
	}
	builder.Write(line)
	for prefixed {
		line, prefixed, err = reader.ReadLine()
		if err != nil {
			return "", err
		}
		builder.Write(line)
	}

	return strings.TrimSpace(builder.String()), nil
}

func clientLoop(ctx context.Context, c rpc.DBMSClient) bool {
	fmt.Print("taroy> ")

	var builder strings.Builder
	for {
		line, err := readLine()
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "IO error: %v", err)
		}

		builder.WriteString(line)

		// Escape characters shouldn't be splitted by line break
		if line[len(line) - 1] == '\\' {
			builder.WriteByte('\n')
		}

		if (builder.Len() == 2 && line == ":q") || line[len(line)-1] == ';' {
			break
		}
		fmt.Print("> ")
	}

	sql := builder.String()
	if sql == ":q" {
		return false
	}

	_, err := c.Execute(ctx, &rpc.RawSQL{Sql: sql})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Execute error: %v\n", err)
	}
	fmt.Println("Send SQL Successfully!")
	return true
}
