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

		if builder.Len() == 0 {
			switch line {
			case "":
				return true
			case ":q":
				return false
			}
		}
		builder.WriteString(line)
		builder.WriteByte('\n')

		if len(line) != 0 && line[len(line)-1] == ';' {
			break
		}
		fmt.Print("> ")
	}

	sql := builder.String()

	resultSet, err := c.Execute(ctx, &rpc.RawSQL{Sql: sql})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Execute error: %v\n", err)
	}
	if resultSet != nil {
		fmt.Println(resultSet.Message)
	}
	return true
}
