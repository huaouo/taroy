// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/huaouo/taroy/rpc"
	"github.com/olekukonko/tablewriter"
	"os"
	"strconv"
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

func printTable(outFile *os.File, table []*rpc.Row) {
	writer := tablewriter.NewWriter(outFile)
	var head []string
	for _, v := range table[0].Fields {
		switch v.Val.(type) {
		case *rpc.Value_IntVal:
			head = append(head, strconv.FormatInt(v.Val.(*rpc.Value_IntVal).IntVal, 10))
		case *rpc.Value_StrVal:
			head = append(head, string(v.Val.(*rpc.Value_StrVal).StrVal))
		}
	}
	writer.SetHeader(head)

	for i := 1; i < len(table); i++ {
		var line []string
		for _, v := range table[i].Fields {
			switch v.Val.(type) {
			case *rpc.Value_IntVal:
				line = append(line, strconv.FormatInt(v.Val.(*rpc.Value_IntVal).IntVal, 10))
			case *rpc.Value_StrVal:
				line = append(line, string(v.Val.(*rpc.Value_StrVal).StrVal))
			}
		}
		writer.Append(line)
	}
	writer.Render()
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

	candidateSqls := strings.Split(builder.String(), ";")
	for _, sql := range candidateSqls {
		sql = strings.TrimSpace(sql)
		if sql == "" {
			continue
		}
		resultSet, err := c.Execute(ctx, &rpc.RawSQL{Sql: sql + ";"})
		if err != nil || resultSet == nil {
			_, _ = fmt.Fprintf(os.Stderr, "Execute error: %v\n", err)
			break
		}

		outFile := os.Stdout
		if resultSet.FailFlag {
			outFile = os.Stderr
		}
		if resultSet.Table != nil {
			printTable(outFile, resultSet.Table)
		}
		_, _ = fmt.Fprintln(outFile, resultSet.Message)
		_, _ = fmt.Fprintln(outFile)
		if resultSet.FailFlag {
			break
		}
	}
	return true
}
