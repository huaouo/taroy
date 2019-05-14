// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package parser

import (
	"github.com/huaouo/taroy/ast"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSelectParser(t *testing.T) {
	stmts, err := Parse(
		"select * from test_table where col > 12;" +
			"select *, a from test_table where b between \"12\" and \"14\";" +
			"select a from test_table;",
	)
	assert.Equal(t, nil, err)

	expectedStmts := &ast.Stmts{
		ast.SelectStmt{
			TableName:  "test_table",
			FieldNames: []string{"*"},
			Where: &ast.WhereClause{
				FieldName: "col",
				CmpOp:     ast.GT,
				Value:     12,
			},
		},
		ast.SelectStmt{
			TableName:  "test_table",
			FieldNames: []string{"*", "a"},
			Where: &ast.WhereClause{
				FieldName: "b",
				CmpOp:     ast.BETWEEN,
				Value:     "12",
				ValueOpt:  "14",
			},
		},
		ast.SelectStmt{
			TableName:  "test_table",
			FieldNames: []string{"a"},
			Where:      nil,
		},
	}
	assert.Equal(t, expectedStmts, stmts)

	_, err = Parse("select hello;")
	assert.Equal(t, InvalidSyntax, err)

	_, err = Parse("select from yu;")
	assert.Equal(t, InvalidSyntax, err)

	_, err = Parse("select where a = 1;")
	assert.Equal(t, InvalidSyntax, err)

	_, err = Parse("select a from b where a = ;")
	assert.Equal(t, InvalidSyntax, err)

	_, err = Parse("select a from b where = 1;")
	assert.Equal(t, InvalidSyntax, err)

	_, err = Parse("select a from b where 21;")
	assert.Equal(t, InvalidSyntax, err)

	_, err = Parse("select a from b where c = 1")
	assert.Equal(t, InvalidSyntax, err)
}

func TestCreateTableParser(t *testing.T) {
	stmts, err := Parse(
		"create table test_table (" +
			"a int primary," +
			"b string unique," +
			"c int index," +
			"d string" +
			");" +

			"create table next_table (" +
			"a int" +
			");" +

			"create table third_table(" +
			"a int primary" +
			");",
	)
	assert.Equal(t, nil, err)

	expectedStmts := &ast.Stmts{
		ast.CreateTableStmt{
			TableName: "test_table",
			Fields: []ast.Field{
				{FieldName: "a", FieldType: ast.INT, FieldTag: ast.PRIMARY},
				{FieldName: "b", FieldType: ast.STRING, FieldTag: ast.UNIQUE},
				{FieldName: "c", FieldType: ast.INT, FieldTag: ast.INDEX},
				{FieldName: "d", FieldType: ast.STRING, FieldTag: ast.UNTAGGED},
			},
		},
		ast.CreateTableStmt{
			TableName: "next_table",
			Fields: []ast.Field{
				{FieldName: "a", FieldType: ast.INT, FieldTag: ast.UNTAGGED},
			},
		},
		ast.CreateTableStmt{
			TableName: "third_table",
			Fields: []ast.Field{
				{FieldName: "a", FieldType: ast.INT, FieldTag: ast.PRIMARY},
			},
		},
	}
	assert.Equal(t, expectedStmts, stmts)

	_, err = Parse("create table a ( b int primary c string);")
	assert.Equal(t, InvalidSyntax, err)

	_, err = Parse("create aa (a int primary);")
	assert.Equal(t, InvalidSyntax, err)

	_, err = Parse("create table mytable (int primary);")
	assert.Equal(t, InvalidSyntax, err)

	_, err = Parse("create table mytable (afield );")
	assert.Equal(t, InvalidSyntax, err)

	_, err = Parse("create table mytable (afield int,);")
	assert.Equal(t, InvalidSyntax, err)

	_, err = Parse("create table mytable();")
	assert.Equal(t, InvalidSyntax, err)
}

func TestDropTableParser(t *testing.T) {
	stmts, err := Parse("drop table table_name;")
	assert.Equal(t, nil, err)

	expectedStmts := &ast.Stmts{
		ast.DropTableStmt{
			TableName: "table_name",
		},
	}
	assert.Equal(t, expectedStmts, stmts)

	_, err = Parse("drop tables *;")
	assert.Equal(t, InvalidSyntax, err)

	_, err = Parse("drop table;")
	assert.Equal(t, InvalidSyntax, err)
}

func TestInsertParser(t *testing.T) {
	stmts, err := Parse(
		"insert into mytable values(1, \"2\");" +
			"insert into mytable values(1);",
	)
	assert.Equal(t, nil, err)

	expectedStmts := &ast.Stmts{
		ast.InsertStmt{
			TableName: "mytable",
			Values:    []interface{}{1, "2"},
		},
		ast.InsertStmt{
			TableName: "mytable",
			Values:    []interface{}{1},
		},
	}
	assert.Equal(t, expectedStmts, stmts)

	_, err = Parse("insert into ll values();")
	assert.Equal(t, InvalidSyntax, err)

	_, err = Parse("insert into ll values (*);")
	assert.Equal(t, InvalidSyntax, err)
}

func TestDeleteParser(t *testing.T) {
	stmts, err := Parse(
		"delete from mytable;" +
			"delete from mytable where a = \"12\";",
	)
	assert.Equal(t, nil, err)

	expectedStmts := &ast.Stmts{
		ast.DeleteStmt{
			TableName: "mytable",
			Where:     nil,
		},
		ast.DeleteStmt{
			TableName: "mytable",
			Where: &ast.WhereClause{
				FieldName: "a",
				CmpOp:     ast.EQ,
				Value:     "12",
			},
		},
	}
	assert.Equal(t, expectedStmts, stmts)

	_, err = Parse("delete from where a = 1;")
	assert.Equal(t, InvalidSyntax, err)
}

func TestUpdateParser(t *testing.T) {
	stmts, err := Parse(
		"update mytable set a = 1, b = \"xmu\" where a != 12;" +
			"update mytable set a = 1 where a != 12;" +
			"update mytable set a = 1;",
	)
	assert.Equal(t, nil, err)

	expectedStmts := &ast.Stmts{
		ast.UpdateStmt{
			TableName: "mytable",
			UpdatePairs: []ast.UpdatePair{
				{FieldName: "a", Value: 1},
				{FieldName: "b", Value: "xmu"},
			},
			Where: &ast.WhereClause{
				FieldName: "a",
				CmpOp:     ast.NE,
				Value:     12,
			},
		},
		ast.UpdateStmt{
			TableName: "mytable",
			UpdatePairs: []ast.UpdatePair{
				{FieldName: "a", Value: 1},
			},
			Where: &ast.WhereClause{
				FieldName: "a",
				CmpOp:     ast.NE,
				Value:     12,
			},
		},
		ast.UpdateStmt{
			TableName: "mytable",
			UpdatePairs: []ast.UpdatePair{
				{FieldName: "a", Value: 1},
			},
			Where: nil,
		},
	}
	assert.Equal(t, expectedStmts, stmts)

	_, err = Parse("update mytable set a = 2, where a = 1;")
	assert.Equal(t, InvalidSyntax, err)
}

func TestShowParser(t *testing.T) {
	stmts, err := Parse("show tables;show mytable;")
	assert.Equal(t, nil, err)

	expectedStmts := &ast.Stmts{
		ast.ShowStmt{
			ShowTables: true,
		},
		ast.ShowStmt{
			ShowTables: false,
			TableName:  "mytable",
		},
	}
	assert.Equal(t, expectedStmts, stmts)

	_, err = Parse("show;")
	assert.Equal(t, InvalidSyntax, err)
}

func TestSimpleParser(t *testing.T) {
	stmts, err := Parse("begin;rollback;commit;")
	assert.Equal(t, nil, err)

	expectedStmts := &ast.Stmts{
		ast.BeginStmt{},
		ast.RollbackStmt{},
		ast.CommitStmt{},
	}
	assert.Equal(t, expectedStmts, stmts)
}
