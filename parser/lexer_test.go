// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package parser

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

type tokenPair struct {
	tok     token
	literal string
}

func testLegal(t *testing.T, sql string, expectedTokens []tokenPair) {
	l := newLexer(strings.NewReader(sql))
	for _, ex := range expectedTokens {
		tok, lit, err := l.nextToken()
		assert.Equal(t, ex.tok, tok, "token mismatch")
		assert.Equal(t, ex.literal, lit, "literal mismatch")
		assert.Nil(t, err, "unexpected error")
	}
	_, _, err := l.nextToken()
	assert.Equal(t, err, EOF, "EOF mismatch")
}

func TestCreateTable(t *testing.T) {
	expectedTokens := []tokenPair{
		{createId, ""},
		{tableId, ""},
		{name, "_0atabl9e2"},
		{lParenthesis, ""},
		{name, "afield"},
		{intId, ""},
		{indexId, ""},
		{comma, ""},
		{name, "bfield"},
		{stringId, ""},
		{uniqueId, ""},
		{comma, ""},
		{name, "cfield"},
		{intId, ""},
		{primaryId, ""},
		{rParenthesis, ""},
		{semicolon, ""},
	}
	testLegal(t, "create table _0aTabl9e2( aField int index, \n"+
		"bField strIng unique, \n"+
		"cField inT primary\n"+
		");   \n",
		expectedTokens,
	)
}

func TestDropTable(t *testing.T) {
	expectedTokens := []tokenPair{
		{dropId, ""},
		{tableId, ""},
		{name, "test_table"},
		{semicolon, ""},
	}
	testLegal(t, "   drop TaBLe  \n test_TablE ; \n", expectedTokens)
}

func TestSelect(t *testing.T) {
	expectedTokens1 := []tokenPair{
		{selectId, ""},
		{name, "_crow"},
		{fromId, ""},
		{name, "_ctable"},
		{whereId, ""},
		{name, "_crow"},
		{le, ""},
		{intLiteral, "2"},
		{semicolon, ""},
	}
	testLegal(t, " selEct _cRow from _cTable where _cRow <= 2 ;", expectedTokens1)

	expectedTokens2 := []tokenPair{
		{selectId, ""},
		{name, "_crow"},
		{fromId, ""},
		{name, "_ctable"},
		{whereId, ""},
		{name, "_crow"},
		{gt, ""},
		{stringLiteral, "20"},
		{semicolon, ""},
	}
	testLegal(t, "select _cRow from \n _cTable where _cRoW > \"20\" ;", expectedTokens2)

	expectedTokens3 := []tokenPair{
		{selectId, ""},
		{name, "_crow"},
		{fromId, ""},
		{name, "_ctable"},
		{whereId, ""},
		{name, "_crow"},
		{betweenId, ""},
		{intLiteral, "1"},
		{andId, ""},
		{intLiteral, "2"},
		{semicolon, ""},
	}
	testLegal(t, "select _cRow from _cTablE wHere _cRow between 1 and 2;", expectedTokens3)
}

func TestInsert(t *testing.T) {
	expectedTokens := []tokenPair{
		{insertId, ""},
		{intoId, ""},
		{name, "my_t_able"},
		{valuesId, ""},
		{lParenthesis, ""},
		{intLiteral, "1"},
		{comma, ""},
		{intLiteral, "2"},
		{comma, ""},
		{stringLiteral, "3"},
		{rParenthesis, ""},
		{semicolon, ""},
	}
	testLegal(t, "InserT \n into \t My_T_able values( 1, 2,\n \"3\");", expectedTokens)
}

func TestDelete(t *testing.T) {
	expectedTokens := []tokenPair{
		{deleteId, ""},
		{fromId, ""},
		{name, "mytable"},
		{whereId, ""},
		{name, "a"},
		{ne, ""},
		{intLiteral, "1"},
		{semicolon, ""},
	}
	testLegal(t, "deleTe from mytable where a != \n 1;", expectedTokens)
}

func TestUpdate(t *testing.T) {
	expectedTokens := []tokenPair{
		{updateId, ""},
		{name, "mytable"},
		{setId, ""},
		{name, "thos"},
		{eq, ""},
		{intLiteral, "1"},
		{semicolon, ""},
	}
	testLegal(t, "update myTable set thos = 1;", expectedTokens)
}

func TestShortStmts(t *testing.T) {
	expectedTokens := []tokenPair{
		{beginId, ""},
		{semicolon, ""},
		{rollbackId, ""},
		{semicolon, ""},
		{commitId, ""},
		{semicolon, ""},
		{showId, ""},
		{name, "mydb"},
		{semicolon, ""},
		{showId, ""},
		{tablesId, ""},
		{semicolon, ""},
	}
	testLegal(t, "begin;RollbacK;commit;show mydb;show TABLEs;", expectedTokens)
}

func TestEscapeCharacter(t *testing.T) {
	expectedTokens := []tokenPair{
		{stringLiteral, "\n\r\t\\\""},
	}
	testLegal(t, "\"\\n\\r\\t\\\\\\\"\"", expectedTokens)
}

func TestMissingQuote(t *testing.T) {
	l := newLexer(strings.NewReader("\""))
	_, _, err := l.nextToken()
	assert.Equal(t, MissingTerminatingDoubleQuote, err, "fail in missing quote test")
}

func TestInvalidSymbol(t *testing.T) {
	l := newLexer(strings.NewReader("  ^"))
	_, _, err := l.nextToken()
	assert.Equal(t, InvalidSymbol, err, "fail in invalid symbol test")
}

func TestInvalidEscapeCharacter(t *testing.T) {
	l := newLexer(strings.NewReader("\"\\k\""))
	_, _, err := l.nextToken()
	assert.Equal(t, InvalidEscapeCharacter, err, "fail in invalid escape character test")
}
