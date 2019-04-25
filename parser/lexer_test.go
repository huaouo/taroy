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

func testLegalLexer(t *testing.T, sql string, expectedTokens []tokenPair) {
	l := newLexer(strings.NewReader(sql))
	for _, ex := range expectedTokens {
		tok, lit, err := l.nextToken()
		assert.Equal(t, ex.tok, tok)
		assert.Equal(t, ex.literal, lit)
		assert.Nil(t, err)
	}
	_, _, err := l.nextToken()
	assert.Equal(t, err, EOF)
}

func TestCreateTableLexer(t *testing.T) {
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
	testLegalLexer(t, "create table _0aTabl9e2( aField int index, \n"+
		"bField strIng unique, \n"+
		"cField inT primary\n"+
		");   \n",
		expectedTokens,
	)
}

func TestDropTableLexer(t *testing.T) {
	expectedTokens := []tokenPair{
		{dropId, ""},
		{tableId, ""},
		{name, "test_table"},
		{semicolon, ""},
	}
	testLegalLexer(t, "   drop TaBLe  \n test_TablE ; \n", expectedTokens)
}

func TestSelectLexer(t *testing.T) {
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
	testLegalLexer(t, " selEct _cRow from _cTable where _cRow <= 2 ;", expectedTokens1)

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
	testLegalLexer(t, "select _cRow from \n _cTable where _cRoW > \"20\" ;", expectedTokens2)

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
	testLegalLexer(t, "select _cRow from _cTablE wHere _cRow between 1 and 2;", expectedTokens3)
}

func TestInsertLexer(t *testing.T) {
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
	testLegalLexer(t, "InserT \n into \t My_T_able values( 1, 2,\n \"3\");", expectedTokens)
}

func TestDeleteLexer(t *testing.T) {
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
	testLegalLexer(t, "deleTe from mytable where a != \n 1;", expectedTokens)
}

func TestUpdateLexer(t *testing.T) {
	expectedTokens := []tokenPair{
		{updateId, ""},
		{name, "mytable"},
		{setId, ""},
		{name, "thos"},
		{eq, ""},
		{intLiteral, "1"},
		{semicolon, ""},
	}
	testLegalLexer(t, "update myTable set thos = 1;", expectedTokens)
}

func TestShortStmtsLexer(t *testing.T) {
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
	testLegalLexer(t, "begin;RollbacK;commit;show mydb;show TABLEs;", expectedTokens)
}

func TestEscapeCharacterLexer(t *testing.T) {
	expectedTokens := []tokenPair{
		{stringLiteral, "\n\r\t\\\""},
	}
	testLegalLexer(t, "\"\\n\\r\\t\\\\\\\"\"", expectedTokens)
}

func TestMissingQuoteLexer(t *testing.T) {
	l := newLexer(strings.NewReader("\""))
	_, _, err := l.nextToken()
	assert.Equal(t, MissingTerminatingDoubleQuote, err)
}

func TestInvalidSymbolLexer(t *testing.T) {
	l := newLexer(strings.NewReader("  ^"))
	_, _, err := l.nextToken()
	assert.Equal(t, InvalidSymbol, err)
}

func TestInvalidEscapeCharacterLexer(t *testing.T) {
	l := newLexer(strings.NewReader("\"\\k\""))
	_, _, err := l.nextToken()
	assert.Equal(t, InvalidEscapeCharacter, err)
}
