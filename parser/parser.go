// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package parser

import (
	"errors"
	"github.com/huaouo/taroy/ast"
	"sort"
	"strconv"
)

var (
	InvalidSyntax     = errors.New("invalid syntax")
	InvalidIntLiteral = errors.New("invalid int literal")
)

type Parser struct {
	l     *lexer
	stmts ast.Stmts
	err   error
}

func newParser(sql string) *Parser {
	return &Parser{l: newLexer(sql), stmts: []interface{}{}}
}

func (p *Parser) parse() (*ast.Stmts, error) {
	tok, _, err := p.l.nextToken()
	for err == nil {
		switch tok {
		case createId:
			p.parseCreateStmt()
		case dropId:
			p.parseDropStmt()
		case selectId:
			p.parseSelectStmt()
		case insertId:
			p.parseInsertStmt()
		case deleteId:
			p.parseDeleteStmt()
		case updateId:
			p.parseUpdateStmt()
		case beginId:
			p.parseBeginStmt()
		case rollbackId:
			p.parseRollbackStmt()
		case commitId:
			p.parseCommitStmt()
		case showId:
			p.parseShowStmt()
		default:
			p.err = InvalidSyntax
		}

		if p.err != nil {
			return nil, p.err
		}

		tok, _, err = p.l.nextToken()
	}

	if err != EOF {
		return nil, err
	}
	return &p.stmts, nil
}

func Parse(sql string) (*ast.Stmts, error) {
	return newParser(sql).parse()
}

func (p *Parser) expectToken(exTokens ...token) (matchedTok token, lit string) {
	matchedTok = illegal
	lit = ""
	if p.err != nil {
		return
	}

	tok, realLit, err := p.l.nextToken()
	if err != nil {
		if err == EOF {
			p.err = InvalidSyntax
		} else {
			p.err = err
		}
		return
	}

	sort.Slice(exTokens, func(i, j int) bool {
		return exTokens[i] < exTokens[j]
	})
	foundPos := sort.Search(len(exTokens), func(i int) bool {
		return exTokens[i] >= tok
	})
	if foundPos < len(exTokens) && exTokens[foundPos] == tok {
		return tok, realLit
	}

	p.err = InvalidSyntax
	return
}

func (p *Parser) parseFieldDefs() (field ast.Field, lastTok token) {
	field = ast.Field{}

	_, field.FieldName = p.expectToken(name)
	fieldTypeTok, _ := p.expectToken(stringId, intId)
	switch fieldTypeTok {
	case stringId:
		field.FieldType = ast.STRING
	case intId:
		field.FieldType = ast.INT
	}

	tok, _ := p.expectToken(primaryId, uniqueId, indexId, comma, rParenthesis)
	if tok == comma || tok == rParenthesis {
		lastTok = tok
	} else {
		switch tok {
		case primaryId:
			field.FieldTag = ast.PRIMARY
		case uniqueId:
			field.FieldTag = ast.UNIQUE
		case indexId:
			field.FieldTag = ast.INDEX
		}
		lastTok, _ = p.expectToken(comma, rParenthesis)
	}
	return
}

func (p *Parser) parseCreateStmt() {
	createStmt := ast.CreateTableStmt{}

	_, _ = p.expectToken(tableId)
	_, createStmt.TableName = p.expectToken(name)
	_, _ = p.expectToken(lParenthesis)

	field, lastTok := p.parseFieldDefs()
	createStmt.Fields = append(createStmt.Fields, field)
	for lastTok == comma {
		field, lastTok = p.parseFieldDefs()
		createStmt.Fields = append(createStmt.Fields, field)
	}
	_, _ = p.expectToken(semicolon)
	p.stmts = append(p.stmts, createStmt)
}

func (p *Parser) parseDropStmt() {
	dropStmt := ast.DropTableStmt{}

	_, _ = p.expectToken(tableId)
	_, dropStmt.TableName = p.expectToken(name)
	_, _ = p.expectToken(semicolon)
	p.stmts = append(p.stmts, dropStmt)
}

func (p *Parser) parseSelectStmt() {
	selectStmt := ast.SelectStmt{}

	tok, lit := p.expectToken(name, star)
	if tok == star {
		lit = "*"
	}
	selectStmt.FieldNames = append(selectStmt.FieldNames, lit)

	for p.err == nil {
		tok, _ = p.expectToken(comma, fromId)
		if tok == fromId {
			break
		}
		tok, lit := p.expectToken(name, star)
		if tok == star {
			lit = "*"
		}
		selectStmt.FieldNames = append(selectStmt.FieldNames, lit)
	}
	_, selectStmt.TableName = p.expectToken(name)
	tok, _ = p.expectToken(whereId, semicolon)
	if tok == whereId {
		selectStmt.Where = p.parseWhereClause()
		_, _ = p.expectToken(semicolon)
	}
	p.stmts = append(p.stmts, selectStmt)
}

func (p *Parser) parseInsertStmt() {
	insertStmt := ast.InsertStmt{}

	_, _ = p.expectToken(intoId)
	_, insertStmt.TableName = p.expectToken(name)
	_, _ = p.expectToken(valuesId)
	_, _ = p.expectToken(lParenthesis)

	insertStmt.Values = append(insertStmt.Values, p.parseLiteral())
	for p.err == nil {
		tok, _ := p.expectToken(comma, rParenthesis)
		if tok == comma {
			insertStmt.Values = append(insertStmt.Values, p.parseLiteral())
		} else {
			break
		}
	}
	_, _ = p.expectToken(semicolon)
	p.stmts = append(p.stmts, insertStmt)
}

func (p *Parser) parseDeleteStmt() {
	deleteStmt := ast.DeleteStmt{}

	_, _ = p.expectToken(fromId)
	_, deleteStmt.TableName = p.expectToken(name)
	tok, _ := p.expectToken(whereId, semicolon)
	if tok == whereId {
		deleteStmt.Where = p.parseWhereClause()
		_, _ = p.expectToken(semicolon)
	}
	p.stmts = append(p.stmts, deleteStmt)
}

func (p *Parser) parseLiteral() interface{} {
	valueType, valueLit := p.expectToken(stringLiteral, intLiteral)
	switch valueType {
	case stringLiteral:
		return valueLit
	case intLiteral:
		valueInt, err := strconv.Atoi(valueLit)
		if err != nil {
			p.err = InvalidIntLiteral
		} else {
			return valueInt
		}
	}
	return nil
}

func (p *Parser) parseUpdatePair() ast.UpdatePair {
	updatePair := ast.UpdatePair{}

	_, updatePair.FieldName = p.expectToken(name)
	_, _ = p.expectToken(eq)
	updatePair.Value = p.parseLiteral()
	return updatePair
}

func (p *Parser) parseUpdateStmt() {
	updateStmt := ast.UpdateStmt{}

	_, updateStmt.TableName = p.expectToken(name)
	_, _ = p.expectToken(setId)

	updatePairs := []ast.UpdatePair{p.parseUpdatePair()}

	haveWhereClause := false
loop:
	for p.err == nil {
		tok, _ := p.expectToken(comma, semicolon, whereId)
		switch tok {
		case comma:
			updatePairs = append(updatePairs, p.parseUpdatePair())
		case whereId:
			haveWhereClause = true
			fallthrough
		default:
			break loop
		}
	}
	updateStmt.UpdatePairs = updatePairs
	if haveWhereClause {
		updateStmt.Where = p.parseWhereClause()
		_, _ = p.expectToken(semicolon)
	}
	p.stmts = append(p.stmts, updateStmt)
}

var compTok2CompOp = map[token]ast.CmpOp{
	lt:        ast.LT,
	le:        ast.LE,
	eq:        ast.EQ,
	ne:        ast.NE,
	ge:        ast.GE,
	gt:        ast.GT,
	betweenId: ast.BETWEEN,
}

func (p *Parser) parseWhereClause() *ast.WhereClause {
	where := &ast.WhereClause{}

	_, where.FieldName = p.expectToken(name)
	compTok, _ := p.expectToken(lt, le, eq, gt, ge, ne, betweenId)
	if p.err != nil {
		return where
	}
	where.CmpOp = compTok2CompOp[compTok]
	where.Value = p.parseLiteral()

	if compTok == betweenId {
		_, _ = p.expectToken(andId)
		where.ValueOpt = p.parseLiteral()
	}
	return where
}

func (p *Parser) parseBeginStmt() {
	_, _ = p.expectToken(semicolon)
	p.stmts = append(p.stmts, ast.BeginStmt{})
}

func (p *Parser) parseRollbackStmt() {
	_, _ = p.expectToken(semicolon)
	p.stmts = append(p.stmts, ast.RollbackStmt{})
}

func (p *Parser) parseCommitStmt() {
	_, _ = p.expectToken(semicolon)
	p.stmts = append(p.stmts, ast.CommitStmt{})
}

func (p *Parser) parseShowStmt() {
	showStmt := ast.ShowStmt{}

	tok, lit := p.expectToken(tablesId, name)
	_, _ = p.expectToken(semicolon)
	if tok == tablesId {
		showStmt.ShowTables = true
	} else {
		showStmt.TableName = lit
	}
	p.stmts = append(p.stmts, showStmt)
}
