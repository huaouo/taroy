// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package parser

import (
	"bufio"
	"bytes"
	"errors"
	"strings"
	"unicode"
)

const eofRune rune = 0

var (
	EOF                           = errors.New("EOF")
	InvalidEscapeCharacter        = errors.New("invalid escape character")
	InvalidSymbol                 = errors.New("invalid symbol")
	MissingTerminatingDoubleQuote = errors.New("missing terminating \"")
)

func isWhitespace(ch rune) bool {
	return ch == ' ' || ch == '\t' || ch == '\n'
}

func isLetter(ch rune) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z'
}

func isDigit(ch rune) bool {
	return '0' <= ch && ch <= '9'
}

func isSymbol(ch rune) bool {
	return ch == '<' || ch == '=' || ch == '>' || ch == '!' ||
		ch == '*' || ch == ',' || ch == ';' || ch == '(' || ch == ')'
}

type lexer struct {
	r   *bufio.Reader
	err error
}

func newLexer(sql string) *lexer {
	return &lexer{r: bufio.NewReader(strings.NewReader(sql))}
}

func (l *lexer) read() rune {
	ch, _, err := l.r.ReadRune()
	if err != nil {
		return eofRune
	}

	if isLetter(ch) && unicode.IsUpper(ch) {
		ch = unicode.ToLower(ch)
	}
	return ch
}

func (l *lexer) unread() {
	_ = l.r.UnreadRune()
}

func (l *lexer) scanIntLiteral() (token, string) {
	buf := new(bytes.Buffer)

	ch := l.read()
	for isDigit(ch) {
		buf.WriteRune(ch)
		ch = l.read()
	}
	l.unread()

	return intLiteral, buf.String()
}

func (l *lexer) getEscapedRune() (rune, error) {
	switch ch := l.read(); ch {
	case '"':
		return '"', nil
	case 'n':
		return '\n', nil
	case 'r':
		return '\r', nil
	case 't':
		return '\t', nil
	case '\\':
		return '\\', nil
	default:
		l.err = InvalidEscapeCharacter
		return eofRune, InvalidEscapeCharacter
	}
}

func (l *lexer) scanStringLiteral() (token, string, error) {
	_ = l.read()
	buf := new(bytes.Buffer)

	ch := l.read()
	for ch != eofRune && ch != '"' && ch != '\n' {
		if ch != '\\' {
			buf.WriteRune(ch)
		} else {
			ch, err := l.getEscapedRune()
			if err != nil {
				l.err = err
				return illegal, "", err
			}
			buf.WriteRune(ch)
		}
		ch = l.read()
	}

	if ch != '"' {
		l.err = MissingTerminatingDoubleQuote
		return illegal, "", MissingTerminatingDoubleQuote
	}

	return stringLiteral, buf.String(), nil
}

var keywordMap = map[string]token{
	"create":   createId,
	"drop":     dropId,
	"select":   selectId,
	"insert":   insertId,
	"delete":   deleteId,
	"update":   updateId,
	"begin":    beginId,
	"rollback": rollbackId,
	"commit":   commitId,
	"show":     showId,
	"set":      setId,
	"table":    tableId,
	"tables":   tablesId,
	"primary":  primaryId,
	"unique":   uniqueId,
	"index":    indexId,
	"from":     fromId,
	"into":     intoId,
	"values":   valuesId,
	"where":    whereId,
	"between":  betweenId,
	"and":      andId,
	"int":      intId,
	"string":   stringId,
}

func (l *lexer) scanIdentifierAndName() (token, string) {
	buf := new(bytes.Buffer)
	buf.WriteRune(l.read())

	ch := l.read()
	for isDigit(ch) || isLetter(ch) || ch == '_' {
		buf.WriteRune(ch)
		ch = l.read()
	}
	l.unread()

	str := buf.String()
	if identifier, ok := keywordMap[str]; ok {
		return identifier, ""
	}
	return name, str
}

func (l *lexer) scanSymbol() (token, error) {
	switch l.read() {
	case '=':
		return eq, nil
	case '*':
		return star, nil
	case ',':
		return comma, nil
	case ';':
		return semicolon, nil
	case '(':
		return lParenthesis, nil
	case ')':
		return rParenthesis, nil
	case '!':
		ch := l.read()
		if ch != '=' {
			l.err = InvalidSymbol
			return illegal, InvalidSymbol
		}
		return ne, nil
	case '<':
		ch := l.read()
		if ch != '=' {
			l.unread()
			return lt, nil
		}
		return le, nil
	case '>':
		ch := l.read()
		if ch != '=' {
			l.unread()
			return gt, nil
		}
		return ge, nil
	default:
		l.err = InvalidSymbol
		return illegal, InvalidSymbol
	}
}

func (l *lexer) nextToken() (token, string, error) {
	if l.err != nil {
		return illegal, "", l.err
	}

	ch := l.read()
	for isWhitespace(ch) {
		ch = l.read()
	}
	l.unread()

	switch {
	case ch == eofRune:
		l.err = EOF
		return illegal, "", EOF
	case isDigit(ch):
		digit, str := l.scanIntLiteral()
		return digit, str, nil
	case ch == '"':
		return l.scanStringLiteral()
	case ch == '_' || isLetter(ch):
		identifier, str := l.scanIdentifierAndName()
		return identifier, str, nil
	case isSymbol(ch):
		symbol, err := l.scanSymbol()
		return symbol, "", err
	default:
		l.err = InvalidSymbol
		return illegal, "", InvalidSymbol
	}
}
