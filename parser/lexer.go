// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package parser

import (
	"bufio"
	"bytes"
	"errors"
	"io"
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
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z';
}

func isDigit(ch rune) bool {
	return '0' <= ch && ch <= '9'
}

func isSymbol(ch rune) bool {
	return ch == '<' || ch == '=' || ch == '>' || ch == '!' ||
		ch == '*' || ch == ',' || ch == ';' || ch == '(' || ch == ')';
}

type lexer struct {
	r   *bufio.Reader
	err error
}

func newLexer(r io.Reader) *lexer {
	return &lexer{r: bufio.NewReader(r)}
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
	for ch != '"' && ch != '\n' {
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
	}

	if ch != '"' {
		l.err = MissingTerminatingDoubleQuote
		return illegal, "", MissingTerminatingDoubleQuote
	}

	return stringLiteral, buf.String(), nil
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

	switch str := buf.String(); str {
	case "create":
		return createId, ""
	case "drop":
		return dropId, ""
	case "select":
		return selectId, ""
	case "insert":
		return insertId, ""
	case "delete":
		return deleteId, ""
	case "update":
		return updateId, ""
	case "begin":
		return beginId, ""
	case "rollback":
		return rollbackId, ""
	case "commit":
		return commitId, ""
	case "show":
		return showId, ""
	case "set":
		return setId, ""
	case "table":
		return tableId, ""
	case "tables":
		return tablesId, ""
	case "primary":
		return primaryId, ""
	case "unique":
		return uniqueId, ""
	case "index":
		return indexId, ""
	case "from":
		return fromId, ""
	case "into":
		return intoId, ""
	case "values":
		return valuesId, ""
	case "where":
		return whereId, ""
	case "between":
		return betweenId, ""
	case "and":
		return andId, ""
	case "int":
		return intId, ""
	case "string":
		return stringId, ""
	default:
		return name, str
	}
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
