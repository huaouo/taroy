// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

package parser

type token int

const (
	illegal token = iota

	createId
	dropId
	selectId
	insertId
	deleteId
	updateId
	beginId
	rollbackId
	commitId
	showId
	setId
	tableId
	tablesId
	primaryId
	uniqueId
	indexId
	fromId
	intoId
	valuesId
	whereId
	betweenId
	andId
	stringId
	intId

	lt
	le
	eq  // =
	gt
	ge
	ne  // !=
	star
	comma
	semicolon
	lParenthesis
	rParenthesis

	name  // tableName, fieldName ...

	stringLiteral
	intLiteral
)

var tokenNames = map[token]string{
	illegal: "illegal",

	createId:   "createId",
	dropId:     "dropId",
	selectId:   "selectId",
	insertId:   "insertId",
	deleteId:   "deleteId",
	updateId:   "updateId",
	beginId:    "beginId",
	rollbackId: "rollbackId",
	commitId:   "commitId",
	showId:     "showId",
	setId:      "setId",
	tableId:    "tableId",
	tablesId:   "tablesId",
	primaryId:  "primaryId",
	uniqueId:   "uniqueId",
	indexId:    "indexId",
	fromId:     "fromId",
	intoId:     "intoId",
	valuesId:   "valuesId",
	whereId:    "whereId",
	betweenId:  "betweenId",
	andId:      "andId",
	stringId:   "stringId",
	intId:      "intId",

	lt:           "lt",
	le:           "le",
	eq:           "eq",
	gt:           "gt",
	ge:           "ge",
	ne:           "ne",
	star:         "star",
	comma:        "comma",
	semicolon:    "semicolon",
	lParenthesis: "lParenthesis",
	rParenthesis: "rParenthesis",

	name: "name",

	stringLiteral: "stringLiteral",
	intLiteral:    "intLiteral",
}

func (t token) String() string {
	return tokenNames[t]
}
