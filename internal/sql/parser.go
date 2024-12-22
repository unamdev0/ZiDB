package sql

import (
	"strings"

	"github.com/unamdev0/ZiDB/internal/storage"
	"github.com/unamdev0/ZiDB/pkg/constants"
)

type Statement struct {
	Type        int
	Input       string
	RowToInsert storage.Row
}

func PrepareCommand(input string) (*Statement, error) {
	var statement Statement
	statement.Input = input

	if strings.HasPrefix(input, "select") {
		statement.Type = constants.Select
	} else if strings.HasPrefix(input, "insert") {
		statement.Type = constants.Insert
	} else {
		return &statement, constants.ErrUnrecognizedStatement
	}

	return &statement, nil
}
