package sql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/unamdev0/ZiDB/internal/btree"
	"github.com/unamdev0/ZiDB/internal/storage"
	"github.com/unamdev0/ZiDB/pkg/constants"
)

func ExecuteCommand(statement *Statement, table *btree.Table) string {
	if statement.Type == constants.Select {
		selectCommand(table)
		return "EXECUTING SELECT COMMAND"
	} else if statement.Type == constants.Insert {
		insertCommand(statement, table)
		return "Executing insert Command"
	}
	return constants.ErrUnrecognizedCommand.Error()
}

func selectCommand(table *btree.Table) {
	cursor := btree.StartingCursor(table)

	for !cursor.IsEnd {
		var row storage.Row
		ptr := btree.CursorValue(cursor)
		storage.Read(&row, ptr)
		fmt.Print(storage.PrintRow(&row))
		cursor = btree.AdvanceCursor(cursor)
	}
}

func insertCommand(statement *Statement, table *btree.Table) string {
	args := strings.Fields(statement.Input)
	if len(args) < 4 {
		return constants.ErrSyntaxError.Error()
	}
	primaryId, err := strconv.Atoi(args[1])

	if err != nil {
		fmt.Println("Conversion error:", err)
	}
	statement.RowToInsert.Id = uint32(primaryId)
	copy(statement.RowToInsert.Username[:], args[2])

	copy(statement.RowToInsert.Email[:], args[3])

	cursor := btree.FetchNodeCursor(table, statement.RowToInsert.Id)
	btree.InsertLeafNode(cursor, statement.RowToInsert.Id, &statement.RowToInsert)
	return "Executing insert Command"
}
