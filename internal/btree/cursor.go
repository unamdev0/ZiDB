package btree

import (
	"fmt"
	"github.com/unamdev0/ZiDB/internal/storage"
	"github.com/unamdev0/ZiDB/pkg/constants"

	"github.com/unamdev0/ZiDB/pkg/utils"

	"os"
)

type Cursor struct {
	Table   *Table
	PageNum uint32
	CellNum uint32
	IsEnd   bool
}

func CursorValue(cursor *Cursor) uintptr {

	pageNum := cursor.PageNum
	page, err := storage.GetPage(cursor.Table.Pager, pageNum)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	return leafNodeValue(page, cursor.CellNum)
}

func StartingCursor(table *Table) *Cursor {

	cursor := FetchNodeCursor(table, 0)
	rootNode, _ := storage.GetPage(table.Pager, cursor.PageNum)
	numCells := utils.ReadUint32(leafNodeNumCell(rootNode))

	cursor.IsEnd = numCells == 0
	return cursor
}

func EndingCursor(table *Table) *Cursor {
	rootNode, _ := storage.GetPage(table.Pager, table.RootPageNum)
	numCells := utils.ReadUint32(leafNodeNumCell(rootNode))

	return &Cursor{
		Table:   table,
		PageNum: table.RootPageNum,
		CellNum: numCells,
		IsEnd:   true,
	}
}

func AdvanceCursor(cursor *Cursor) *Cursor {

	pageNum := cursor.PageNum
	node, _ := storage.GetPage(cursor.Table.Pager, pageNum)

	cursor.CellNum += 1
	if cursor.CellNum >= utils.ReadUint32(leafNodeNumCell(node)) {

		nextLeafNode := utils.ReadUint32(leafNodeNextLeaf(node))
		if nextLeafNode == 0 {
			cursor.IsEnd = true
		} else {
			cursor.PageNum = nextLeafNode
			cursor.CellNum = 0
		}
	}
	return cursor

}

func FetchNodeCursor(table *Table, key uint32) *Cursor {

	node, _ := storage.GetPage(table.Pager, table.RootPageNum)
	if getNodeType(node) == constants.Leaf {
		return fetchLeafNode(table, table.RootPageNum, key)
	} else if getNodeType(node) == constants.Internal {
		return fetchInternalNode(table, table.RootPageNum, key)
	} else {
		os.Exit(-1)
		return fetchInternalNode(table, table.RootPageNum, key)

	}

}

func fetchLeafNode(table *Table, pageNum, key uint32) *Cursor {
	node, _ := storage.GetPage(table.Pager, pageNum)
	numCells := utils.ReadUint32(leafNodeNumCell(node))
	minIndex := uint32(0)
	maxIndex := numCells
	for maxIndex != minIndex {
		index := (minIndex + maxIndex) / 2
		keyAtIndex := utils.ReadUint32(leafNodeCell(node, index))
		if keyAtIndex == key {
			return &Cursor{
				Table:   table,
				PageNum: pageNum,
				CellNum: index,
				IsEnd:   false,
			}
		}
		if key < keyAtIndex {
			maxIndex = index
		} else {
			minIndex = index + 1
		}
	}

	return &Cursor{
		Table:   table,
		PageNum: pageNum,
		CellNum: minIndex,
		IsEnd:   false,
	}
}

func fetchInternalNode(table *Table, pageNum, key uint32) *Cursor {
	node, _ := storage.GetPage(table.Pager, pageNum)

	childIndex := fetchInternalNodeChildIndex(node, key)
	childNum := utils.ReadUint32(internalNodeChild(node, childIndex))
	childNode, _ := storage.GetPage(table.Pager, childNum)

	if getNodeType(childNode) == constants.Leaf {
		return fetchLeafNode(table, childNum, key)
	} else if getNodeType(childNode) == constants.Internal {
		return fetchInternalNode(table, childNum, key)
	} else {
		os.Exit(-1)
		return fetchInternalNode(table, childNum, key)

	}

}
