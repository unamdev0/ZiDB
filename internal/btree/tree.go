package btree

import (
	"github.com/unamdev0/ZiDB/internal/storage"
	"github.com/unamdev0/ZiDB/pkg/constants"
	"github.com/unamdev0/ZiDB/pkg/utils"
	"unsafe"
)

type Table struct {
	RootPageNum uint32
	Pager       *storage.Pager
}

func CreateNewRoot(table *Table, pageNum uint32) {
	root, _ := storage.GetPage(table.Pager, table.RootPageNum)
	rightNode, _ := storage.GetPage(table.Pager, pageNum)
	leftPageNum := storage.FetchUnusedPageNum(table.Pager)
	leftNode, _ := storage.GetPage(table.Pager, leftPageNum)
	if getNodeType(root) == constants.Internal {
		initializeInternalNode(rightNode)
		initializeInternalNode(leftNode)
	}
	copy(unsafe.Slice((*byte)(unsafe.Pointer(leftNode)), constants.PAGE_SIZE), unsafe.Slice((*byte)(unsafe.Pointer(root)), constants.PAGE_SIZE))
	setNodeRoot(leftNode, false)

	if getNodeType(leftNode) == constants.Internal {
		var child uintptr
		numKeys := utils.ReadUint32(internalNodeNumKeys(leftNode))
		for i := 0; i < int(numKeys); i++ {
			child, _ = storage.GetPage(table.Pager, utils.ReadUint32(internalNodeChild(leftNode, uint32(i))))

			*(*uint32)(unsafe.Pointer(parentNode(child))) = leftPageNum
		}
	}
	initializeInternalNode(root)
	setNodeRoot(root, true)
	*(*uint32)(unsafe.Pointer(internalNodeNumKeys(root))) = 1
	*(*uint32)(unsafe.Pointer(internalNodeChild(root, 0))) = leftPageNum
	leftChildMaxKey := utils.ReadUint32(getNodeMaxKey(table.Pager, leftNode))
	*(*uint32)(unsafe.Pointer(internalNodeKey(root, 0))) = leftChildMaxKey
	*(*uint32)(unsafe.Pointer(internalNodeRightChild(root))) = pageNum
	*(*uint32)(unsafe.Pointer(parentNode(leftNode))) = table.RootPageNum
	*(*uint32)(unsafe.Pointer(parentNode(rightNode))) = table.RootPageNum

}

func SplitNode(cursor *Cursor, key uint32, data *storage.Row) {
	oldNode, _ := storage.GetPage(cursor.Table.Pager, cursor.PageNum)
	oldMaxVal := utils.ReadUint32(getNodeMaxKey(cursor.Table.Pager, oldNode))
	newPageNum := storage.FetchUnusedPageNum(cursor.Table.Pager)

	newNode, _ := storage.GetPage(cursor.Table.Pager, newPageNum)
	initalizeLeafNode(newNode)

	*(*uint32)(unsafe.Pointer(parentNode(newNode))) = utils.ReadUint32(parentNode(oldNode))
	*(*uint32)(unsafe.Pointer(leafNodeNextLeaf(newNode))) = utils.ReadUint32(leafNodeNextLeaf(oldNode))
	*(*uint32)(unsafe.Pointer(leafNodeNextLeaf(oldNode))) = newPageNum

	var keyDestinationNode uintptr
	var keyDestinationIndex uint32

	for i := int(constants.LEAF_NODE_MAX_CELLS); i >= 0; i-- {
		var destinationNode uintptr
		var indexInNode uint32

		if uint32(i) >= constants.LEAF_NODE_LEFT_SPLIT_COUNT {
			destinationNode = newNode
			indexInNode = uint32(i) - constants.LEAF_NODE_LEFT_SPLIT_COUNT
		} else {
			destinationNode = oldNode
			indexInNode = uint32(i)
		}

		if uint32(i) == cursor.CellNum {
			keyDestinationNode = destinationNode
			keyDestinationIndex = indexInNode
		} else if uint32(i) > cursor.CellNum {
			if i > 0 {
				source := leafNodeCell(oldNode, uint32(i-1))
				dest := leafNodeCell(destinationNode, indexInNode)
				copy(unsafe.Slice((*byte)(unsafe.Pointer(dest)), constants.LEAF_NODE_CELL_SIZE),
					unsafe.Slice((*byte)(unsafe.Pointer(source)), constants.LEAF_NODE_CELL_SIZE))
			}
		} else {
			source := leafNodeCell(oldNode, uint32(i))
			dest := leafNodeCell(destinationNode, indexInNode)
			copy(unsafe.Slice((*byte)(unsafe.Pointer(dest)), constants.LEAF_NODE_CELL_SIZE),
				unsafe.Slice((*byte)(unsafe.Pointer(source)), constants.LEAF_NODE_CELL_SIZE))
		}
	}

	if keyDestinationNode != 0 {
		destination := leafNodeCell(keyDestinationNode, keyDestinationIndex)
		*(*uint32)(unsafe.Pointer(destination)) = key
		storage.Store(leafNodeValue(keyDestinationNode, keyDestinationIndex), data)
	}

	*(*uint32)(unsafe.Pointer(leafNodeNumCell(oldNode))) = constants.LEAF_NODE_LEFT_SPLIT_COUNT
	*(*uint32)(unsafe.Pointer(leafNodeNumCell(newNode))) = constants.LEAF_NODE_RIGHT_SPLIT_COUNT

	if isNodeRoot(oldNode) {
		CreateNewRoot(cursor.Table, newPageNum)
		return
	} else {
		parentPage := utils.ReadUint32(parentNode(oldNode))
		newMaxVal := utils.ReadUint32(getNodeMaxKey(cursor.Table.Pager, oldNode))
		parentNode, _ := storage.GetPage(cursor.Table.Pager, parentPage)

		updateInternalNode(parentNode, oldMaxVal, newMaxVal)
		InternalNodeInsert(cursor.Table, parentPage, newPageNum)
		return

	}
}

func InternalNodeInsert(table *Table, parentPage, childPage uint32) {

	parentNode, _ := storage.GetPage(table.Pager, parentPage)
	childNode, _ := storage.GetPage(table.Pager, childPage)
	childMaxVal := utils.ReadUint32(getNodeMaxKey(table.Pager, childNode))
	index := fetchInternalNodeChildIndex(parentNode, childMaxVal)
	originalNumKeys := utils.ReadUint32(internalNodeNumKeys(parentNode))

	if originalNumKeys >= constants.INTERNAL_NODE_MAX_KEYS {
		InternalNodeSplitAndInsert(table, parentPage, childPage)
		return
	}
	rightChildPageNum := utils.ReadUint32(internalNodeRightChild(parentNode))
	if rightChildPageNum == constants.INVALID_PAGE_NUM {
		*(*uint32)(unsafe.Pointer(internalNodeRightChild(parentNode))) = childPage
		return

	}
	rightChildNode, _ := storage.GetPage(table.Pager, rightChildPageNum)
	*(*uint32)(unsafe.Pointer(internalNodeNumKeys(parentNode))) = originalNumKeys + 1

	if childMaxVal > utils.ReadUint32(getNodeMaxKey(table.Pager, rightChildNode)) {
		*(*uint32)(unsafe.Pointer(internalNodeChild(parentNode, originalNumKeys))) = rightChildPageNum

		*(*uint32)(unsafe.Pointer(internalNodeKey(parentNode, originalNumKeys))) = utils.ReadUint32(getNodeMaxKey(table.Pager, rightChildNode))
		*(*uint32)(unsafe.Pointer(internalNodeRightChild(parentNode))) = childPage

	} else {
		for i := originalNumKeys; i > index; i-- {
			copy(unsafe.Slice((*byte)(unsafe.Pointer(internalNodeCell(parentNode, i))), constants.INTERNAL_NODE_CELL_SIZE), unsafe.Slice((*byte)(unsafe.Pointer(internalNodeCell(parentNode, i-1))), constants.INTERNAL_NODE_CELL_SIZE))
		}
		*(*uint32)(unsafe.Pointer(internalNodeChild(parentNode, index))) = childPage
		*(*uint32)(unsafe.Pointer(internalNodeKey(parentNode, index))) = childMaxVal

	}

}

func InternalNodeSplitAndInsert(table *Table, parentPageNum, childPageNum uint32) {
	oldPage := parentPageNum
	oldNode, _ := storage.GetPage(table.Pager, oldPage)
	oldMaxVal := utils.ReadUint32(getNodeMaxKey(table.Pager, oldNode))

	childNode, _ := storage.GetPage(table.Pager, childPageNum)
	childMaxVal := utils.ReadUint32(getNodeMaxKey(table.Pager, childNode))

	newPageNum := storage.FetchUnusedPageNum(table.Pager)
	isSplittingRoot := isNodeRoot(oldNode)

	var parent uintptr
	var newNode uintptr
	if isSplittingRoot {
		CreateNewRoot(table, newPageNum)
		parent, _ = storage.GetPage(table.Pager, table.RootPageNum)

		oldPage = utils.ReadUint32(internalNodeChild(parent, 0))
		oldNode, _ = storage.GetPage(table.Pager, oldPage)
	} else {

		parent, _ = storage.GetPage(table.Pager, utils.ReadUint32(parentNode(oldNode)))
		newNode, _ = storage.GetPage(table.Pager, newPageNum)

		initializeInternalNode(newNode)
	}

	oldNumKeys := internalNodeNumKeys(oldNode)
	currentPage := utils.ReadUint32(internalNodeRightChild(oldNode))

	currNode, _ := storage.GetPage(table.Pager, currentPage)

	InternalNodeInsert(table, newPageNum, currentPage)
	*(*uint32)(unsafe.Pointer(parentNode(currNode))) = newPageNum
	*(*uint32)(unsafe.Pointer(internalNodeRightChild(oldNode))) = constants.INVALID_PAGE_NUM
	for i := constants.INTERNAL_NODE_MAX_KEYS - 1; i > constants.INTERNAL_NODE_MAX_KEYS/2; i-- {
		currentPage = utils.ReadUint32(internalNodeChild(oldNode, uint32(i)))

		currNode, _ = storage.GetPage(table.Pager, currentPage)
		*(*uint32)(unsafe.Pointer(parentNode(currNode))) = newPageNum
		*(*uint32)(unsafe.Pointer(oldNumKeys)) = utils.ReadUint32(oldNumKeys) - 1
	}
	*(*uint32)(unsafe.Pointer(internalNodeRightChild(oldNode))) = utils.ReadUint32(internalNodeChild(oldNode, utils.ReadUint32(oldNumKeys)) - 1)
	*(*uint32)(unsafe.Pointer(oldNumKeys)) = utils.ReadUint32(oldNumKeys) - 1

	maxAfterSplit := utils.ReadUint32(getNodeMaxKey(table.Pager, oldNode))
	var destinationPageNum uint32

	if childMaxVal < maxAfterSplit {
		destinationPageNum = oldPage
	} else {
		destinationPageNum = newPageNum
	}
	InternalNodeInsert(table, destinationPageNum, childPageNum)
	*(*uint32)(unsafe.Pointer(parentNode(childNode))) = destinationPageNum

	updateInternalNode(parent, oldMaxVal, utils.ReadUint32(getNodeMaxKey(table.Pager, oldNode)))

	if !isSplittingRoot {
		InternalNodeInsert(table, utils.ReadUint32(parentNode(oldNode)), newPageNum)

		*(*uint32)(unsafe.Pointer(parentNode(newNode))) =  utils.ReadUint32(parentNode(oldNode))
	}

}

func CloseDB(table *Table) {
	for i := 0; i < constants.MAX_PAGE_NUM; i++ {
		if table.Pager.Pages[i] != nil {
			storage.WriteToFile(table.Pager, uint32(i))
		} else {
			break
		}
	}
}

func OpenDB(filename string) (*Table, error) {
	pager, err := storage.OpenOrCreateFile(filename)

	if err != nil {
		return nil, err
	}
	table := &Table{RootPageNum: 0, Pager: pager}
	if pager.NumPages == 0 {
		rootNode, _ := storage.GetPage(pager, 0)
		initalizeLeafNode(rootNode)
		setNodeRoot(rootNode, true)
	}

	return table, nil

}
