package btree

import (
	"fmt"
	"github.com/unamdev0/ZiDB/internal/storage"
	"github.com/unamdev0/ZiDB/pkg/constants"

	"github.com/unamdev0/ZiDB/pkg/utils"

	"os"
	"unsafe"
)

func leafNodeNumCell(nodeAddr uintptr) uintptr {

	return nodeAddr + constants.LEAF_NODE_NUM_CELLS_OFFSET
}

func leafNodeCell(node uintptr, cellNum uint32) uintptr {
	return node + constants.LEAF_NODE_HEADER_SIZE + (uintptr(cellNum) * constants.LEAF_NODE_CELL_SIZE)
}

func leafNodeValue(node uintptr, cellNum uint32) uintptr {
	return leafNodeCell(node, cellNum) + constants.LEAF_NODE_KEY_SIZE
}

func leafNodeNextLeaf(nodeAddr uintptr) uintptr {
	return nodeAddr + constants.LEAF_NODE_NEXT_LEAF_OFFSET
}

func initalizeLeafNode(node uintptr) {

	setNodeType(node, constants.Leaf)
	*(*uint32)(unsafe.Pointer(leafNodeNumCell(node))) = 0
	setNodeRoot(node, false)
	*(*uint8)(unsafe.Pointer(node + constants.NODE_TYPE_OFFSET)) = constants.Leaf
	*(*uint32)(unsafe.Pointer(leafNodeNextLeaf(node))) = 0

}

func initializeInternalNode(node uintptr) {

	setNodeType(node, constants.Internal)
	*(*uint32)(unsafe.Pointer(leafNodeNumCell(node))) = 0
	setNodeRoot(node, false)
	*(*uint8)(unsafe.Pointer(node + constants.NODE_TYPE_OFFSET)) = constants.Internal
	*(*uint32)(unsafe.Pointer(internalNodeRightChild(node))) = constants.INVALID_PAGE_NUM

}

func internalNodeNumKeys(nodeAddr uintptr) uintptr {
	return nodeAddr + constants.INTERNAL_NODE_NUM_KEYS_OFFSET
}

func internalNodeRightChild(nodeAddr uintptr) uintptr {
	return nodeAddr + constants.INTERNAL_NODE_RIGHT_CHILD_OFFSET
}

func internalNodeCell(nodeAddr uintptr, cellNum uint32) uintptr {
	return nodeAddr + constants.INTERNAL_NODE_HEADER_SIZE + uintptr(cellNum)*constants.INTERNAL_NODE_CELL_SIZE
}

func internalNodeChild(nodeAddr uintptr, childNum uint32) uintptr {
	numKeys := utils.ReadUint32(internalNodeNumKeys(nodeAddr))
	if childNum > numKeys {
		fmt.Printf("Tried to access ChildNum %d > NumKeys %d\n", childNum, numKeys)
		os.Exit(-1)
		return nodeAddr
	} else if childNum == numKeys {

		//DOUBT
		rightChild := internalNodeRightChild(nodeAddr)

		if *(*uint32)(unsafe.Pointer(rightChild)) == (constants.INVALID_PAGE_NUM) {
			fmt.Println("Tried to access right child of node, but was invalid page")
			os.Exit(-1)
		}
		return rightChild
	} else {
		child := internalNodeCell(nodeAddr, childNum)
		if *(*uint32)(unsafe.Pointer(child)) == (constants.INVALID_PAGE_NUM) {
			fmt.Println("Tried to access child of node, but was invalid page")
			os.Exit(-1)
		}
		return child
	}
}

func internalNodeKey(nodeAddr uintptr, keyNum uint32) uintptr {
	return internalNodeCell(nodeAddr, keyNum) + constants.INTERNAL_NODE_CHILD_SIZE
}

func updateInternalNode(nodeAddr uintptr, oldMaxValue, newMaxValue uint32) {
	oldChildIndex := fetchInternalNodeChildIndex(nodeAddr, oldMaxValue)
	*(*uint32)(unsafe.Pointer(internalNodeKey(nodeAddr, oldChildIndex))) = newMaxValue
}

func getNodeType(nodeAddr uintptr) uint8 {
	return *(*uint8)(unsafe.Pointer(nodeAddr + constants.NODE_TYPE_OFFSET))
}

func parentNode(nodeAddr uintptr) uintptr {
	return nodeAddr + constants.PARENT_POINTER_OFFSET
}

func InsertLeafNode(cursor *Cursor, key uint32, data *storage.Row) {

	node, _ := storage.GetPage(cursor.Table.Pager, cursor.PageNum)
	numCells := utils.ReadUint32(leafNodeNumCell(node))
	if numCells >= uint32(constants.LEAF_NODE_MAX_CELLS) {
		SplitNode(cursor, key, data)
		return
	}

	if cursor.CellNum < numCells {
		for i := numCells; i > cursor.CellNum; i-- {
			copy(unsafe.Slice((*byte)(unsafe.Pointer(leafNodeCell(node, i))), constants.LEAF_NODE_CELL_SIZE), unsafe.Slice((*byte)(unsafe.Pointer(leafNodeCell(node, i-1))), constants.LEAF_NODE_CELL_SIZE))
		}
	}

	*(*uint32)(unsafe.Pointer(leafNodeNumCell(node))) = utils.ReadUint32(leafNodeNumCell(node)) + 1
	*(*uint32)(unsafe.Pointer(leafNodeCell(node, cursor.CellNum))) = key
	storage.Store(leafNodeValue(node, cursor.CellNum), data)

}

func setNodeRoot(nodeAddr uintptr, isRoot bool) {
	if isRoot {
		*(*uint8)(unsafe.Pointer(nodeAddr + constants.IS_ROOT_OFFSET)) = 1
	} else {
		*(*uint8)(unsafe.Pointer(nodeAddr + constants.IS_ROOT_OFFSET)) = 0
	}
}

func setNodeType(nodeAddr uintptr, nodeType int) {

	if nodeType == constants.Leaf {
		*(*uint8)(unsafe.Pointer(nodeAddr + constants.NODE_TYPE_OFFSET)) = constants.Leaf
	} else if nodeType == constants.Internal {
		*(*uint8)(unsafe.Pointer(nodeAddr + constants.NODE_TYPE_OFFSET)) = constants.Internal

	}
}

func fetchInternalNodeChildIndex(nodeAddr uintptr, key uint32) uint32 {

	numKeys := utils.ReadUint32(internalNodeNumKeys(nodeAddr))
	minIndex := uint32(0)
	maxIndex := numKeys

	for minIndex != maxIndex {
		index := (minIndex + maxIndex) / 2
		maxKey := utils.ReadUint32(internalNodeKey(nodeAddr, index))
		if maxKey >= key {
			maxIndex = index
		} else {
			minIndex = index + 1
		}
	}

	return minIndex

}

func isNodeRoot(nodeAddr uintptr) bool {

	return *(*bool)(unsafe.Pointer(nodeAddr + constants.IS_ROOT_OFFSET))

}

func getNodeMaxKey(pager *storage.Pager, nodeAddr uintptr) uintptr {
	if getNodeType(nodeAddr) == constants.Leaf {
		return leafNodeCell(nodeAddr, utils.ReadUint32(leafNodeNumCell(nodeAddr))-1)
	}

	rightNode, _ := storage.GetPage(pager, utils.ReadUint32(internalNodeRightChild(nodeAddr)))

	return getNodeMaxKey(pager, rightNode)

}
