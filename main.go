package main

// ITERATION 3:

//	Need to save data entered in a file
//  A single file which will contain all the pages(array of bytes)
//  when exiting program, all the data is written to this file
//  when server is started the whole data is again loaded from this file

// ITERATION 4:

// Need to create cursor(s)
// they'll be used to point to different rows and perform action based on it
// Initially two cursors will be created, one which will point to the start of the table and
// one will point to the last data of the table
// First will be used for select statements while the second will be used for insert statements

// **************************TODO***********************

// TODO: need to optimize struct declaration because of padding

// TODO: Make sure the address provided in rowAddress is divisible by 4,as we're going to start
//  	 saving id and then rest of the columns

//TODO: Skipped saving partial page to db for now

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"unsafe"
)

const defaultPort = ":6001"

var (
	ErrUnrecognizedStatement = errors.New("unrecognized statement")
	ErrUnrecognizedCommand   = errors.New("unrecognized command")
	ErrSyntaxError           = errors.New("syntax error")
	ErrOutOfBoundPageNum     = errors.New("page number entered is greater than total DB capacity")
	ErrReadingFile           = errors.New("can't read db file")
	ErrNoContentFound        = errors.New("page does not exist")
	ErrDataNotSaved          = errors.New("could not save data to DB")
)

const INVALID_PAGE_NUM = math.MaxUint32

const (
	COLUMN_USERNAME_SIZE = 32
	COLUMN_EMAIL_SIZE    = 255
	PAGE_SIZE            = 4096
	ROWS_PER_PAGE        = 100
	MAX_PAGE_NUM         = 100
	ID_OFFSET            = 0
	ID_SIZE              = unsafe.Sizeof(Row{}.id)
	USERNAME_SIZE        = unsafe.Sizeof(Row{}.username)
	EMAIL_SIZE           = unsafe.Sizeof(Row{}.email)
	USERNAME_OFFSET      = ID_OFFSET + ID_SIZE
	EMAIL_OFFSET         = USERNAME_OFFSET + USERNAME_SIZE
	ROW_SIZE             = ID_SIZE + EMAIL_SIZE + USERNAME_SIZE
)

const (
	NODE_TYPE_SIZE                = unsafe.Sizeof(uint8(0))
	NODE_TYPE_OFFSET              = 0
	IS_ROOT_SIZE                  = unsafe.Sizeof(uint8(0))
	IS_ROOT_OFFSET                = NODE_TYPE_SIZE
	PARENT_POINTER_SIZE           = unsafe.Sizeof(uint32(0))
	PARENT_POINTER_OFFSET         = NODE_TYPE_OFFSET + IS_ROOT_OFFSET
	COMMON_NODE_HEADER_SIZE       = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE
	LEAF_NODE_NUM_CELLS_SIZE      = unsafe.Sizeof(uint32(0))
	LEAF_NODE_NUM_CELLS_OFFSET    = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE
	LEAF_NODE_NEXT_LEAF_NODE_SIZE = unsafe.Sizeof(uint32(0))
	LEAF_NODE_NEXT_LEAF_OFFSET    = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE

	LEAF_NODE_HEADER_SIZE       = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_NODE_SIZE
	LEAF_NODE_KEY_SIZE          = unsafe.Sizeof(uint32(0))
	LEAF_NODE_KEY_OFFSET        = 0
	LEAF_NODE_VALUE_SIZE        = ROW_SIZE
	LEAF_NODE_VALUE_OFFSET      = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE
	LEAF_NODE_CELL_SIZE         = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
	LEAF_NODE_SPACE_FOR_CELLS   = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
	LEAF_NODE_MAX_CELLS         = uint32(LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE)
	LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2
	LEAF_NODE_LEFT_SPLIT_COUNT  = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT

	INTERNAL_NODE_NUM_KEYS_SIZE      = unsafe.Sizeof(uint32(0))
	INTERNAL_NODE_NUM_KEYS_OFFSET    = COMMON_NODE_HEADER_SIZE
	INTERNAL_NODE_RIGHT_CHILD_SIZE   = unsafe.Sizeof(uint32(0))
	INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE
	INTERNAL_NODE_HEADER_SIZE        = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE

	INTERNAL_NODE_KEY_SIZE   = unsafe.Sizeof(uint32(0))
	INTERNAL_NODE_CHILD_SIZE = unsafe.Sizeof(uint32(0))
	INTERNAL_NODE_CELL_SIZE  = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE

	INTERNAL_NODE_MAX_KEYS = 3
)

const (
	// starting with 1 as 0 will be put as default value
	Select = iota + 1
	Insert
)

const (
	// Node type 1 denotes leaf node 2 denotes internal node
	Leaf = iota + 1
	Internal
)

type MemoryBlock struct {
	data [PAGE_SIZE]byte
}

type Row struct {
	id       uint32
	username [COLUMN_USERNAME_SIZE]byte
	email    [COLUMN_EMAIL_SIZE]byte
}

type Statement struct {
	Type        int
	Input       string
	RowToInsert Row
	// InsertData
}

type Table struct {
	rootPageNum uint32
	pager       *Pager
}

type Pager struct {
	file     *os.File
	fileSize uint32
	numPages uint32
	pages    [MAX_PAGE_NUM]*MemoryBlock
}

type Cursor struct {
	table   *Table
	pageNum uint32
	cellNum uint32
	isEnd   bool
}

func cursorValue(cursor *Cursor) uintptr {

	pageNum := cursor.pageNum
	page, err := getPage(cursor.table.pager, pageNum)
	// if page == nil {
	// 	// Allocate memory only when we try to access the page
	// 	page = &MemoryBlock{}
	// 	table.pages[pageNum] = page
	// }
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	return leafNodeValue(uintptr(unsafe.Pointer(&page.data[0])), cursor.cellNum)
}

func store(to uintptr, data *Row) {
	*(*uint32)(unsafe.Pointer(to)) = data.id


	usernameSlice := (*[USERNAME_SIZE]byte)(unsafe.Pointer(to + uintptr(USERNAME_OFFSET)))

	copy(usernameSlice[:], data.username[:])

	emailSlice := (*[EMAIL_SIZE]byte)(unsafe.Pointer(to + uintptr(EMAIL_OFFSET)))

	copy(emailSlice[:], data.email[:])
}

func read(data *Row, from uintptr) {

	data.id = *(*uint32)(unsafe.Pointer(from))
	copy(data.username[:], (*(*[USERNAME_SIZE]byte)(unsafe.Pointer(from + uintptr(USERNAME_OFFSET))))[:])
	copy(data.email[:], (*(*[EMAIL_SIZE]byte)(unsafe.Pointer(from + uintptr(EMAIL_OFFSET))))[:])

}

// OpenOrCreateFile function opens a file if it exists, or creates it if it doesn't
func OpenOrCreateFile(filename string) (*Pager, error) {
	// Open the file with read-write permissions, creating it if necessary
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open or create file: %w", err)
	}

	fi, err := file.Stat()
	if err != nil {
		// Could not obtain stat, handle error
	}

	fmt.Printf("The file is %d bytes long\n", fi.Size())

	// Return the file wrapped in a FileHandler struct
	pager := &Pager{file: file, fileSize: uint32(fi.Size()), numPages: uint32(fi.Size() / PAGE_SIZE)}
	return pager, nil
}

func main() {
	table, err := openDB("db.txt")
	if err != nil {
		fmt.Println("Error while opening file:", err)
		os.Exit(-1)
	}
	// Listen for incoming connections on port 6001 by default
	listener, err := net.Listen("tcp", defaultPort)
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer listener.Close()
	fmt.Println("TCP server listening on port", defaultPort)

	for {
		// Accept an incoming connection
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		fmt.Println("Client connected")

		// // Handle connection in a separate goroutine
		go handleConnection(conn, table)
	}

}

func handleConnection(conn net.Conn, table *Table) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		// Read message from the client
		message, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Client disconnected")
			break
		}
		fmt.Printf("Received from client: %s", message)

		// If the client sends "exit", close the connection
		if message == "exit\n" {
			closeDB(table)
			fmt.Println("Client requested to close the connection")
			break
		}

		statement, err := prepareCommand(message)

		if err != nil {
			fmt.Printf("Error: %s\n", err)
		}

		// Send a response back to the client
		response := executeCommand(statement, table)
		_, err = conn.Write([]byte(response))
		if err != nil {
			fmt.Println("Error writing to client:", err)
			break
		}
	}
}

func prepareCommand(input string) (*Statement, error) {
	var statement Statement
	statement.Input = input
	if strings.HasPrefix(input, "select") {
		statement.Type = Select
	} else if strings.HasPrefix(input, "insert") {
		statement.Type = Insert
	} else {
		return &statement, ErrUnrecognizedStatement
	}

	return &statement, nil
}

func executeCommand(statement *Statement, table *Table) string {
	if statement.Type == Select {
		selectCommand(table)
		return "EXECUTING SELECT COMMAND"
	} else if statement.Type == Insert {
		insertCommand(statement, table)
		return "Executing insert Command"

	}
	return ErrUnrecognizedCommand.Error()
}

func insertCommand(statement *Statement, table *Table) string {
	args := strings.Fields(statement.Input)
	if len(args) < 4 {
		return ErrSyntaxError.Error()
	}
	primaryId, err := strconv.Atoi(args[1])

	if err != nil {
		fmt.Println("Conversion error:", err)
	}
	statement.RowToInsert.id = uint32(primaryId)
	copy(statement.RowToInsert.username[:], args[2])

	copy(statement.RowToInsert.email[:], args[3])

	cursor := fetchNodeCursor(table, statement.RowToInsert.id)
	insertLeafNode(cursor, statement.RowToInsert.id, &statement.RowToInsert)
	return "Executing insert Command"
}

func selectCommand(table *Table) {
	cursor := startingCursor(table)

	for !cursor.isEnd {

		var row Row
		ptr := cursorValue(cursor)

		read(&row, ptr)
		fmt.Print(printRow(&row))
		cursor = advanceCursor(cursor)
	}

}

func printRow(row *Row) string {
	return fmt.Sprintf("ROW ID->%d, USERNAME->%s, EMAIL->%s\n", row.id, row.username, row.email)
}

func getPage(pager *Pager, pageNum uint32) (*MemoryBlock, error) {
	if pageNum > MAX_PAGE_NUM {
		return nil, ErrOutOfBoundPageNum
	}

	if pager.pages[pageNum] == nil {
		var page MemoryBlock
		var numPages uint32
		page = MemoryBlock{}
		numPages = pager.fileSize / PAGE_SIZE

		if pager.fileSize%uint32(PAGE_SIZE) != 0 {
			numPages += 1
		}

		if pageNum <= numPages {
			// Calculate the offset and seek to the desired position
			offset := int64(pageNum * PAGE_SIZE)
			_, err := pager.file.Seek(offset, 0) // 0 is equivalent to SEEK_SET
			if err != nil {
				fmt.Println("Error seeking file:", err)
				return nil, ErrReadingFile
			}
			len, err := pager.file.Read(page.data[:])
			if len == -1 {
				fmt.Println("Error reading file:", err)
				return nil, ErrReadingFile
			}

		}
		pager.pages[pageNum] = &page

		if pageNum >= pager.numPages {
			pager.numPages = pageNum + 1
		}
	}
	return pager.pages[pageNum], nil
}

func openDB(filename string) (*Table, error) {
	pager, err := OpenOrCreateFile(filename)

	if err != nil {
		return nil, err
	}
	table := &Table{rootPageNum: 0, pager: pager}
	if pager.numPages == 0 {
		rootNode, _ := getPage(pager, 0)
		initalizeLeafNode(uintptr(unsafe.Pointer(&rootNode.data[0])))
		setNodeRoot(uintptr(unsafe.Pointer(&rootNode.data[0])), true)
	}

	return table, nil

}

func closeDB(table *Table) {
	for i := 0; i < MAX_PAGE_NUM; i++ {
		if table.pager.pages[i] != nil {
			writeToFile(table.pager, uint32(i))
		} else {
			break
		}
	}
}

func writeToFile(pager *Pager, pageNum uint32) {
	if pager.pages[pageNum] == nil {
		fmt.Println(ErrNoContentFound)
		os.Exit(-1)
	}

	offset := int64(pageNum * PAGE_SIZE)
	_, err := pager.file.WriteAt(pager.pages[pageNum].data[:], offset)
	if err != nil {
		fmt.Println(ErrDataNotSaved)
		os.Exit(-1)
	}
}

func startingCursor(table *Table) *Cursor {

	cursor := fetchNodeCursor(table, 0)
	rootNode, _ := getPage(table.pager, cursor.pageNum)
	numCells := *(*uint32)(unsafe.Pointer(leafNodeNumCell(uintptr(unsafe.Pointer(&rootNode.data[0])))))

	cursor.isEnd = numCells == 0
	return cursor
}

func endingCursor(table *Table) *Cursor {
	rootNode, _ := getPage(table.pager, table.rootPageNum)
	numCells := *(*uint32)(unsafe.Pointer(leafNodeNumCell(uintptr(unsafe.Pointer(&rootNode.data[0])))))

	return &Cursor{
		table:   table,
		pageNum: table.rootPageNum,
		cellNum: numCells,
		isEnd:   true,
	}
}

func advanceCursor(cursor *Cursor) *Cursor {

	pageNum := cursor.pageNum
	node, _ := getPage(cursor.table.pager, pageNum)
	nodeAddr := uintptr(unsafe.Pointer(&node.data[0]))

	cursor.cellNum += 1
	if cursor.cellNum >= *(*uint32)(unsafe.Pointer(leafNodeNumCell(nodeAddr))) {

		nextLeafNode := *(*uint32)(unsafe.Pointer(leafNodeNextLeaf(nodeAddr)))
		if nextLeafNode == 0 {
			cursor.isEnd = true
		} else {
			cursor.pageNum = nextLeafNode
			cursor.cellNum = 0
		}
	}
	return cursor

}

func leafNodeNumCell(nodeAddr uintptr) uintptr {

	return nodeAddr + LEAF_NODE_NUM_CELLS_OFFSET
}

func leafNodeCell(node uintptr, cellNum uint32) uintptr {
	return node + LEAF_NODE_HEADER_SIZE + (uintptr(cellNum) * LEAF_NODE_CELL_SIZE)
}

func leafNodeValue(node uintptr, cellNum uint32) uintptr {
	return leafNodeCell(node, cellNum) + LEAF_NODE_KEY_SIZE
}

func leafNodeNextLeaf(nodeAddr uintptr) uintptr {
	return nodeAddr + LEAF_NODE_NEXT_LEAF_OFFSET
}

func initalizeLeafNode(node uintptr) {

	setNodeType(node, Leaf)
	*(*uint32)(unsafe.Pointer(leafNodeNumCell(node))) = 0
	setNodeRoot(node, false)
	*(*uint8)(unsafe.Pointer(node + NODE_TYPE_OFFSET)) = Leaf
	*(*uint32)(unsafe.Pointer(leafNodeNextLeaf(node))) = 0

}

func initializeInternalNode(node uintptr) {

	setNodeType(node, Internal)
	*(*uint32)(unsafe.Pointer(leafNodeNumCell(node))) = 0
	setNodeRoot(node, false)
	*(*uint8)(unsafe.Pointer(node + NODE_TYPE_OFFSET)) = Internal
	*(*uint32)(unsafe.Pointer(internalNodeRightChild(node))) = INVALID_PAGE_NUM

}

func internalNodeNumKeys(nodeAddr uintptr) uintptr {
	return nodeAddr + INTERNAL_NODE_NUM_KEYS_OFFSET
}

func internalNodeRightChild(nodeAddr uintptr) uintptr {
	return nodeAddr + INTERNAL_NODE_RIGHT_CHILD_OFFSET
}

func internalNodeCell(nodeAddr uintptr, cellNum uint32) uintptr {
	return nodeAddr + INTERNAL_NODE_HEADER_SIZE + uintptr(cellNum)*INTERNAL_NODE_CELL_SIZE
}

func internalNodeChild(nodeAddr uintptr, childNum uint32) uintptr {
	numKeys := *(*uint32)(unsafe.Pointer(internalNodeNumKeys(nodeAddr)))
	if childNum > numKeys {
		fmt.Printf("Tried to access ChildNum %d > NumKeys %d\n", childNum, numKeys)
		os.Exit(-1)
		return nodeAddr
	} else if childNum == numKeys {

		//DOUBT
		rightChild := internalNodeRightChild(nodeAddr)

		if *(*uint32)(unsafe.Pointer(rightChild)) == (INVALID_PAGE_NUM) {
			fmt.Println("Tried to access right child of node, but was invalid page")
			os.Exit(-1)
		}
		return rightChild
	} else {
		child := internalNodeCell(nodeAddr, childNum)
		if *(*uint32)(unsafe.Pointer(child)) == (INVALID_PAGE_NUM) {
			fmt.Println("Tried to access child of node, but was invalid page")
			os.Exit(-1)
		}
		return child
	}
}

func internalNodeKey(nodeAddr uintptr, keyNum uint32) uintptr {
	return internalNodeCell(nodeAddr, keyNum) + INTERNAL_NODE_CHILD_SIZE
}

func updateInternalNode(nodeAddr uintptr, oldMaxValue, newMaxValue uint32) {
	oldChildIndex := fetchInternalNodeChildIndex(nodeAddr, oldMaxValue)
	*(*uint32)(unsafe.Pointer(internalNodeKey(nodeAddr, oldChildIndex))) = newMaxValue
}

func getNodeType(nodeAddr uintptr) uint8 {
	return *(*uint8)(unsafe.Pointer(nodeAddr + NODE_TYPE_OFFSET))
}

func parentNode(nodeAddr uintptr) uintptr {
	return nodeAddr + PARENT_POINTER_OFFSET
}

func insertLeafNode(cursor *Cursor, key uint32, data *Row) {

	node, _ := getPage(cursor.table.pager, cursor.pageNum)
	nodeAddr := uintptr(unsafe.Pointer(&node.data[0]))
	numCells := *(*uint32)(unsafe.Pointer(leafNodeNumCell(nodeAddr)))
	if numCells >= uint32(LEAF_NODE_MAX_CELLS) {
		splitNode(cursor, key, data)
		return
	}

	if cursor.cellNum < numCells {
		for i := numCells; i > cursor.cellNum; i-- {
			copy(unsafe.Slice((*byte)(unsafe.Pointer(leafNodeCell(nodeAddr, i))), LEAF_NODE_CELL_SIZE), unsafe.Slice((*byte)(unsafe.Pointer(leafNodeCell(nodeAddr, i-1))), LEAF_NODE_CELL_SIZE))
		}
	}

	*(*uint32)(unsafe.Pointer(leafNodeNumCell(nodeAddr))) = *(*uint32)(unsafe.Pointer(leafNodeNumCell(nodeAddr))) + 1
	*(*uint32)(unsafe.Pointer(leafNodeCell(nodeAddr, cursor.cellNum))) = key
	store(leafNodeValue(nodeAddr, cursor.cellNum), data)

}

func fetchNodeCursor(table *Table, key uint32) *Cursor {

	node, _ := getPage(table.pager, table.rootPageNum)
	nodeAddr := uintptr(unsafe.Pointer(&node.data[0]))
	if getNodeType(nodeAddr) == Leaf {
		return fetchLeafNode(table, table.rootPageNum, key)
	} else if getNodeType(nodeAddr) == Internal {
		return fetchInternalNode(table, table.rootPageNum, key)
	} else {
		os.Exit(-1)
		return fetchInternalNode(table, table.rootPageNum, key)

	}

}

func fetchLeafNode(table *Table, pageNum, key uint32) *Cursor {
	node, _ := getPage(table.pager, pageNum)
	nodeAddr := uintptr(unsafe.Pointer(&node.data[0]))
	numCells := *(*uint32)(unsafe.Pointer(leafNodeNumCell(nodeAddr)))
	minIndex := uint32(0)
	maxIndex := numCells
	for maxIndex != minIndex {
		index := (minIndex + maxIndex) / 2
		keyAtIndex := *(*uint32)(unsafe.Pointer(leafNodeCell(nodeAddr, index)))
		if keyAtIndex == key {
			return &Cursor{
				table:   table,
				pageNum: pageNum,
				cellNum: index,
				isEnd:   false,
			}
		}
		if key < keyAtIndex {
			maxIndex = index
		} else {
			minIndex = index + 1
		}
	}

	return &Cursor{
		table:   table,
		pageNum: pageNum,
		cellNum: minIndex,
		isEnd:   false,
	}
}

func fetchInternalNode(table *Table, pageNum, key uint32) *Cursor {
	node, _ := getPage(table.pager, pageNum)
	nodeAddr := uintptr(unsafe.Pointer(&node.data[0]))

	childIndex := fetchInternalNodeChildIndex(nodeAddr, key)
	childNum := *(*uint32)(unsafe.Pointer(internalNodeChild(nodeAddr, childIndex)))
	childNode, _ := getPage(table.pager, childNum)
	childNodeAddr := uintptr(unsafe.Pointer(&childNode.data[0]))

	if getNodeType(childNodeAddr) == Leaf {
		return fetchLeafNode(table, childNum, key)
	} else if getNodeType(childNodeAddr) == Internal {
		return fetchInternalNode(table, childNum, key)
	} else {
		os.Exit(-1)
		return fetchInternalNode(table, childNum, key)

	}

}

func fetchInternalNodeChildIndex(nodeAddr uintptr, key uint32) uint32 {

	numKeys := *(*uint32)(unsafe.Pointer(internalNodeNumKeys(nodeAddr)))
	minIndex := uint32(0)
	maxIndex := numKeys

	for minIndex != maxIndex {
		index := (minIndex + maxIndex) / 2
		maxKey := *(*uint32)(unsafe.Pointer(internalNodeKey(nodeAddr, index)))
		if maxKey >= key {
			maxIndex = index
		} else {
			minIndex = index + 1
		}
	}

	return minIndex

}

func internalNodeInsert(table *Table, parentPage, childPage uint32) {

	parentNode, _ := getPage(table.pager, parentPage)
	parentNodeAddr := uintptr(unsafe.Pointer(&parentNode.data[0]))
	childNode, _ := getPage(table.pager, childPage)
	childNodeAddr := uintptr(unsafe.Pointer(&childNode.data[0]))
	childMaxVal := *(*uint32)(unsafe.Pointer(getNodeMaxKey(table.pager, childNodeAddr)))
	index := fetchInternalNodeChildIndex(parentNodeAddr, childMaxVal)
	originalNumKeys := *(*uint32)(unsafe.Pointer(internalNodeNumKeys(parentNodeAddr)))

	if originalNumKeys >= INTERNAL_NODE_MAX_KEYS {
		internalNodeSplitAndInsert(table, parentPage, childPage)
		return
	}
	rightChildPageNum := *(*uint32)(unsafe.Pointer(internalNodeRightChild(parentNodeAddr)))
	if rightChildPageNum == INVALID_PAGE_NUM {
		*(*uint32)(unsafe.Pointer(internalNodeRightChild(parentNodeAddr))) = childPage
		return

	}
	rightChildNode, _ := getPage(table.pager, rightChildPageNum)
	rightChildNodeAddr := uintptr(unsafe.Pointer(&rightChildNode.data[0]))
	*(*uint32)(unsafe.Pointer(internalNodeNumKeys(parentNodeAddr))) = originalNumKeys + 1

	if childMaxVal > *(*uint32)(unsafe.Pointer(getNodeMaxKey(table.pager, rightChildNodeAddr))) {
		*(*uint32)(unsafe.Pointer(internalNodeChild(parentNodeAddr, originalNumKeys))) = rightChildPageNum

		*(*uint32)(unsafe.Pointer(internalNodeKey(parentNodeAddr, originalNumKeys))) = *(*uint32)(unsafe.Pointer(getNodeMaxKey(table.pager, rightChildNodeAddr)))
		*(*uint32)(unsafe.Pointer(internalNodeRightChild(parentNodeAddr))) = childPage

	} else {
		for i := originalNumKeys; i > index; i-- {
			copy(unsafe.Slice((*byte)(unsafe.Pointer(internalNodeCell(parentNodeAddr, i))), INTERNAL_NODE_CELL_SIZE), unsafe.Slice((*byte)(unsafe.Pointer(internalNodeCell(parentNodeAddr, i-1))), INTERNAL_NODE_CELL_SIZE))
		}
		*(*uint32)(unsafe.Pointer(internalNodeChild(parentNodeAddr, index))) = childPage
		*(*uint32)(unsafe.Pointer(internalNodeKey(parentNodeAddr, index))) = childMaxVal

	}

}

func internalNodeSplitAndInsert(table *Table, parentPageNum, childPageNum uint32) {
	oldPage := parentPageNum
	oldNode, _ := getPage(table.pager, oldPage)
	oldNodeAddr := uintptr(unsafe.Pointer(&oldNode.data[0]))
	oldMaxVal := *(*uint32)(unsafe.Pointer(getNodeMaxKey(table.pager, oldNodeAddr)))

	childNode, _ := getPage(table.pager, childPageNum)
	childNodeAddr := uintptr(unsafe.Pointer(&childNode.data[0]))
	childMaxVal := *(*uint32)(unsafe.Pointer(getNodeMaxKey(table.pager, childNodeAddr)))

	newPageNum := fetchUnusedPageNum(table.pager)
	isSplittingRoot := isNodeRoot(oldNodeAddr)

	var parent *MemoryBlock
	var newNode *MemoryBlock
	if isSplittingRoot {
		createNewRoot(table, newPageNum)
		parent, _ = getPage(table.pager, table.rootPageNum)
		parentAddr := uintptr(unsafe.Pointer(&parent.data[0]))

		oldPage = *(*uint32)(unsafe.Pointer(internalNodeChild(parentAddr, 0)))
		oldNode, _ = getPage(table.pager, oldPage)
		oldNodeAddr = uintptr(unsafe.Pointer(&oldNode.data[0]))
	} else {

		parent, _ = getPage(table.pager, *(*uint32)(unsafe.Pointer(parentNode(oldNodeAddr))))
		newNode, _ = getPage(table.pager, newPageNum)
		newNodeAddr := uintptr(unsafe.Pointer(&newNode.data[0]))

		initializeInternalNode(newNodeAddr)
	}

	oldNumKeys := internalNodeNumKeys(oldNodeAddr)
	currentPage := *(*uint32)(unsafe.Pointer(internalNodeRightChild(oldNodeAddr)))

	currNode, _ := getPage(table.pager, currentPage)
	currNodeAddr := uintptr(unsafe.Pointer(&currNode.data[0]))

	internalNodeInsert(table, newPageNum, currentPage)
	*(*uint32)(unsafe.Pointer(parentNode(currNodeAddr))) = newPageNum
	*(*uint32)(unsafe.Pointer(internalNodeRightChild(oldNodeAddr))) = INVALID_PAGE_NUM
	for i := INTERNAL_NODE_MAX_KEYS - 1; i > INTERNAL_NODE_MAX_KEYS/2; i-- {
		currentPage = *(*uint32)(unsafe.Pointer(internalNodeChild(oldNodeAddr, uint32(i))))

		currNode, _ = getPage(table.pager, currentPage)
		currNodeAddr = uintptr(unsafe.Pointer(&currNode.data[0]))
		*(*uint32)(unsafe.Pointer(parentNode(currNodeAddr))) = newPageNum
		*(*uint32)(unsafe.Pointer(oldNumKeys)) = *(*uint32)(unsafe.Pointer(oldNumKeys)) - 1
	}
	*(*uint32)(unsafe.Pointer(internalNodeRightChild(oldNodeAddr))) = *(*uint32)(unsafe.Pointer(internalNodeChild(oldNodeAddr, *(*uint32)(unsafe.Pointer(oldNumKeys))-1)))
	*(*uint32)(unsafe.Pointer(oldNumKeys)) = *(*uint32)(unsafe.Pointer(oldNumKeys)) - 1

	maxAfterSplit := *(*uint32)(unsafe.Pointer(getNodeMaxKey(table.pager, oldNodeAddr)))
	var destinationPageNum uint32

	if childMaxVal < maxAfterSplit {
		destinationPageNum = oldPage
	} else {
		destinationPageNum = newPageNum
	}
	internalNodeInsert(table, destinationPageNum, childPageNum)
	*(*uint32)(unsafe.Pointer(parentNode(childNodeAddr))) = destinationPageNum

	parentNodeAddr := uintptr(unsafe.Pointer(&parent.data[0]))

	updateInternalNode(parentNodeAddr, oldMaxVal, *(*uint32)(unsafe.Pointer(getNodeMaxKey(table.pager, oldNodeAddr))))

	if !isSplittingRoot {
		internalNodeInsert(table, *(*uint32)(unsafe.Pointer(parentNode(oldNodeAddr))), newPageNum)
		newNodeAddr := uintptr(unsafe.Pointer(&newNode.data[0]))

		*(*uint32)(unsafe.Pointer(parentNode(newNodeAddr))) = *(*uint32)(unsafe.Pointer(parentNode(oldNodeAddr)))
	}
	return
}

func splitNode(cursor *Cursor, key uint32, data *Row) {
	oldNode, _ := getPage(cursor.table.pager, cursor.pageNum)
	oldNodeAddr := uintptr(unsafe.Pointer(&oldNode.data[0]))
	oldMaxVal := *(*uint32)(unsafe.Pointer(getNodeMaxKey(cursor.table.pager, oldNodeAddr)))
	newPageNum := fetchUnusedPageNum(cursor.table.pager)

	newNode, _ := getPage(cursor.table.pager, newPageNum)
	newNodeAddr := uintptr(unsafe.Pointer(&newNode.data[0]))
	initalizeLeafNode(newNodeAddr)

	*(*uint32)(unsafe.Pointer(parentNode(newNodeAddr))) = *(*uint32)(unsafe.Pointer(parentNode(oldNodeAddr)))
	*(*uint32)(unsafe.Pointer(leafNodeNextLeaf(newNodeAddr))) = *(*uint32)(unsafe.Pointer(leafNodeNextLeaf(oldNodeAddr)))
	*(*uint32)(unsafe.Pointer(leafNodeNextLeaf(oldNodeAddr))) = newPageNum

	var keyDestinationNode *MemoryBlock
	var keyDestinationIndex uint32

	for i := int(LEAF_NODE_MAX_CELLS); i >= 0; i-- {
		var destinationNode *MemoryBlock
		var indexInNode uint32

		if uint32(i) >= LEAF_NODE_LEFT_SPLIT_COUNT {
			destinationNode = newNode
			indexInNode = uint32(i) - LEAF_NODE_LEFT_SPLIT_COUNT
		} else {
			destinationNode = oldNode
			indexInNode = uint32(i)
		}

		destinationNodeAddr := uintptr(unsafe.Pointer(&destinationNode.data[0]))

		if uint32(i) == cursor.cellNum {
			keyDestinationNode = destinationNode
			keyDestinationIndex = indexInNode
		} else if uint32(i) > cursor.cellNum {
			if i > 0 { 
				source := leafNodeCell(oldNodeAddr, uint32(i-1))
				dest := leafNodeCell(destinationNodeAddr, indexInNode)
				copy(unsafe.Slice((*byte)(unsafe.Pointer(dest)), LEAF_NODE_CELL_SIZE),
					unsafe.Slice((*byte)(unsafe.Pointer(source)), LEAF_NODE_CELL_SIZE))
			}
		} else {
			source := leafNodeCell(oldNodeAddr, uint32(i))
			dest := leafNodeCell(destinationNodeAddr, indexInNode)
			copy(unsafe.Slice((*byte)(unsafe.Pointer(dest)), LEAF_NODE_CELL_SIZE),
				unsafe.Slice((*byte)(unsafe.Pointer(source)), LEAF_NODE_CELL_SIZE))
		}
	}

	if keyDestinationNode != nil {
		destinationNodeAddr := uintptr(unsafe.Pointer(&keyDestinationNode.data[0]))
		destination := leafNodeCell(destinationNodeAddr, keyDestinationIndex)
		*(*uint32)(unsafe.Pointer(destination)) = key
		store(leafNodeValue(destinationNodeAddr, keyDestinationIndex), data)
	}

	*(*uint32)(unsafe.Pointer(leafNodeNumCell(oldNodeAddr))) = LEAF_NODE_LEFT_SPLIT_COUNT
	*(*uint32)(unsafe.Pointer(leafNodeNumCell(newNodeAddr))) = LEAF_NODE_RIGHT_SPLIT_COUNT

	if isNodeRoot(oldNodeAddr) {
		createNewRoot(cursor.table, newPageNum)
		return
	} else {
		parentPage := *(*uint32)(unsafe.Pointer(parentNode(oldNodeAddr)))
		newMaxVal := *(*uint32)(unsafe.Pointer(getNodeMaxKey(cursor.table.pager, oldNodeAddr)))
		parentNode, _ := getPage(cursor.table.pager, parentPage)
		parentNodeAddr := uintptr(unsafe.Pointer(&parentNode.data[0]))

		updateInternalNode(parentNodeAddr, oldMaxVal, newMaxVal)
		internalNodeInsert(cursor.table, parentPage, newPageNum)
		return

	}
}

func isNodeRoot(nodeAddr uintptr) bool {

	return *(*bool)(unsafe.Pointer(nodeAddr + IS_ROOT_OFFSET))

}

func createNewRoot(table *Table, pageNum uint32) {
	root, _ := getPage(table.pager, table.rootPageNum)
	rightNode, _ := getPage(table.pager, pageNum)
	leftPageNum := fetchUnusedPageNum(table.pager)
	leftNode, _ := getPage(table.pager, leftPageNum)
	rootNodeAddr := uintptr(unsafe.Pointer(&root.data[0]))
	rightNodeAddr := uintptr(unsafe.Pointer(&rightNode.data[0]))
	leftNodeAddr := uintptr(unsafe.Pointer(&leftNode.data[0]))

	if getNodeType(rootNodeAddr) == Internal {
		initializeInternalNode(rightNodeAddr)
		initializeInternalNode(leftNodeAddr)
	}
	copy(unsafe.Slice((*byte)(unsafe.Pointer(leftNodeAddr)), PAGE_SIZE), unsafe.Slice((*byte)(unsafe.Pointer(rootNodeAddr)), PAGE_SIZE))
	setNodeRoot(leftNodeAddr, false)

	if getNodeType(leftNodeAddr) == Internal {
		var child *MemoryBlock
		numKeys := *(*uint32)(unsafe.Pointer(internalNodeNumKeys(leftNodeAddr)))
		for i := 0; i < int(numKeys); i++ {
			child, _ = getPage(table.pager, *(*uint32)(unsafe.Pointer(internalNodeChild(leftNodeAddr, uint32(i)))))
			childNodeAddr := uintptr(unsafe.Pointer(&child.data[0]))

			*(*uint32)(unsafe.Pointer(parentNode(childNodeAddr))) = leftPageNum
		}
	}
	initializeInternalNode(rootNodeAddr)
	setNodeRoot(rootNodeAddr, true)
	*(*uint32)(unsafe.Pointer(internalNodeNumKeys(rootNodeAddr))) = 1
	*(*uint32)(unsafe.Pointer(internalNodeChild(rootNodeAddr, 0))) = leftPageNum
	leftChildMaxKey := *(*uint32)(unsafe.Pointer(getNodeMaxKey(table.pager, leftNodeAddr)))
	*(*uint32)(unsafe.Pointer(internalNodeKey(rootNodeAddr, 0))) = leftChildMaxKey
	*(*uint32)(unsafe.Pointer(internalNodeRightChild(rootNodeAddr))) = pageNum
	*(*uint32)(unsafe.Pointer(parentNode(leftNodeAddr))) = table.rootPageNum
	*(*uint32)(unsafe.Pointer(parentNode(rightNodeAddr))) = table.rootPageNum

}

func fetchUnusedPageNum(pager *Pager) uint32 {
	return pager.numPages
}

func setNodeRoot(nodeAddr uintptr, isRoot bool) {
	if isRoot {
		*(*uint8)(unsafe.Pointer(nodeAddr + IS_ROOT_OFFSET)) = 1
	} else {
		*(*uint8)(unsafe.Pointer(nodeAddr + IS_ROOT_OFFSET)) = 0
	}
}

func getNodeMaxKey(pager *Pager, nodeAddr uintptr) uintptr {
	if getNodeType(nodeAddr) == Leaf {
		return leafNodeCell(nodeAddr, *(*uint32)(unsafe.Pointer(leafNodeNumCell(nodeAddr)))-1)
		// case Internal:
		// 	return internalNodeKey(nodeAddr, uint32(leafNodeNumCell(nodeAddr))-1)
		// }
		// return nodeAddr
	}

	rightNode, _ := getPage(pager, *(*uint32)(unsafe.Pointer(internalNodeRightChild(nodeAddr))))
	rightNodeAddr := uintptr(unsafe.Pointer(&rightNode.data[0]))

	return getNodeMaxKey(pager, rightNodeAddr)

}

func setNodeType(nodeAddr uintptr, nodeType int) {

	if nodeType == Leaf {
		*(*uint8)(unsafe.Pointer(nodeAddr + NODE_TYPE_OFFSET)) = Leaf
	} else if nodeType == Internal {
		*(*uint8)(unsafe.Pointer(nodeAddr + NODE_TYPE_OFFSET)) = Internal

	}
}
