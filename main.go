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
	"net"
	"os"
	"strconv"
	"strings"
	"unsafe"
)

const defaultPort = ":6001"

type StatementType int
type NodeType int

var (
	ErrUnrecognizedStatement = errors.New("unrecognized statement")
	ErrUnrecognizedCommand   = errors.New("unrecognized command")
	ErrSyntaxError           = errors.New("syntax error")
	ErrOutOfBoundPageNum     = errors.New("page number entered is greater than total DB capacity")
	ErrReadingFile           = errors.New("can't read db file")
	ErrNoContentFound        = errors.New("page does not exist")
	ErrDataNotSaved          = errors.New("could not save data to DB")
)

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
	// starting with 1 as 0 will be put as default value
	Select StatementType = 1
	Insert StatementType = 2
)

const (
	// Node type 0 denotes leaf node 1 denotes internal node
	Leaf     NodeType = 1
	Internal NodeType = 1
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
	Type        StatementType
	Input       string
	RowToInsert Row
	// InsertData
}

type Table struct {
	rowsInserted uint32
	pager        *Pager
}

type Pager struct {
	file     *os.File
	fileSize uint32
	pages    [MAX_PAGE_NUM]*MemoryBlock
}

type Cursor struct {
	table  *Table
	rowNum uint32
	isEnd  bool
}

func cursorValue(cursor *Cursor) uintptr {

	pageNum := cursor.rowNum / ROWS_PER_PAGE
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
	rowOffset := cursor.rowNum % ROWS_PER_PAGE
	byteOffset := rowOffset * uint32(ROW_SIZE)

	return uintptr(unsafe.Pointer(&page.data[0])) + uintptr(byteOffset)
}

func store(to uintptr, data *Row) {
	*(*uint32)(unsafe.Pointer(to)) = data.id

	// Since copy func in golang works only on slice, we'll have to convert email and username of row struct to slice

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

	fmt.Printf("The file is %d bytes long", fi.Size())

	// Return the file wrapped in a FileHandler struct
	pager := &Pager{file: file, fileSize: uint32(fi.Size())}
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

	cursor := endingCursor(table)
	ptr := cursorValue(cursor)

	store(ptr, &statement.RowToInsert)
	table.rowsInserted += 1
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
	}
	return pager.pages[pageNum], nil
}

func openDB(filename string) (*Table, error) {
	pager, err := OpenOrCreateFile(filename)

	if err != nil {
		return nil, err
	}
	var numRows uint32 = pager.fileSize / uint32(ROW_SIZE)
	table := &Table{rowsInserted: numRows, pager: pager}

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
	return &Cursor{
		table:  table,
		rowNum: 0,
		isEnd:  table.rowsInserted == 0,
	}
}

func endingCursor(table *Table) *Cursor {
	return &Cursor{
		table:  table,
		rowNum: table.rowsInserted,
		isEnd:  true,
	}
}

func advanceCursor(cursor *Cursor) *Cursor {
	cursor.rowNum += 1
	if cursor.rowNum == cursor.table.rowsInserted {
		cursor.isEnd = true
	}
	return cursor

}
