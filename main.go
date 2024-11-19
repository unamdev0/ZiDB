package main

// TODO: need to optimize struct declaration because of padding

// TODO: Make sure the address provided in rowAddress is divisible by 4,as we're going to start
//  	 saving id and then rest of the columns 	


import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"unsafe"
)

const defaultPort = ":6001"

type StatementType int

var (
	ErrUnrecognizedStatement = errors.New("unrecognized statement")
	ErrUnrecognizedCommand   = errors.New("unrecognized command")
	ErrSyntaxError           = errors.New("syntax error")
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
	pages        [MAX_PAGE_NUM]*MemoryBlock
}

func rowAddress(rowNumber uint32, table *Table) uintptr {

	pageNum := rowNumber / ROWS_PER_PAGE
	page := table.pages[pageNum]
	if page == nil {
		// Allocate memory only when we try to access the page
		page = &MemoryBlock{}
		table.pages[pageNum] = page
	}
	rowOffset := rowNumber % ROWS_PER_PAGE
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

func main() {
	table := &Table{rowsInserted: 0}
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

	ptr := rowAddress(table.rowsInserted, table)

	store(ptr, &statement.RowToInsert)
	table.rowsInserted += 1
	return "Executing insert Command"
}

func selectCommand(table *Table) {
	var i uint32 = 0
	for i < (table.rowsInserted) {
		var row Row
		ptr := rowAddress(i, table)

		read(&row, ptr)
		fmt.Print(printRow(&row))
		i++
	}

}

func printRow(row *Row) string {
	return fmt.Sprintf("ROW ID->%d, USERNAME->%s, EMAIL->%s\n", row.id, row.username, row.email)
}
