package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
)

const defaultPort = ":6001"

type StatementType int

var (
	ErrUnrecognizedStatement = errors.New("unrecognized statement")
	ErrUnrecognizedCommand   = errors.New("unrecognized command")
)

const (
	// starting with 1 as 0 will be put as default value
	Select StatementType = 1
	Insert StatementType = 2
)

type Statement struct {
	StatementType StatementType
}

func main() {
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
		go handleConnection(conn)
	}

}

func handleConnection(conn net.Conn) {
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
		response := executeCommand(statement)
		_, err = conn.Write([]byte(response))
		if err != nil {
			fmt.Println("Error writing to client:", err)
			break
		}
	}
}

func prepareCommand(input string) (*Statement, error) {
	var statement Statement
	if strings.HasPrefix(input, "select") {
		statement.StatementType = Select
	} else if strings.HasPrefix(input, "insert") {
		statement.StatementType = Insert
	} else {
		return &statement, ErrUnrecognizedStatement
	}

	return &statement, nil
}

func executeCommand(statement *Statement) string {
	if statement.StatementType == Select {
		return "Executing select command"
	} else if statement.StatementType == Insert {
		return "Executing insert Command"
	}
	return ErrUnrecognizedCommand.Error()
}
