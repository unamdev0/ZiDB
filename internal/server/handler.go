package server

import (
	"bufio"
	"fmt"
	"net"

	"github.com/unamdev0/ZiDB/internal/btree"
	"github.com/unamdev0/ZiDB/internal/sql"
)

func HandleConnection(conn net.Conn, table *btree.Table) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Client disconnected")
			break
		}

		if message == "exit\n" {
			btree.CloseDB(table)
			fmt.Println("Client requested to close the connection")
			break
		}

		statement, err := sql.PrepareCommand(message)
		if err != nil {
			fmt.Printf("Error: %s\n", err)
		}

		response := sql.ExecuteCommand(statement, table)
		conn.Write([]byte(response))
	}
}
