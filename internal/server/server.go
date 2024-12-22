package server

import (
	"fmt"
	"net"

	"github.com/unamdev0/ZiDB/internal/btree"
)

type DBServer struct {
	listener net.Listener
	table    *btree.Table
	port     string
}

// NewServer creates and returns a new database server instance
func NewServer(port string, table *btree.Table) *DBServer {
	return &DBServer{
		port:  port,
		table: table,
	}
}

// Start initializes and starts the database server
func (s *DBServer) Start() error {
	listener, err := net.Listen("tcp", s.port)
	if err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}
	s.listener = listener

	fmt.Printf("TCP server listening on port %s\n", s.port)

	return s.acceptConnections()
}

// acceptConnections accepts incoming client connections
func (s *DBServer) acceptConnections() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return fmt.Errorf("error accepting connection: %w", err)
		}
		fmt.Println("Client connected")

		// Handle each client connection in a separate goroutine
		go HandleConnection(conn, s.table)
	}
}

// Stop gracefully shuts down the server
func (s *DBServer) Stop() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}
