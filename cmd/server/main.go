// package main

// // ITERATION 3:

// //	Need to save data entered in a file
// //  A single file which will contain all the pages(array of bytes)
// //  when exiting program, all the data is written to this file
// //  when server is started the whole data is again loaded from this file

// // ITERATION 4:

// // Need to create cursor(s)
// // they'll be used to point to different rows and perform action based on it
// // Initially two cursors will be created, one which will point to the start of the table and
// // one will point to the last data of the table
// // First will be used for select statements while the second will be used for insert statements

// // **************************TODO***********************

// // TODO: need to optimize struct declaration because of padding

// // TODO: Make sure the address provided in rowAddress is divisible by 4,as we're going to start
// //  	 saving id and then rest of the columns

package main

import (
	"fmt"
	"os"

	"github.com/unamdev0/ZiDB/internal/btree"
	"github.com/unamdev0/ZiDB/internal/server"
)

const defaultPort = ":6001"

func main() {
	table, err := btree.OpenDB("db.txt")
	if err != nil {
		fmt.Println("Error while opening file:", err)
		os.Exit(-1)
	}

	dbServer := server.NewServer(defaultPort, table)
	if err := dbServer.Start(); err != nil {
		fmt.Printf("Server error: %v\n", err)
		os.Exit(1)
	}
}
