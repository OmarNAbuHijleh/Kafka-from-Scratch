package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// // Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
// var _ = net.Listen
// var _ = os.Exit

func main() {
	// First create a server that can connect on port 9092
	fmt.Println("Creating listener . . . ")
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close()

	fmt.Println("Listening on port 9092 . . .")

	// Accept the incoming connection
	for {
		fmt.Println("Accepting an oncoming connection . . .")
		netConnection, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleConnection(netConnection)
	}
}

// This function takes in a net.Conn struct and obtains the header value from it. Following this, it will return a response
func handleConnection(conn net.Conn) {
	// 3 parts to our response
	var message_size int32
	var correlation_id int32 // This is part of the response header

	/*
		Steps:
		1.) The client generates a correlation_id
		2.) The client sends a request that includes the correlation_id
		3.) The server sends a response that includes the same correlation_id
		4.) The client receives the response and matches the received correlation_id to the original request
	*/

	// For now, our response will be a hard-coded correlation_id of 7
	message_size = 0
	correlation_id = 7

	// err = binary.Read(netConnection, binary.BigEndian, &correlation_id)
	// if err != nil {
	// 	fmt.Println("Binary Reading Error: ", err.Error())
	// 	os.Exit(1)
	// }

	//Now we want to send the binary values
	err := binary.Write(conn, binary.BigEndian, message_size)
	if err != nil {
		fmt.Println("Failed to write the message size with error: ", err)
		os.Exit(1)
	}

	err = binary.Write(conn, binary.BigEndian, correlation_id)
	if err != nil {
		fmt.Println("Failed to write the correlation ID with error: ", err)
		os.Exit(1)
	}
}
