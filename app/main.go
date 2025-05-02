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

	// Getting the correlation ID:

	//Since we know the message size is 4 bytes long (i.e. 32 bits . . .) and the
	// correlationID is part of the header, where it is the 32nd through 64th bits, then we know that bits 64-86 are the necessary bits
	message_size = 0

	buffer := make([]byte, 10) // We need to take in 12 bytes
	_, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Failed to read message: ", err)
		os.Exit(1)
	}

	// Now we need to parse so that of the 12 bytes we have, we take the 9th and 10th byte
	correlation_id = int32(binary.BigEndian.Uint32(buffer[8:10]))
	fmt.Println(correlation_id)

	//Now we want to send the binary values
	err = binary.Write(conn, binary.BigEndian, message_size)
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
