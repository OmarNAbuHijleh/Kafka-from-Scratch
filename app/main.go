package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

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
	// var message_size int32
	var correlation_id int32 // This is part of the response header
	var request_api_version int16
	var request_api_key int16

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
	buffer := make([]byte, 12) // We need to take in 12 bytes
	_, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Failed to read message: ", err)
		os.Exit(1)
	}

	// message_size is first 4 bytes
	// message_size = int32(binary.BigEndian.Uint32(buffer[0:4])) // A number representing the size of the rest of the message
	// Now we need to parse so that of the 12 bytes we have, we take the 8th through 11th bytes
	correlation_id = int32(binary.BigEndian.Uint32(buffer[8:12]))     // 4 bytes
	request_api_version = int16(binary.BigEndian.Uint16(buffer[6:8])) // 2 bytes
	request_api_key = int16(binary.BigEndian.Uint16(buffer[4:6]))     // 2 bytes

	var response bytes.Buffer //This is where we'll store all the elements of the response. It's a buffer prior to writing out the response!

	//Now we want to send the binary values
	// binary.Write(conn, binary.BigEndian, message_size)   // 4 bytes in length. The value we write may be subject to change

	binary.Write(&response, binary.BigEndian, correlation_id) // 4 bytes

	//API Versioning
	api_error_code := valid_version(request_api_version)
	binary.Write(&response, binary.BigEndian, api_error_code) // 2 bytes

	// we need to write the following
	// 1.) Array Length as a varint. It's the length of the API Versions array + 1
	binary.Write(&response, binary.BigEndian, uint8(3+1)) // Default is 4? - 1 byte

	// API Version 1 info:
	binary.Write(&response, binary.BigEndian, request_api_key) // 2 bytes
	binary.Write(&response, binary.BigEndian, uint16(3))       //min version - 2 bytes
	binary.Write(&response, binary.BigEndian, uint16(4))       //max version - 2 bytes
	binary.Write(&response, binary.BigEndian, uint8(0))        // Tag buffer - 1 byte

	// API Version 2 info:
	binary.Write(&response, binary.BigEndian, request_api_key) // 2 bytes
	binary.Write(&response, binary.BigEndian, uint16(3))       //min version - 2 bytes
	binary.Write(&response, binary.BigEndian, uint16(4))       //max version - 2 bytes
	binary.Write(&response, binary.BigEndian, uint8(0))        // Tag buffer - 1 byte

	// API Version 3 info:
	binary.Write(&response, binary.BigEndian, request_api_key) // 2 bytes
	binary.Write(&response, binary.BigEndian, uint16(3))       //min version - 2 bytes
	binary.Write(&response, binary.BigEndian, uint16(4))       //max version - 2 bytes
	binary.Write(&response, binary.BigEndian, uint8(0))        // Tag buffer - 1 byte

	binary.Write(&response, binary.BigEndian, uint32(0)) // throttle time - 4 bytes
	binary.Write(&response, binary.BigEndian, uint8(0))  // Tag buffer - 1 byte

	// Now that we have the buffer we can do the following
	payload_bytes := response.Bytes()
	response_message_size_bytes := uint32(len(payload_bytes))
	binary.Write(conn, binary.BigEndian, response_message_size_bytes)
	conn.Write(payload_bytes)
}

// Check that we're dealing with API version 4 or above!
func valid_version(api_version int16) int16 {
	if api_version > 4 {
		return 35
	}
	return 0
}
