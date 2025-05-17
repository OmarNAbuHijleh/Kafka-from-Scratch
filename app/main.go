package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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
	for {
		// 3 parts to our response
		// var message_size int32
		var correlation_id int32 // This is part of the response header
		var request_api_version uint16
		var request_api_key uint16
		var received_message_size uint32 // this is the size of the request in bytes

		// Getting the message size:
		received_message_buffer := make([]byte, 4)
		_, err := io.ReadFull(conn, received_message_buffer)
		if err != nil {
			fmt.Println("Failed to receive message input size")
			return
		}

		// Now we create the next buffer based on the rest of the message's size
		received_message_size = binary.BigEndian.Uint32(received_message_buffer)
		buffer := make([]byte, received_message_size)
		_, err = io.ReadFull(conn, buffer) //conn.Read(buffer)
		if err != nil {
			fmt.Println("Failed to read message: ", err)
			return
		}

		// message_size = int32(binary.BigEndian.Uint32(buffer[0:4])) // A number representing the size of the rest of the message
		// Now we need to parse so that of the 12 bytes we have, we take the 8th through 11th bytes
		request_api_key = uint16(binary.BigEndian.Uint16(buffer[0:2]))     // 2 bytes
		request_api_version = uint16(binary.BigEndian.Uint16(buffer[2:4])) // 2 bytes
		correlation_id = int32(binary.BigEndian.Uint32(buffer[4:8]))       // 4 bytes

		// Response portion of the code ---------------------------------------------------------------------------------------------------------------------------------
		var response bytes.Buffer //This is where we'll store all the elements of the response. It's a buffer prior to writing out the response!

		binary.Write(&response, binary.BigEndian, correlation_id) // 4 bytes

		//The next part depends on API versioning
		if request_api_key == 18 {
			//API Versioning
			api_error_code := valid_version(request_api_key, request_api_version)
			binary.Write(&response, binary.BigEndian, api_error_code) // 2 bytes
			eighteen_response_block(&response, request_api_key)
		} else if request_api_key == 75 {
			// Let's get the rest of the message we've received

			//client ID
			client_id_length := binary.BigEndian.Uint16(buffer[8:10])
			next_starting_point := 10 + client_id_length
			client_id_contents := buffer[10:next_starting_point]
			tag_buffered := binary.BigEndian.Uint16(buffer[next_starting_point : next_starting_point+1])
			next_starting_point += 1

			seventy_five_response_block(&response, client_id_contents)
		}

		// Now that we have the buffer we can do the following
		payload_bytes := response.Bytes()
		response_message_size_bytes := uint32(len(payload_bytes))
		binary.Write(conn, binary.BigEndian, response_message_size_bytes)
		conn.Write(payload_bytes)
	}
}

func eighteen_response_block(bytesBuffer *bytes.Buffer, request_api_key uint16) {
	binary.Write(bytesBuffer, binary.BigEndian, uint8(3+1)) // Default is 4? - 1 byte

	// API Version 1 info:
	binary.Write(bytesBuffer, binary.BigEndian, request_api_key) // 2 bytes
	binary.Write(bytesBuffer, binary.BigEndian, uint16(3))       //min version - 2 bytes
	binary.Write(bytesBuffer, binary.BigEndian, uint16(4))       //max version - 2 bytes
	binary.Write(bytesBuffer, binary.BigEndian, uint8(0))        // Tag buffer - 1 byte

	// API Version 2 info:
	binary.Write(bytesBuffer, binary.BigEndian, request_api_key) // 2 bytes
	binary.Write(bytesBuffer, binary.BigEndian, uint16(3))       //min version - 2 bytes
	binary.Write(bytesBuffer, binary.BigEndian, uint16(4))       //max version - 2 bytes
	binary.Write(bytesBuffer, binary.BigEndian, uint8(0))        // Tag buffer - 1 byte

	// API Version 3 info:
	binary.Write(bytesBuffer, binary.BigEndian, uint16(75)) // 2 bytes
	binary.Write(bytesBuffer, binary.BigEndian, uint16(0))  //min version - 2 bytes
	binary.Write(bytesBuffer, binary.BigEndian, uint16(0))  //max version - 2 bytes
	binary.Write(bytesBuffer, binary.BigEndian, uint8(0))   // Tag buffer - 1 byte

	binary.Write(bytesBuffer, binary.BigEndian, uint32(0)) // throttle time - 4 bytes
	binary.Write(bytesBuffer, binary.BigEndian, uint8(0))  // Tag buffer - 1 byte
}

// This is the response for
func seventy_five_response_block(bytesBuffer *bytes.Buffer, client_id_contents []byte) {
	binary.Write(bytesBuffer, binary.BigEndian, uint8(0))  // Tag buffer (1 bytes)
	binary.Write(bytesBuffer, binary.BigEndian, uint32(0)) //throttle time
	binary.Write(bytesBuffer, binary.BigEndian, uint8(2))  //Array length
	binary.Write(bytesBuffer, binary.BigEndian, uint16(3))
	binary.Write(bytesBuffer, binary.BigEndian, uint8(4)) //Array length
	binary.Write(bytesBuffer, binary.BigEndian, client_id_contents)
	// binary.Write(bytesBuffer, binary.BigEndian, []byte("foo"))
	binary.Write(bytesBuffer, binary.BigEndian, make([]byte, 16))
	binary.Write(bytesBuffer, binary.BigEndian, uint8(0))
	binary.Write(bytesBuffer, binary.BigEndian, uint8(1))

	// topic authorized operations
	binary.Write(bytesBuffer, binary.BigEndian, uint8(0))
	binary.Write(bytesBuffer, binary.BigEndian, uint8(13))
	binary.Write(bytesBuffer, binary.BigEndian, uint16(248))

	binary.Write(bytesBuffer, binary.BigEndian, uint8(0))
	binary.Write(bytesBuffer, binary.BigEndian, uint8(255))
	binary.Write(bytesBuffer, binary.BigEndian, uint8(0))
}

// Check that we're dealing with API version that is valid for the message we're receiving
func valid_version(api_key uint16, api_version uint16) uint16 {
	big_boolean := ((api_version > 4) && (api_key == 18)) || ((api_version != 0) && (api_key == 75))
	if big_boolean {
		return 35
	}
	return 0
}
