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
			var start_idx uint16

			//client ID - 2 bytes for the length parameter, followed by a variable number of bites for what the length equals
			client_id_length := uint16(binary.BigEndian.Uint16(buffer[8:10]))

			start_idx = 10
			end_idx := start_idx + client_id_length
			// client_id_contents := string(buffer[start_idx:end_idx])

			// For the tag buffer (1 byte)
			start_idx = end_idx + 1

			// topics array
			end_idx = start_idx + 1
			array_length := uint8(buffer[start_idx]) // 1 byte
			start_idx++

			// Iterate through the top
			topic_names := make([]string, array_length-1)

			var i uint8 = 0
			for i < array_length-1 {
				name_length := uint16(buffer[start_idx])
				start_idx++
				topic_names[i] = string(buffer[start_idx : start_idx+name_length])
				start_idx += name_length + 1 // Add an extra 1 because of the tag buffer
				i++
			}
			// Response partition limit
			end_idx = start_idx + 4
			// response_partition_limit := uint32(binary.BigEndian.Uint32(buffer[start_idx:end_idx]))

			start_idx = end_idx
			// cursor := uint8(buffer[start_idx])

			seventy_five_response_block(&response, topic_names)
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
func seventy_five_response_block(bytesBuffer *bytes.Buffer, topic_names []string) {
	binary.Write(bytesBuffer, binary.BigEndian, uint8(0))                  // Tag buffer (1 bytes)
	binary.Write(bytesBuffer, binary.BigEndian, uint32(0))                 //throttle time
	binary.Write(bytesBuffer, binary.BigEndian, uint8(len(topic_names)+1)) //Array length

	for _, topic := range topic_names {
		binary.Write(bytesBuffer, binary.BigEndian, uint16(3))
		binary.Write(bytesBuffer, binary.BigEndian, uint8(len(topic))) //Array length
		binary.Write(bytesBuffer, binary.BigEndian, []byte(topic))
		binary.Write(bytesBuffer, binary.BigEndian, make([]byte, 16)) //topic id
		binary.Write(bytesBuffer, binary.BigEndian, uint8(0))         // is internal

		// next is the partitions array
		binary.Write(bytesBuffer, binary.BigEndian, uint8(1))
		// Authorized Operations
		binary.Write(bytesBuffer, binary.BigEndian, uint8(0))
		binary.Write(bytesBuffer, binary.BigEndian, uint8(13))
		binary.Write(bytesBuffer, binary.BigEndian, uint16(248))
		//Tag buffer
		binary.Write(bytesBuffer, binary.BigEndian, uint8(0))
	}

	// topic authorized operations
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
