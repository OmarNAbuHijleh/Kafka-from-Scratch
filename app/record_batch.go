package main

import "encoding/binary"

// import (
// 	"bytes"
// 	"encoding/binary"
// )

// A value struct can be one of 3 types - feature record, partition record, and topic record
type Value struct {
	frameVersion      byte  // 1 byte big endian integer indicating the version of the format of the record
	recordType        int8  // 1 byte big endian integer explaining the type of the record - 03 means partition record, 02 is a topic record, 12 in decimal is a feature level record
	version           byte  // 1 byte big endian integer indicating the version feature/topic/partition level record
	taggedFieldsCount int64 //unsigned var size integer indicating the number of tagged fields

	//Feature level
	feature_level int64 // 2 byte big endian integer indicating the level of the feature level record

	// Topic level and Partition level
	uuid []byte //16 byte uuid

	// Feature level and Topic level
	nameLength int64  // unsigned variable size integer indicating the length of the name as a compact string (so it's actually length of the name + 1)
	name       string // the name in accordance with name length. this is a string. Can be the topic level name, or feature level name

	// Partition Level
	partitionID                 []byte   //4 byte ID of the partition
	replicaArrayLength          int64    //varint length of replica array. Compact Int and replicaArray length is actually this value - 1
	replicaArray                []uint32 // 4 byte big endian integers containing replicaID of the replicas
	lengthInSyncReplicaArray    int64    //varint length of replica sync array. Compact Int and replicaSyncArray length is actually this value - 1
	inSyncReplicaArray          []uint32 // 4 byte big endian integers containing replicaID of in sync replicas
	lengthRemovingReplicasArray int64    //varint length of removing replicas array. Compact Int and replicaSyncArray length is actually this value - 1
	removingReplicasArray       []uint32
	lengthAddingReplicasArray   int64 //varint length of adding replicas array. Compact Int and replicaSyncArray length is actually this value - 1
	addingReplicasArray         []uint32
	leader                      []byte   // 4 byte big endian integer indicating replicaID of the leader
	leaderEpoch                 []byte   // 4 byte big endian integer indicating the epoch of the leader
	partitionEpoch              []byte   // 4 byte big endian integer indicating the epoch of the partition
	lengthOfDirectoriesArr      int64    // unsigned var size integer indicating # directories in array. length is actually this value - 1 since compact
	directoriesArray            [][]byte //16 byte raw byte arrays
}

type Record struct {
	length            int64  // signed variable size integer indicating the length of the record from the attributes field to the end of the record
	attributes        byte   // indicates the attributes of the record
	timeStampDelta    int64  // signed variable integer indicating the difference between the timestamp of this record and the timestamp of the record batch
	offsetDelta       int64  // signed variable size integer indicating the difference between the offset of the record and the base offset of the record batch
	keyLength         int64  // signed variable size integer indicating the length of the key of the record
	key               []byte // byte array indicating the key of the record. Empty is null
	valueLength       int64  // signed variable size integer indicating the length of the value of this record
	valueStruct       Value  // This is the partition record value
	headersArrayCount int64  // unsigned variable size integer indicating the number of headers present
}

type RecordBatch struct {
	recordBatch          []byte   // 8 bytes -- base offset - indicates the offset of the first record in this batch
	batchLength          []byte   // 4 bytes -- indicates the length of the entire record batch in bytes, excluding the recordBatch base offset and itself
	partitionLeaderEpoch []byte   // 4 bytes -- indicates the epoch of the leader of this partition
	magicByte            byte     // indicates the version of the record batch format
	crc                  []byte   // 4 bytes -- checksum
	attributes           []byte   // 2 bytes -- indicates the attributes of each record
	lastOffsetDelta      []byte   //4 bytes -- indicates the difference between the last offset of this record batch and base offset
	baseTimeStamp        []byte   // 8 bytes -- timestamp of the first record in this batch
	maxTimeStamp         []byte   // 8 bytes -- max timestamp of the records in this batch
	producerID           []byte   // 8 bytes -- ID of the producer that produced the records of this batch
	producerEpoch        []byte   // 2 bytes -- Epoch of the producer that produced this batch
	baseSequence         []byte   // 4 bytes -- indicates the sequence number of the first record in a batch
	recordsLength        []byte   // 4 bytes -- indicates the number of records in this batch
	recordSlice          []Record // A slice of records

}

// Helper function to check MSB value
func hasMSBSet(b byte) bool {
	return (b & 0x80) != 0 // 0x80 is 10000000 in binary
}

// Helper function to take the last 7 bits of each byte and interpret them as a little endian value
func Decode7BitLittleEndian(b []byte) uint64 {
	var result uint64 = 0
	for i := 0; i < len(b); i++ {
		result |= uint64(b[i]&0x7F) << (7 * i)
	}
	return result
}

// Helper function to decode a bit pattern using zigzag decoding
func ZigZagDecode(n uint64) int64 {
	return int64((n >> 1) ^ uint64(-(n & 1)))
}

// Helper function for varint parsing. We use it on record items
func varIntParser(currentByte *int, inputBytes []byte) int64 {
	ending_byte := *currentByte
	for hasMSBSet(inputBytes[ending_byte]) {
		ending_byte += 1
	}
	val := ZigZagDecode(Decode7BitLittleEndian(inputBytes[*currentByte : ending_byte+1])) // We now have the value of the length. We need to pass it through a zigzag decoder
	*currentByte = ending_byte + 1
	return val
}

// this will parse the value information for all types of levels (partition, topic, and feature!)
func parseValueInformation(currentByte *int, inputBytes []byte) Value {
	val_obj := Value{}
	val_obj.frameVersion = inputBytes[*currentByte]
	*currentByte++
	val_obj.recordType = int8(inputBytes[*currentByte]) // indicates the type of record. How we parse everything else depends on this
	*currentByte++

	val_obj.version = inputBytes[*currentByte]
	*currentByte++

	if val_obj.recordType == 3 {
		val_obj.partitionID = inputBytes[*currentByte : *currentByte+4]
		*currentByte += 4

		val_obj.uuid = inputBytes[*currentByte : *currentByte+16]
		*currentByte += 16

		val_obj.replicaArrayLength = varIntParser(currentByte, inputBytes)
		val_obj.replicaArray = make([]uint32, val_obj.replicaArrayLength-1)
		tmp_counter := uint32(0)
		for tmp_counter < uint32(val_obj.replicaArrayLength) {
			val_obj.replicaArray = append(val_obj.replicaArray, binary.BigEndian.Uint32(inputBytes[*currentByte:*currentByte+4]))
			*currentByte += 4
			tmp_counter++
		}

		val_obj.lengthInSyncReplicaArray = varIntParser(currentByte, inputBytes)
		val_obj.inSyncReplicaArray = make([]uint32, val_obj.lengthInSyncReplicaArray-1)
		tmp_counter = uint32(0)
		for tmp_counter < uint32(val_obj.lengthInSyncReplicaArray) {
			val_obj.inSyncReplicaArray = append(val_obj.inSyncReplicaArray, binary.BigEndian.Uint32(inputBytes[*currentByte:*currentByte+4]))
			*currentByte += 4
			tmp_counter++
		}

		val_obj.lengthRemovingReplicasArray = varIntParser(currentByte, inputBytes)
		val_obj.removingReplicasArray = make([]uint32, val_obj.lengthRemovingReplicasArray-1)
		tmp_counter = uint32(0)
		for tmp_counter < uint32(val_obj.lengthRemovingReplicasArray) {
			val_obj.removingReplicasArray = append(val_obj.removingReplicasArray, binary.BigEndian.Uint32(inputBytes[*currentByte:*currentByte+4]))
			*currentByte += 4
			tmp_counter++
		}

		val_obj.lengthAddingReplicasArray = varIntParser(currentByte, inputBytes)
		val_obj.addingReplicasArray = make([]uint32, val_obj.lengthAddingReplicasArray-1)
		tmp_counter = uint32(0)
		for tmp_counter < uint32(val_obj.lengthAddingReplicasArray) {
			val_obj.addingReplicasArray = append(val_obj.addingReplicasArray, binary.BigEndian.Uint32(inputBytes[*currentByte:*currentByte+4]))
			*currentByte += 4
			tmp_counter++
		}

		val_obj.leader = inputBytes[*currentByte : *currentByte+4]
		*currentByte += 4

		val_obj.leaderEpoch = inputBytes[*currentByte : *currentByte+4]
		*currentByte += 4

		val_obj.partitionEpoch = inputBytes[*currentByte : *currentByte+4]
		*currentByte += 4

		val_obj.lengthOfDirectoriesArr = varIntParser(currentByte, inputBytes)
		val_obj.directoriesArray = make([][]byte, val_obj.lengthOfDirectoriesArr-1)
		tmp_counter = 0
		for tmp_counter < uint32(val_obj.lengthOfDirectoriesArr) {
			val_obj.directoriesArray[tmp_counter] = inputBytes[*currentByte : *currentByte+16]
			*currentByte += 16
			tmp_counter++
		}

	} else if val_obj.recordType == 2 {
		val_obj.nameLength = varIntParser(currentByte, inputBytes)

		val_obj.name = string(inputBytes[*currentByte : *currentByte+int(val_obj.nameLength)])
		*currentByte += int(val_obj.nameLength)

		// topic uuid
		val_obj.uuid = inputBytes[*currentByte : *currentByte+16]
		*currentByte += 16
	} else if val_obj.recordType == 12 {
		val_obj.nameLength = varIntParser(currentByte, inputBytes)

		val_obj.name = string(inputBytes[*currentByte : *currentByte+int(val_obj.nameLength)])
		*currentByte += int(val_obj.nameLength)

		val_obj.feature_level = int64(binary.BigEndian.Uint16(inputBytes[*currentByte : *currentByte+2]))
		*currentByte += 2
	}

	val_obj.taggedFieldsCount = varIntParser(currentByte, inputBytes)
	if val_obj.taggedFieldsCount != 0 {
		*currentByte += int(val_obj.taggedFieldsCount)
	}

	return val_obj
}

// For a given stream of bytes, we obtain the metadata and return a slice of RecordBatch
func createClusterMetaData(input_bytes []byte) []RecordBatch {
	var retRecordBatch []RecordBatch
	var currentByte int = 0
	for currentByte < len(input_bytes) {
		// Create the record batch
		recordBatchItem := RecordBatch{
			recordBatch:          input_bytes[currentByte : currentByte+8],
			batchLength:          input_bytes[currentByte+8 : currentByte+12],
			partitionLeaderEpoch: input_bytes[currentByte+12 : currentByte+16],
			magicByte:            input_bytes[currentByte+16],
			crc:                  input_bytes[currentByte+17 : currentByte+21],
			attributes:           input_bytes[currentByte+21 : currentByte+23],
			lastOffsetDelta:      input_bytes[currentByte+23 : currentByte+27],
			baseTimeStamp:        input_bytes[currentByte+27 : currentByte+35],
			maxTimeStamp:         input_bytes[currentByte+35 : currentByte+43],
			producerID:           input_bytes[currentByte+43 : currentByte+51],
			producerEpoch:        input_bytes[currentByte+51 : currentByte+53],
			baseSequence:         input_bytes[currentByte+53 : currentByte+57],
			recordsLength:        input_bytes[currentByte+57 : currentByte+61],
		}

		// Last part of the record batch - the record slice
		var recordSlice []Record

		currentByte += 61
		numRecords := int(binary.BigEndian.Uint32(recordBatchItem.recordsLength)) // convert to an integer to see the number of records in the batch

		var recordCounter int = 0
		for recordCounter < numRecords {
			// Making the record
			recordItem := Record{}

			// Get the record length
			recordItem.length = varIntParser(&currentByte, input_bytes)

			// Get the attributes of ther record
			recordItem.attributes = input_bytes[currentByte]
			currentByte++

			// Get the timestamp delta
			recordItem.timeStampDelta = varIntParser(&currentByte, input_bytes)

			// Get the offset delta
			recordItem.offsetDelta = varIntParser(&currentByte, input_bytes)

			// Key Length
			recordItem.keyLength = varIntParser(&currentByte, input_bytes)

			// Key
			if recordItem.keyLength > 0 {
				recordItem.key = input_bytes[currentByte : int64(currentByte)+recordItem.keyLength]
				currentByte = currentByte + int(recordItem.keyLength)
			}

			// Value Length
			recordItem.valueLength = varIntParser(&currentByte, input_bytes)

			// ------------------------------------------------------------
			// Value portion (Partition Record)
			recordItem.valueStruct = parseValueInformation(&currentByte, input_bytes)

			// ------------------------------------------------------------

			// Headers Array Count
			recordItem.headersArrayCount = varIntParser(&currentByte, input_bytes)

			recordSlice = append(recordSlice, recordItem)
			recordCounter++
		}

	}

	return retRecordBatch
}
