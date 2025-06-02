package main

import "encoding/binary"

// import (
// 	"bytes"
// 	"encoding/binary"
// )

type Value struct {
	frameVersion                byte     //integer indicating the version format of the record
	recordType                  byte     //integer explaining the type of the record
	versionFeatureLevel         byte     // indicates the version of the feature type record
	partitionID                 []byte   //4 byte ID of the partition
	uuid                        []byte   //16 byte uuid
	replicaArrayLength          int64    //varint length of replica array. Compact Int and replicaArray length is actually this value - 1
	replicaArray                []uint32 // 4 byte big endian integers containing replicaID of the replicas
	lengthInSyncReplicaArray    int64    //varint length of replica sync array. Compact Int and replicaSyncArray length is actually this value - 1
	inSyncReplicaArray          []byte   // 4 byte big endian integers containing replicaID of in sync replicas
	lengthRemovingReplicasArray int64    //varint length of removing replicas array. Compact Int and replicaSyncArray length is actually this value - 1
	lengthAddingReplicasArray   int64    //varint length of adding replicas array. Compact Int and replicaSyncArray length is actually this value - 1
	leader                      []byte   // 4 byte big endian integer indicating replicaID of the leader
	leaderEpoch                 []byte   // 4 byte big endian integer indicating the epoch of the leader
	partitionEpoch              []byte   // 4 byte big endian integer indicating the epoch of the partition
	lengthOfDirectoriesArr      int64    // unsigned var size integer indicating # directories in array. length is actually this value - 1 since compact
	directoriesArray            []byte   //16 byte raw byte arrays
	taggedFieldsCount           int64    //unsigned var size integer
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

func parseValueInformation(currentByte *int, inputBytes []byte) Value {
	val_obj := Value{}

	// frame version
	val_obj.frameVersion = inputBytes[*currentByte]
	*currentByte++

	// type
	val_obj.recordType = inputBytes[*currentByte]
	*currentByte++

	// version
	val_obj.versionFeatureLevel = inputBytes[*currentByte]
	*currentByte++

	// partition id
	val_obj.partitionID = inputBytes[*currentByte : *currentByte+4] // copy(val_obj.partitionID, input_bytes[currentByte:currentByte+4])
	*currentByte += 4

	// topic uuid
	val_obj.uuid = inputBytes[*currentByte : *currentByte+16]
	*currentByte += 16

	// replica array length
	val_obj.replicaArrayLength = varIntParser(currentByte, inputBytes)

	// replica array
	var counter int64 = 0
	var tmp_val uint32
	val_obj.replicaArray = make([]uint32, 0, val_obj.replicaArrayLength)
	for counter < val_obj.replicaArrayLength {
		tmp_val = binary.BigEndian.Uint32(inputBytes[*currentByte : *currentByte+4])
		val_obj.replicaArray = append(val_obj.replicaArray, tmp_val)
		*currentByte += 4
		counter++
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
