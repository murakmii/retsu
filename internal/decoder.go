package internal

import "encoding/binary"

func Int32Decoder(data []byte) (int32, []byte) {
	return int32(binary.LittleEndian.Uint32(data)), data[4:]
}

func Int64Decoder(data []byte) (int64, []byte) {
	return int64(binary.LittleEndian.Uint64(data)), data[8:]
}
