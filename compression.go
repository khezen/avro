package avro

const (
	// CompressionNull - The "null" codec simply passes through data uncompressed.
	CompressionNull = "null"
	// CompressionDeflate - The "deflate" codec writes the data block using the deflate algorithm as specified in RFC 1951,
	// and typically implemented using the zlib library.
	// Note that this format (unlike the "zlib format" in RFC 1950) does not have a checksum.
	CompressionDeflate = "deflate"
	// CompressionSnappy - The "snappy" codec uses Google's Snappy compression library.
	// Each compressed block is followed by the 4-byte, big-endian CRC32 checksum of the uncompressed data in the block.
	CompressionSnappy = "snappy"
)
