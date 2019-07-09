package redshiftavro

import "strconv"

const (
	// SortStyleCompound -
	SortStyleCompound SortStyle = "COMPOUND"
	// SortStyleInterleaved -
	SortStyleInterleaved SortStyle = "INTERLEAVED"
	// SortStyleNormal -
	SortStyleNormal SortStyle = ""
)

const (
	// DistStyleAuto -
	DistStyleAuto DistStyle = "AUTO"
	// DistStyleEven -
	DistStyleEven DistStyle = "EVEN"
	// DistStyleKey -
	DistStyleKey DistStyle = "KEY"
	// DistStyleAll -
	DistStyleAll DistStyle = "ALL"
)

const (
	// SmallInt -
	SmallInt RedshiftType = "SMALLINT"
	// Integer -
	Integer RedshiftType = "INTEGER"
	// BigInt -
	BigInt RedshiftType = "BIGINT"
	// Decimal -
	Decimal RedshiftType = "DECIMAL"
	// Real -
	Real RedshiftType = "REAL"
	// Double -
	Double RedshiftType = "DOUBLE"
	// Boolean -
	Boolean RedshiftType = "BOOLEAN"
	// Char -
	Char RedshiftType = "CHAR"
	// VarChar -
	VarChar RedshiftType = "VARCHAR"
	// Date -
	Date RedshiftType = "Date"
	// Timestamp -
	Timestamp RedshiftType = "TIMESTAMP"
	// Timestamptz -
	Timestamptz RedshiftType = "TIMESTAMPTZ"
)

const (
	// ByteDict -
	ByteDict RedshiftEncoding = "BYTEDICT"

	// Delta -
	Delta RedshiftEncoding = "DELTA"

	// Delta32K -
	Delta32K RedshiftEncoding = "DELTA32K"

	// LZO -
	LZO RedshiftEncoding = "LZO"

	// Mostly8 -
	Mostly8 RedshiftEncoding = "MOSTLY8"

	// Mostly16 -
	Mostly16 RedshiftEncoding = "MOSTLY16"

	// Mostly32 -
	Mostly32 RedshiftEncoding = "MOSTLY32"

	// Raw -
	Raw RedshiftEncoding = "RAW"

	// RunLength -
	RunLength RedshiftEncoding = "RUNLENGTH"

	// Text255 -
	Text255 RedshiftEncoding = "TEXT255"

	// Text32K -
	Text32K RedshiftEncoding = "TEXT32K"

	// ZSTD -
	ZSTD RedshiftEncoding = "ZSTD"
)

var (
	varcharLen    int64 = 65535
	varcharLenStr       = strconv.FormatInt(varcharLen, 10)
)
