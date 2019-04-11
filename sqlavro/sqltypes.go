package sqlavro

// SQLType -
type SQLType string

const (

	// Text types

	// Char -
	Char SQLType = "char"
	// NChar -
	NChar SQLType = "nchar"
	// VarChar -
	VarChar SQLType = "varchar"
	// NVarChar -
	NVarChar SQLType = "nvarchar"
	// Text -
	Text SQLType = "text"
	// TinyText -
	TinyText SQLType = "tinytext"
	// MediumText -
	MediumText SQLType = "mdeiumtext"
	// LongText -
	LongText SQLType = "longtext"
	// Blob -
	Blob SQLType = "blob"
	// MediumBlob -
	MediumBlob SQLType = "mediumblob"
	// LongBlob -
	LongBlob SQLType = "longblob"
	// Enum -
	Enum SQLType = "enum"

	// Number types

	// TinyInt -
	TinyInt SQLType = "tinyint"
	// SmallInt -
	SmallInt SQLType = "smallint"
	// MediumInt -
	MediumInt SQLType = "mediumint"
	// Int -
	Int SQLType = "int"
	// BigInt -
	BigInt SQLType = "bigint"
	// Float -
	Float SQLType = "float"
	// Double -
	Double SQLType = "double"
	// Decimal -
	Decimal SQLType = "decimal"

	// Date types

	// Date -
	Date SQLType = "date"
	// DateTime -
	DateTime SQLType = "datetime"
	// Timestamp -
	Timestamp SQLType = "timestamp"
	// Time -
	Time SQLType = "time"
	// Year -
	Year SQLType = "year"
)
