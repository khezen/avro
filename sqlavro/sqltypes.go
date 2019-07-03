package sqlavro

import "github.com/khezen/avro"

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
	// Set -
	Set SQLType = "set"

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

	// MySQL specific

	// Bit -
	Bit SQLType = "bit"

	// SQLDateTimeFormat -
	SQLDateTimeFormat = "2006-01-02 15:04:05"

	// SQLDateFormat -
	SQLDateFormat = "2006-01-02"

	// SQLTimeFormat -
	SQLTimeFormat = "15:04:05"
)

// UnderlyingType -
func UnderlyingType(union avro.UnionSchema) (avro.Schema, error) {
	isNullable := false
	var subSchema avro.Schema
	if len(union) > 2 {
		return nil, ErrUnsupportedTypeForSQL
	}
	for _, t := range union {
		if t.TypeName() == avro.TypeNull {
			isNullable = true
		} else {
			subSchema = t
		}
	}
	if !isNullable {
		return nil, ErrUnsupportedTypeForSQL
	}
	return subSchema, nil
}
