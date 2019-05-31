package sqlavro

import (
	"database/sql"
	"strconv"

	"github.com/khezen/avro"
)

func sql2CSVFieldNullable(schema avro.Schema, sqlField interface{}) (string, error) {
	union := schema.(avro.UnionSchema)
	subSchema, err := underlyingType(union)
	if err != nil {
		return "", err
	}
	switch subSchema.TypeName() {
	case avro.TypeInt64:
		return sql2CSVInt64Nullable(sqlField), nil
	case avro.TypeInt32:
		return sql2CSVInt32Nullable(sqlField), nil
	case avro.Type(avro.LogicalTypeDate):
		return sql2CSVDateNullable(sqlField), nil
	case avro.Type(avro.LogicalTypeTime):
		return sql2CSVTimeNullable(sqlField), nil
	case avro.Type(avro.LogicalTypeTimestamp):
		return sql2CSVTimestampNullable(subSchema, sqlField)
	case avro.TypeFloat64:
		return sql2CSVFloat64Nullable(sqlField), nil
	case avro.TypeFloat32:
		return sql2CSVFloat32Nullable(sqlField), nil
	case avro.TypeString:
		return sql2CSVStringNullable(sqlField), nil
	case avro.TypeBytes, avro.TypeFixed:
		return sql2CSVBytesNFixedNullable(subSchema, sqlField), nil
	case avro.Type(avro.LogicalTypeDecimal):
		return sql2CSVDecimalNullable(sqlField), nil
	}
	return "", ErrUnsupportedTypeForSQL
}

func sql2CSVInt64Nullable(sqlField interface{}) string {
	nullableField := sqlField.(*sql.NullInt64)
	return strconv.FormatInt(nullableField.Int64, 64)
}

func sql2CSVInt32Nullable(sqlField interface{}) string {
	nullableField := sqlField.(*sql.NullInt64)
	return strconv.FormatInt(nullableField.Int64, 32)
}

func sql2CSVFloat64Nullable(sqlField interface{}) string {
	nullableField := sqlField.(*sql.NullFloat64)
	return strconv.FormatFloat(nullableField.Float64, 'f', -1, 64)
}

func sql2CSVFloat32Nullable(sqlField interface{}) string {
	nullableField := sqlField.(*sql.NullFloat64)
	return strconv.FormatFloat(nullableField.Float64, 'f', -1, 32)
}

func sql2CSVStringNullable(sqlField interface{}) string {
	nullableField := sqlField.(*sql.NullString)
	return nullableField.String
}

func sql2CSVBytesNFixedNullable(schema avro.Schema, sqlField interface{}) string {
	field := *sqlField.(*[]byte)
	return string(field)
}

func sql2CSVDateNullable(sqlField interface{}) string {
	nullableField := sqlField.(*sql.NullString)
	return nullableField.String
}

func sql2CSVTimeNullable(sqlField interface{}) string {
	nullableField := sqlField.(*sql.NullString)
	return nullableField.String
}

func sql2CSVTimestampNullable(schema avro.Schema, sqlField interface{}) (string, error) {
	switch schema.(*avro.DerivedPrimitiveSchema).Documentation {
	case string(DateTime):
		nullableField := sqlField.(*sql.NullString)
		return nullableField.String, nil
	case "", string(Timestamp):
		nullableField := sqlField.(*sql.NullInt64)
		return strconv.FormatInt(nullableField.Int64, 64), nil
	default:
		return "", ErrUnsupportedTypeForSQL
	}
}

func sql2CSVDecimalNullable(sqlField interface{}) string {
	field := *sqlField.(*[]byte)
	return string(field)
}
