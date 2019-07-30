package sqlavro

import (
	"database/sql"
	"strconv"

	"github.com/khezen/avro"
)

func sql2StringFieldNullable(schema avro.Schema, sqlField interface{}) (string, error) {
	union := schema.(avro.UnionSchema)
	subSchema, err := UnderlyingType(union)
	if err != nil {
		return "", err
	}
	switch subSchema.TypeName() {
	case avro.TypeInt64:
		return sql2StringInt64Nullable(sqlField), nil
	case avro.TypeInt32:
		return sql2StringInt32Nullable(sqlField), nil
	case avro.Type(avro.LogicalTypeDate):
		return sql2StringDateNullable(sqlField), nil
	case avro.Type(avro.LogicalTypeTime):
		return sql2StringTimeNullable(sqlField), nil
	case avro.Type(avro.LogicalTypeTimestamp):
		return sql2StringTimestampNullable(subSchema, sqlField)
	case avro.TypeFloat64:
		return sql2StringFloat64Nullable(sqlField), nil
	case avro.TypeFloat32:
		return sql2StringFloat32Nullable(sqlField), nil
	case avro.TypeString:
		return sql2StringStringNullable(sqlField), nil
	case avro.TypeBytes, avro.TypeFixed:
		return sql2StringBytesNFixedNullable(subSchema, sqlField), nil
	case avro.Type(avro.LogicalTypeDecimal):
		return sql2StringDecimalNullable(sqlField), nil
	}
	return "", ErrUnsupportedTypeForSQL
}

func sql2StringInt64Nullable(sqlField interface{}) string {
	nullableField := sqlField.(*sql.NullInt64)
	return strconv.FormatInt(nullableField.Int64, 10)
}

func sql2StringInt32Nullable(sqlField interface{}) string {
	nullableField := sqlField.(*sql.NullInt64)
	return strconv.FormatInt(nullableField.Int64, 10)
}

func sql2StringFloat64Nullable(sqlField interface{}) string {
	nullableField := sqlField.(*sql.NullFloat64)
	return strconv.FormatFloat(nullableField.Float64, 'f', -1, 64)
}

func sql2StringFloat32Nullable(sqlField interface{}) string {
	nullableField := sqlField.(*sql.NullFloat64)
	return strconv.FormatFloat(nullableField.Float64, 'f', -1, 32)
}

func sql2StringStringNullable(sqlField interface{}) string {
	nullableField := sqlField.(*sql.NullString)
	return nullableField.String
}

func sql2StringBytesNFixedNullable(schema avro.Schema, sqlField interface{}) string {
	field := *sqlField.(*[]byte)
	return string(field)
}

func sql2StringDateNullable(sqlField interface{}) string {
	nullableField := sqlField.(*sql.NullString)
	return nullableField.String
}

func sql2StringTimeNullable(sqlField interface{}) string {
	nullableField := sqlField.(*sql.NullString)
	return nullableField.String
}

func sql2StringTimestampNullable(schema avro.Schema, sqlField interface{}) (string, error) {
	nullableField := sqlField.(*sql.NullString)
	return nullableField.String, nil
}

func sql2StringDecimalNullable(sqlField interface{}) string {
	field := *sqlField.(*[]byte)
	return string(field)
}
