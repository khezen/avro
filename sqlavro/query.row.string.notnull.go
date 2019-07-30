package sqlavro

import (
	"strconv"

	"github.com/khezen/avro"
)

func sql2StringFieldNotNull(schema avro.Schema, sqlField interface{}) (string, error) {
	switch schema.TypeName() {
	case avro.TypeInt64:
		return strconv.FormatInt(*sqlField.(*int64), 10), nil
	case avro.TypeInt32:
		return strconv.FormatInt(int64(*sqlField.(*int32)), 10), nil
	case avro.Type(avro.LogicalTypeDate):
		return sql2StringDate(sqlField)
	case avro.Type(avro.LogicalTypeTime):
		return sql2StringTime(sqlField)
	case avro.Type(avro.LogicalTypeTimestamp):
		return sql2StringTimestamp(schema, sqlField)
	case avro.TypeFloat64:
		return strconv.FormatFloat(*sqlField.(*float64), 'f', -1, 64), nil
	case avro.TypeFloat32:
		return strconv.FormatFloat(float64(*sqlField.(*float32)), 'f', -1, 64), nil
	case avro.TypeString:
		return *sqlField.(*string), nil
	case avro.TypeBytes, avro.TypeFixed:
		return string(*sqlField.(*[]byte)), nil
	case avro.Type(avro.LogicalTypeDecimal):
		return sql2StringDecimal(sqlField)
	}
	return "", ErrUnsupportedTypeForSQL
}

func sql2StringTimestamp(schema avro.Schema, sqlField interface{}) (string, error) {
	return *sqlField.(*string), nil
}

func sql2StringTime(sqlField interface{}) (string, error) {
	return *sqlField.(*string), nil
}

func sql2StringDate(sqlField interface{}) (string, error) {
	return *sqlField.(*string), nil
}

func sql2StringDecimal(sqlField interface{}) (string, error) {
	field := *sqlField.(*[]byte)
	return string(field), nil
}
