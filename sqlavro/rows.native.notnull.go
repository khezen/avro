package sqlavro

import (
	"fmt"
	"math/big"
	"time"

	"github.com/khezen/avro"
)

func renderNativeFieldNotNull(schema avro.Schema, sqlField interface{}) (interface{}, error) {
	switch schema.TypeName() {
	case avro.TypeInt64:
		return *sqlField.(*int64), nil
	case avro.TypeInt32:
		return *sqlField.(*int32), nil
	case avro.Type(avro.LogicalTypeDate):
		return renderNativeDate(sqlField)
	case avro.Type(avro.LogicalTypeTime):
		return renderNativeTime(sqlField)
	case avro.Type(avro.LogicalTypeTimestamp):
		return renderNativeTimestamp(schema, sqlField)
	case avro.TypeFloat64:
		return *sqlField.(*float64), nil
	case avro.TypeFloat32:
		return *sqlField.(*float32), nil
	case avro.TypeString:
		return *sqlField.(*string), nil
	case avro.TypeBytes, avro.TypeFixed:
		return *sqlField.(*[]byte), nil
	case avro.Type(avro.LogicalTypeDecimal):
		return renderNativeDecimal(sqlField)
	}
	return nil, ErrUnsupportedTypeForSQL
}

func renderNativeTimestamp(schema avro.Schema, sqlField interface{}) (interface{}, error) {
	switch schema.(*avro.DerivedPrimitiveSchema).Documentation {
	case string(DateTime):
		timeStr := *sqlField.(*string)
		t, err := time.Parse(SQLDateTimeFormat, timeStr)
		if err != nil {
			return nil, err
		}
		return int32(t.Unix()), nil
	case "", string(Timestamp):
		return *sqlField.(*int32), nil
	default:
		return nil, ErrUnsupportedTypeForSQL
	}
}

func renderNativeTime(sqlField interface{}) (interface{}, error) {
	timeStr := *sqlField.(*string)
	t, err := time.Parse(SQLTimeFormat, timeStr)
	if err != nil {
		return nil, err
	}
	t = t.AddDate(1970, 1, 1)
	return int32(t.Unix()), nil
}

func renderNativeDate(sqlField interface{}) (interface{}, error) {
	timeStr := *sqlField.(*string)
	t, err := time.Parse(SQLDateFormat, timeStr)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func renderNativeDecimal(sqlField interface{}) (interface{}, error) {
	field := *sqlField.(*[]byte)
	r := new(big.Rat)
	_, err := fmt.Sscan(string(field), r)
	if err != nil {
		return nil, err
	}
	return r, nil
}