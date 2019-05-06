package sqlavro

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/khezen/avro"
)

func sqlColumn2AVRO(columnName string, dataType SQLType, isNullable bool, defaultValue []byte, numPrecision, numScale, charBytesLen int) (*avro.RecordFieldSchema, error) {
	fieldType, err := sqlColumn2AVROType(columnName, dataType, isNullable, numPrecision, numScale, charBytesLen)
	if err != nil {
		return nil, err
	}
	if defaultValue != nil {
		defaultValue = sqlDefault2AVRODefault(dataType, defaultValue)
	}
	if isNullable {
		if defaultValue == nil || strings.EqualFold("null", strings.ToLower(string(defaultValue))) {
			fieldType = avro.UnionSchema([]avro.Schema{avro.TypeNull, fieldType})
		} else {
			fieldType = avro.UnionSchema([]avro.Schema{fieldType, avro.TypeNull})
		}
	}
	return &avro.RecordFieldSchema{
		Name:    columnName,
		Type:    fieldType,
		Default: defaultValue,
	}, nil
}

func sqlColumn2AVROType(columnName string, dataType SQLType, isNullable bool, numPrecision, numScale, charBytesLen int) (fieldType avro.Schema, err error) {
	switch dataType {
	case Char, NChar:
		return &avro.FixedSchema{
			Name: columnName,
			Type: avro.TypeFixed,
			Size: charBytesLen,
		}, nil
	case VarChar, NVarChar,
		Text, TinyText, MediumText, LongText,
		Enum, Set:
		return avro.TypeString, nil
	case Blob, MediumBlob, LongBlob:
		return avro.TypeBytes, nil
	case TinyInt, SmallInt, MediumInt, Int, Year:
		return avro.TypeInt32, nil
	case BigInt:
		return avro.TypeInt64, nil
	case Float:
		return avro.TypeFloat32, nil
	case Double:
		return avro.TypeFloat64, nil
	case Decimal:
		return &avro.DerivedPrimitiveSchema{
			Type:        avro.TypeBytes,
			LogicalType: avro.LogicalTypeDecimal,
			Precision:   &numPrecision,
			Scale:       &numScale,
		}, nil
	case Date:
		return &avro.DerivedPrimitiveSchema{
			Type:        avro.TypeInt32,
			LogicalType: avro.LogicalTypeDate,
		}, nil
	case Time:
		return &avro.DerivedPrimitiveSchema{
			Type:        avro.TypeInt32,
			LogicalType: avro.LogicalTypeTime,
		}, nil
	case DateTime:
		return &avro.DerivedPrimitiveSchema{
			Type:          avro.TypeInt32,
			Documentation: string(DateTime),
			LogicalType:   avro.LogicalTypeTimestamp,
		}, nil
	case Timestamp:
		return &avro.DerivedPrimitiveSchema{
			Type:          avro.TypeInt32,
			Documentation: string(Timestamp),
			LogicalType:   avro.LogicalTypeTimestamp,
		}, nil
	default:
		return nil, avro.ErrUnsupportedType
	}
}

func sqlDefault2AVRODefault(dataType SQLType, sqlDefaultValue []byte) (avroDefault []byte) {
	switch dataType {
	case Char, NChar, VarChar, NVarChar,
		Text, TinyText, MediumText, LongText,
		Enum, Set:
		return []byte(fmt.Sprintf(`"%s"`, string(sqlDefaultValue)))
	case Date, Time, DateTime:
		var format string
		switch dataType {
		case Date:
			format = "2006-01-02"
			break
		case Time:
			format = "15:04:05"
			break
		case DateTime:
			format = "2006-01-02 15:04:05"
		}
		t, err := time.Parse(format, string(sqlDefaultValue))
		if err != nil {
			return nil
		}
		if dataType == Time {
			t = t.AddDate(1970, 0, 0)
		}
		return []byte(strconv.Itoa(int(t.Unix())))
	default:
		return sqlDefaultValue
	}
}
