package redshiftavro

import (
	"bytes"
	"strconv"

	"github.com/khezen/avro"

	"github.com/khezen/avro/sqlavro"
)

// CreateTableStatement -
func CreateTableStatement(cfg CreateConfig) (string, error) {
	buf := bytes.NewBufferString("CREATE TABLE ")
	if cfg.IfNotExists {
		buf.WriteString("IF NOT EXISTS ")
	}
	tableName := sqlavro.SQLEscape(cfg.Schema.Name)
	buf.WriteString(tableName)
	buf.WriteRune('(')
	var (
		fieldsLen  = len(cfg.Schema.Fields)
		i          int
		field      avro.RecordFieldSchema
		columnStmt string
		err        error
	)
	for i, field = range cfg.Schema.Fields {
		columnStmt, err = createColumnStatement(field, nil, nil)
		if err != nil {
			return "", err
		}
		buf.WriteString(columnStmt)
		if i < fieldsLen-1 {
			buf.WriteRune(',')
		}
	}
	buf.WriteRune(')')
	return buf.String(), nil
}

func createColumnStatement(field avro.RecordFieldSchema, sortKey *SortKey, distKey *DistKey) (string, error) {
	var (
		columnName = sqlavro.SQLEscape(field.Name)
		buf        = new(bytes.Buffer)
	)
	typeStatement, redshiftType, isNullable, err := renderType(field)
	if err != nil {
		return "", err
	}
	encoding, err := renderDefaultEncoding(redshiftType, sortKey)
	buf.WriteString(columnName)
	buf.WriteRune(' ')
	buf.WriteString(typeStatement)
	buf.WriteRune(' ')
	buf.WriteString("ENCODE ")
	buf.WriteString(string(encoding))
	buf.WriteRune(' ')
	if sortKey != nil {
		buf.WriteString("SORTKEY ")
		buf.WriteString(string(sortKey.SortStyle))
		buf.WriteRune(' ')
	}
	if distKey != nil {
		buf.WriteString("DISTKEY ")
		buf.WriteString(string(distKey.DistStyle))
		buf.WriteRune(' ')
	}
	if !isNullable {
		buf.WriteString("NOT NULL")
	} else {
		buf.WriteString("NULL")
	}
	return buf.String(), nil
}

func renderType(field avro.RecordFieldSchema) (typeStatement string, redshiftType RedshiftType, isNullable bool, err error) {
	isNullable = field.Type.TypeName() == avro.TypeUnion
	var schema avro.Schema
	if isNullable {
		schema, err = sqlavro.UnderlyingType(field.Type.(avro.UnionSchema))
		if err != nil {
			return "", "", false, err
		}
	} else {
		schema = field.Type
	}
	switch schema.TypeName() {
	case avro.TypeInt32:
		return string(Integer), Integer, isNullable, nil
	case avro.TypeInt64, avro.Type(avro.LogialTypeDuration):
		return string(BigInt), BigInt, isNullable, nil
	case avro.TypeFloat32:
		return string(Real), Real, isNullable, nil
	case avro.TypeFloat64:
		return string(Double), Double, isNullable, nil
	case avro.Type(avro.LogicalTypeDecimal):
		buf := new(bytes.Buffer)
		buf.WriteString(string(Decimal))
		buf.WriteRune('(')
		dec := schema.(*avro.DerivedPrimitiveSchema)
		precistionStr := strconv.FormatInt(int64(*dec.Precision), 10)
		buf.WriteString(precistionStr)
		buf.WriteRune(',')
		scaleStr := strconv.FormatInt(int64(*dec.Scale), 10)
		buf.WriteString(scaleStr)
		buf.WriteRune(')')
		return buf.String(), Decimal, isNullable, nil
	case avro.TypeEnum, avro.TypeString, avro.TypeBytes:
		buf := new(bytes.Buffer)
		buf.WriteString(string(VarChar))
		buf.WriteRune('(')
		buf.WriteString(varcharLenStr)
		buf.WriteRune(')')
		return buf.String(), VarChar, isNullable, nil
	case avro.TypeFixed:
		buf := new(bytes.Buffer)
		buf.WriteString(string(Char))
		fix := schema.(*avro.FixedSchema)
		size := strconv.FormatInt(int64(fix.Size), 10)
		buf.WriteRune('(')
		buf.WriteString(size)
		buf.WriteRune(')')
		return buf.String(), Char, isNullable, nil
	case avro.TypeBoolean:
		return string(Boolean), Boolean, isNullable, nil
	case avro.Type(avro.LogicalTypeDate):
		return string(Date), Date, isNullable, nil
	case avro.Type(avro.LogicalTypeTime), avro.Type(avro.LogicalTypeTimestamp):
		return string(Timestamp), Timestamp, isNullable, nil
	default:
		return "", RedshiftType(""), false, ErrUnsupportedRedshiftType
	}
}

func renderDefaultEncoding(redshiftType RedshiftType, sortKey *SortKey) (encoding RedshiftEncoding, err error) {
	if sortKey != nil {
		return Raw, nil
	}
	switch redshiftType {
	case Integer:
		return LZO, nil
	case BigInt:
		return LZO, nil
	case Decimal:
		return Raw, nil
	case Real:
		return Raw, nil
	case Double:
		return Raw, nil
	case Boolean:
		return Raw, nil
	case Char:
		return ZSTD, nil
	case VarChar:
		return ZSTD, nil
	case Date:
		return LZO, nil
	case Timestamp:
		return LZO, nil
	default:
		return RedshiftEncoding(""), ErrUnsupportedRedshiftType
	}
}
