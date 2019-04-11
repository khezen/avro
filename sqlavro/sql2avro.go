package sqlavro

import (
	"database/sql"
	"strings"

	"github.com/khezen/avro"
)

// SQLDatabase2AVRO -
func SQLDatabase2AVRO(db *sql.DB, dbName string) ([]avro.Schema, error) {
	tables, err := getTables(db, dbName)
	if err != nil {
		return nil, err
	}
	var (
		tableName string
		schema    avro.Schema
		schemas   = make([]avro.Schema, 0, len(tables))
	)
	for _, tableName = range tables {
		schema, err = SQLTable2AVRO(db, dbName, tableName)
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, schema)
	}
	return schemas, nil
}

func getTables(db *sql.DB, dbName string) ([]string, error) {
	rows, err := db.Query(
		`SELECT TABLE_NAME 
		 FROM INFORMATION_SCHEMA.TABLES 
		 WHERE TABLE_SCHEMA=?`,
		dbName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var (
		tableName string
		tables    = make([]string, 0, 20)
	)
	for rows.Next() {
		err = rows.Scan(&tableName)
		if err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}
	return tables, nil
}

// SQLTable2AVRO -
func SQLTable2AVRO(db *sql.DB, dbName, tableName string) (*avro.RecordSchema, error) {
	rows, err := db.Query(
		`SELECT COLUMN_NAME,DATA_TYPE,IS_NULLABLE,COLUMN_DEFAULT,NUMERIC_PRECISION,NUMERIC_SCALE,CHARACTER_OCTET_LENGTH
		 FROM INFORMATION_SCHEMA.COLUMNS 
		 WHERE TABLE_SCHEMA=? 
		 AND TABLE_NAME=?`,
		dbName,
		tableName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var (
		fields            = make([]avro.RecordFieldSchema, 0, 50)
		columnName        string
		dataType          string
		isNullableStr     string
		isNullable        bool
		defaultValue      sql.NullString
		defaultValueBytes []byte
		numPrecision      sql.NullInt64
		numScale          sql.NullInt64
		charBytesLen      sql.NullInt64
	)
	for rows.Next() {
		err = rows.Scan(&columnName, &dataType, &isNullableStr, &defaultValue, &numPrecision, &numScale, &charBytesLen)
		if err != nil {
			return nil, err
		}
		dataType = strings.ToLower(dataType)
		isNullableStr = strings.ToLower(isNullableStr)
		isNullable = isNullableStr == "yes"
		if defaultValue.Valid {
			defaultValueBytes = []byte(defaultValue.String)
		} else {
			defaultValueBytes = nil
		}

		field, err := renderField(columnName, SQLType(dataType), isNullable, defaultValueBytes, int(numPrecision.Int64), int(numScale.Int64), int(charBytesLen.Int64))
		if err != nil {
			return nil, err
		}
		fields = append(fields, *field)
	}
	return &avro.RecordSchema{
		Type:      avro.TypeRecord,
		Namespace: dbName,
		Name:      tableName,
		Fields:    fields,
	}, nil
}

func renderField(columnName string, dataType SQLType, isNullable bool, defaultValue []byte, numPrecision, numScale, charBytesLen int) (*avro.RecordFieldSchema, error) {
	var (
		fieldType avro.Schema
	)
	switch dataType {
	case Char, NChar, VarChar, NVarChar,
		Text, TinyText, MediumText, LongText,
		Enum:
		fieldType = avro.TypeString
		break
	case Blob, MediumBlob, LongBlob:
		fieldType = avro.TypeBytes
		break
	case TinyInt, SmallInt, MediumInt, Int, Year:
		fieldType = avro.TypeInt32
		break
	case BigInt:
		fieldType = avro.TypeInt64
		break
	case Float:
		fieldType = avro.TypeFloat32
		break
	case Double:
		fieldType = avro.TypeFloat64
		break
	case Decimal:
		fieldType = &avro.DerivedPrimitiveSchema{
			Type:        avro.TypeBytes,
			LogicalType: avro.LogicalTypeDecimal,
			Precision:   &numPrecision,
			Scale:       &numScale,
		}
		break
	case Date:
		fieldType = &avro.DerivedPrimitiveSchema{
			Type:        avro.TypeInt32,
			LogicalType: avro.LogicalTypeDate,
		}
		break
	case Time:
		fieldType = &avro.DerivedPrimitiveSchema{
			Type:        avro.TypeInt32,
			LogicalType: avro.LogicalTypeTime,
		}
		break
	case DateTime, Timestamp:
		fieldType = &avro.DerivedPrimitiveSchema{
			Type:        avro.TypeInt32,
			LogicalType: avro.LogicalTypeTimestamp,
		}
		break
	default:
		return nil, avro.ErrUnsupportedType
	}
	if isNullable {
		fieldType = avro.UnionSchema([]avro.Schema{avro.TypeNull, fieldType})
	}
	return &avro.RecordFieldSchema{
		Name:    columnName,
		Type:    fieldType,
		Default: defaultValue,
	}, nil
}
