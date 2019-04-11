package sqlavro

import (
	"database/sql"
	"strings"

	"github.com/khezen/avro"
)

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
		fields            = make([]avro.RecordFieldSchema, 0, 100)
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
		field, err := sqlColumn2AVRO(columnName, SQLType(dataType), isNullable, defaultValueBytes, int(numPrecision.Int64), int(numScale.Int64), int(charBytesLen.Int64))
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
