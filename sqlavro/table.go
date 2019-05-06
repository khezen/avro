package sqlavro

import (
	"bytes"
	"database/sql"
	"strings"

	"github.com/khezen/avro"
)

// SQLTable2AVRO - transalte the given SQL table to AVRO schema
func SQLTable2AVRO(db *sql.DB, dbName, tableName string) (*avro.RecordSchema, error) {
	qBuf := bytes.NewBufferString(`
		 SELECT TABLE_SCHEMA,COLUMN_NAME,DATA_TYPE,IS_NULLABLE,COLUMN_DEFAULT,NUMERIC_PRECISION,NUMERIC_SCALE,CHARACTER_OCTET_LENGTH
		 FROM INFORMATION_SCHEMA.COLUMNS 
		 WHERE TABLE_NAME=?
	`)
	params := make([]interface{}, 0, 2)
	params = append(params, tableName)
	if len(dbName) > 0 {
		qBuf.WriteString(` AND TABLE_SCHEMA=?`)
		params = append(params, dbName)
	}
	rows, err := db.Query(
		qBuf.String(),
		params...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var (
		fields            = make([]avro.RecordFieldSchema, 0, 100)
		tableSchema       string
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
		err = rows.Scan(&tableSchema, &columnName, &dataType, &isNullableStr, &defaultValue, &numPrecision, &numScale, &charBytesLen)
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
		Namespace: tableSchema,
		Name:      tableName,
		Fields:    fields,
	}, nil
}
