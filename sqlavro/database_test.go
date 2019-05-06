package sqlavro

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestSQL2AVRO(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	var (
		tableColumns = []string{
			"TABLE_NAME",
		}
		mockedTableRows = sqlmock.NewRows(tableColumns)
		tableRowsValues = [][]driver.Value{
			{"table1"},
		}
		fieldColumns = []string{
			"TABLE_SCHEMA",
			"COLUMN_NAME",
			"DATA_TYPE",
			"IS_NULLABLE",
			"COLUMN_DEFAULT",
			"NUMERIC_PRECISION",
			"NUMRIC_SCALE",
			"CHARACTER_OCTET_LENGTH",
		}
		mockFieldRows   = sqlmock.NewRows(fieldColumns)
		fieldRowsValues = [][]driver.Value{
			{"dbTest", "some_char", "CHAR", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: true, Int64: 108}},
			{"dbTest", "some_varchar", "VARCHAR", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: true, Int64: 108}},
			{"dbTest", "some_bolb", "LONGBLOB", "YES", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: true, Int64: 4294967295}},
			{"dbTest", "some_int", "INT", "NO", sql.NullInt64{Valid: true, Int64: 18}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"dbTest", "some_bigint", "BIGINT", "NO", sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"dbTest", "some_float", "FLOAT", "NO", sql.NullFloat64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"dbTest", "some_double", "DOUBLE", "NO", sql.NullFloat64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"dbTest", "some_decimal", "DECIMAL", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: true, Int64: 8}, sql.NullInt64{Valid: true, Int64: 12}, sql.NullInt64{Valid: false}},
			{"dbTest", "date", "DATE", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"dbTest", "time", "TIME", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"dbTest", "datetime", "DATETIME", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"dbTest", "date_default", "DATE", "NO", sql.NullString{Valid: true, String: "1970-01-01"}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"dbTest", "time_default", "TIME", "NO", sql.NullString{Valid: true, String: "00:00:00"}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"dbTest", "timestamp", "TIMESTAMP", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			// blog post fields example
			{"dbTest", "ID", "INT", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"dbTest", "title", "VARCHAR", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: true, Int64: 384}},
			{"dbTest", "body", "LONGBLOB", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: true, Int64: 4294967295}},
			{"dbTest", "content_type", "VARCHAR", "YES", sql.NullString{Valid: true, String: "text/markdown; charset=UTF-8"}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: true, Int64: 384}},
			{"dbTest", "post_date", "DATETIME", "NO", sql.NullString{Valid: true, String: "CURRENT_TIMESTAMP"}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"dbTest", "update_date", "DATETIME", "YES", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"dbTest", "reading_time_minutes", "DECIMAL", "YES", sql.NullString{Valid: false}, sql.NullInt64{Valid: true, Int64: 3}, sql.NullInt64{Valid: true, Int64: 1}, sql.NullInt64{Valid: false}},
		}
	)
	for _, rowValues := range tableRowsValues {
		mockedTableRows.AddRow(rowValues...)
	}
	for _, rowValues := range fieldRowsValues {
		mockFieldRows.AddRow(rowValues...)
	}
	mock.ExpectQuery(
		`SELECT TABLE_NAME 
		 FROM INFORMATION_SCHEMA.TABLES(.*)`,
	).WillReturnRows(mockedTableRows)

	mock.ExpectQuery(
		`SELECT TABLE_SCHEMA,COLUMN_NAME,DATA_TYPE,IS_NULLABLE,COLUMN_DEFAULT,NUMERIC_PRECISION,NUMERIC_SCALE,CHARACTER_OCTET_LENGTH
		FROM INFORMATION_SCHEMA.COLUMNS (.*)`,
	).WillReturnRows(mockFieldRows)
	schemas, err := SQLDatabase2AVRO(db, "")
	if err != nil {
		panic(err)
	}
	schemasBytes, err := json.Marshal(schemas)
	if err != nil {
		panic(err)
	}
	expectedSchemas := []byte(`[{"type":"record","namespace":"dbTest","name":"table1","fields":[{"name":"some_char","type":{"type":"fixed","name":"some_char","size":108}},{"name":"some_varchar","type":"string"},{"name":"some_bolb","type":["null","bytes"]},{"name":"some_int","type":"int","default":18},{"name":"some_bigint","type":"long"},{"name":"some_float","type":"float"},{"name":"some_double","type":"double"},{"name":"some_decimal","type":{"type":"bytes","logicalType":"decimal","precision":8,"scale":12}},{"name":"date","type":{"type":"int","logicalType":"date"}},{"name":"time","type":{"type":"int","logicalType":"time"}},{"name":"datetime","type":{"type":"int","doc":"datetime","logicalType":"timestamp"}},{"name":"date_default","type":{"type":"int","logicalType":"date"},"default":0},{"name":"time_default","type":{"type":"int","logicalType":"time"},"default":0},{"name":"timestamp","type":{"type":"int","doc":"timestamp","logicalType":"timestamp"}},{"name":"ID","type":"int"},{"name":"title","type":"string"},{"name":"body","type":"bytes"},{"name":"content_type","type":["string","null"],"default":"text/markdown; charset=UTF-8"},{"name":"post_date","type":{"type":"int","doc":"datetime","logicalType":"timestamp"}},{"name":"update_date","type":["null",{"type":"int","doc":"datetime","logicalType":"timestamp"}]},{"name":"reading_time_minutes","type":["null",{"type":"bytes","logicalType":"decimal","precision":3,"scale":1}]}]}]`)
	if !bytes.EqualFold(schemasBytes, expectedSchemas) {
		t.Errorf("expected:\n%s\ngot:\n%s\n", string(expectedSchemas), string(schemasBytes))
	}
}
