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
			[]driver.Value{"table1"},
		}
		fieldColumns = []string{
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
			[]driver.Value{"some_char", "CHAR", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: true, Int64: 108}},
			[]driver.Value{"some_varchar", "VARCHAR", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: true, Int64: 108}},
			[]driver.Value{"some_bolb", "LONGBLOB", "YES", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: true, Int64: 4294967295}},
			[]driver.Value{"some_int", "INT", "NO", sql.NullInt64{Valid: true, Int64: 18}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			[]driver.Value{"some_bigint", "BIGINT", "NO", sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			[]driver.Value{"some_float", "FLOAT", "NO", sql.NullFloat64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			[]driver.Value{"some_double", "DOUBLE", "NO", sql.NullFloat64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			[]driver.Value{"some_decimal", "DECIMAL", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: true, Int64: 8}, sql.NullInt64{Valid: true, Int64: 12}, sql.NullInt64{Valid: false}},
			[]driver.Value{"date", "DATE", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			[]driver.Value{"time", "TIME", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			[]driver.Value{"datetime", "DATETIME", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			[]driver.Value{"timestamp", "TIMESTAMP", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
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
		 FROM INFORMATION_SCHEMA.TABLES (.+)`,
	).WillReturnRows(mockedTableRows)

	mock.ExpectQuery(
		`SELECT COLUMN_NAME,DATA_TYPE,IS_NULLABLE,COLUMN_DEFAULT,NUMERIC_PRECISION,NUMERIC_SCALE,CHARACTER_OCTET_LENGTH
		FROM INFORMATION_SCHEMA.COLUMNS (.+)`,
	).WillReturnRows(mockFieldRows)
	schemas, err := SQLDatabase2AVRO(db, "dbTest")
	if err != nil {
		panic(err)
	}
	schemasBytes, err := json.Marshal(schemas)
	if err != nil {
		panic(err)
	}
	expectedSchemas := []byte(`[{"type":"record","namespace":"dbTest","name":"table1","fields":[{"name":"some_char","type":{"type":"fixed","name":"","size":108}},{"name":"some_varchar","type":"string"},{"name":"some_bolb","type":["null","bytes"]},{"name":"some_int","type":"int","default":18},{"name":"some_bigint","type":"long"},{"name":"some_float","type":"float"},{"name":"some_double","type":"double"},{"name":"some_decimal","type":{"type":"bytes","logicalType":"decimal","precision":8,"scale":12}},{"name":"date","type":{"type":"int","logicalType":"date"}},{"name":"time","type":{"type":"int","logicalType":"time"}},{"name":"datetime","type":{"type":"int","logicalType":"timestamp"}},{"name":"timestamp","type":{"type":"int","logicalType":"timestamp"}}]}]`)
	if !bytes.EqualFold(schemasBytes, expectedSchemas) {
		t.Errorf("expected:\n%s\ngot:\n%s\n", string(expectedSchemas), string(schemasBytes))
	}
}
