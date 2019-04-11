package sqlavro

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
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
			[]driver.Value{"uuid", "char", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: true, Int64: 108}},
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
	fmt.Println(string(schemasBytes))
}
