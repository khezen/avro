package sqlavro

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"strconv"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestQuery2CSV(t *testing.T) {
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
			{"posts"},
		}
		infoColumns = []string{
			"TABLE_SCHEMA",
			"COLUMN_NAME",
			"DATA_TYPE",
			"IS_NULLABLE",
			"COLUMN_DEFAULT",
			"NUMERIC_PRECISION",
			"NUMRIC_SCALE",
			"CHARACTER_OCTET_LENGTH",
		}
		mockInfoRows   = sqlmock.NewRows(infoColumns)
		infoRowsValues = [][]driver.Value{
			{"blog", "ID", "INT", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"blog", "title", "VARCHAR", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: true, Int64: 384}},
			{"blog", "body", "LONGBLOB", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: true, Int64: 4294967295}},
			{"blog", "content_type", "VARCHAR", "YES", sql.NullString{Valid: true, String: "text/markdown; charset=UTF-8"}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: true, Int64: 384}},
			{"blog", "author", "VARCHAR", "YES", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: true, Int64: 384}},
			{"blog", "post_datetime", "DATETIME", "NO", sql.NullString{Valid: true, String: "CURRENT_TIMESTAMP"}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"blog", "update_datetime", "DATETIME", "YES", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"blog", "reading_time_minutes", "DECIMAL", "YES", sql.NullString{Valid: false}, sql.NullInt64{Valid: true, Int64: 3}, sql.NullInt64{Valid: true, Int64: 1}, sql.NullInt64{Valid: false}},
			{"blog", "daily_average_traffic", "DECIMAL", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: true, Int64: 14}, sql.NullInt64{Valid: true, Int64: 2}, sql.NullInt64{Valid: false}},
			{"blog", "post_date", "DATE", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"blog", "post_time", "TIME", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"blog", "post_timestamp", "TIMESTAMP", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"blog", "some_int64", "BIGINT", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"blog", "some_float64", "DOUBLE", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"blog", "some_float32", "FLOAT", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"blog", "update_date", "DATE", "YES", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"blog", "update_time", "TIME", "YES", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"blog", "update_timestamp", "TIMESTAMP", "YES", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"blog", "some_nullable_int32", "INT", "YES", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"blog", "some_nullable_int64", "BIGINT", "YES", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"blog", "some_nullable_float64", "DOUBLE", "YES", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"blog", "some_nullable_float32", "FLOAT", "YES", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			{"blog", "some_nullable_blob", "BLOB", "YES", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
		}
	)
	for _, rowValues := range tableRowsValues {
		mockedTableRows.AddRow(rowValues...)
	}
	for _, rowValues := range infoRowsValues {
		mockInfoRows.AddRow(rowValues...)
	}
	mock.ExpectQuery(
		`SELECT TABLE_NAME 
		 FROM INFORMATION_SCHEMA.TABLES(.*)`,
	).WillReturnRows(mockedTableRows)
	mock.ExpectQuery(
		`SELECT TABLE_SCHEMA,COLUMN_NAME,DATA_TYPE,IS_NULLABLE,COLUMN_DEFAULT,NUMERIC_PRECISION,NUMERIC_SCALE,CHARACTER_MAXIMUM_LENGTH
		FROM INFORMATION_SCHEMA.COLUMNS (.+)`,
	).WillReturnRows(mockInfoRows)
	schemas, err := SQLDatabase2AVRO(db, "blog")
	if err != nil {
		t.Error(err)
	}
	var (
		postsColumns = []string{
			"ID",
			"title",
			"body",
			"content_type",
			"author",
			"post_datetime",
			"update_datetime",
			"reading_time_minute",
			"daily_average_traffic",
			"post_date",
			"post_time",
			"post_timestamp",
			"some_int64",
			"some_float64",
			"some_float32",
			"update_date",
			"update_time",
			"update_timestamp",
			"some_nullable_int32",
			"some_nullable_int64",
			"some_nullable_float64",
			"some_nullable_float32",
			"some_nullable_blob",
		}
		mockPostsRows  = sqlmock.NewRows(postsColumns)
		postRowsValues = [][]driver.Value{
			{
				42,
				"lorem ipsum",
				[]byte("lorem ipsum etc..."),
				sql.NullString{Valid: false},
				sql.NullString{Valid: true, String: "John Doe"},
				"2009-04-10 00:00:00",
				sql.NullString{Valid: true, String: "2009-04-10 00:00:00"},
				sql.NullString{Valid: true, String: "2.0"},
				"3000.46",
				"2009-04-10",
				"00:00:00",
				1254614400,
				4242,
				4242.4242,
				42.42,
				sql.NullString{Valid: true, String: "2009-04-10"},
				sql.NullString{Valid: true, String: "00:00:00"},
				sql.NullInt64{Valid: true, Int64: 1254614400},
				sql.NullInt64{Valid: true, Int64: 42},
				sql.NullInt64{Valid: true, Int64: 4242},
				sql.NullFloat64{Valid: true, Float64: 4242.4242},
				sql.NullFloat64{Valid: true, Float64: 42.42},
				[]byte("lorem ipsum dolor etc..."),
			},
		}
	)
	for _, rowValues := range postRowsValues {
		mockPostsRows.AddRow(rowValues...)
	}
	mock.ExpectQuery(
		"SELECT (.+) FROM `blog`.`posts`(.*)",
	).WillReturnRows(mockPostsRows)
	dateStr := json.RawMessage(`"1970-01-01"`)
	dateTimeStr := json.RawMessage(`"1970-01-01T00:00:00.0Z"`)
	timeStampStr := json.RawMessage(strconv.FormatInt(0, 10))
	csvBytes, _, err := Query(QueryConfig{
		DB:     db,
		DBName: "blog",
		Schema: &schemas[0],
		Limit:  10,
		Criteria: []Criterion{
			{
				FieldName: "post_date",
				RawLimit:  &dateStr,
			},
			{
				FieldName: "post_datetime",
				RawLimit:  &dateTimeStr,
			},
			{
				FieldName: "update_timestamp",
				RawLimit:  &timeStampStr,
			},
			{
				FieldName: "update_time",
				RawLimit:  nil,
			},
		},
		Output: outputCSV,
	})
	if err != nil {
		t.Error(err)
	}
	expectedCSV := []byte(`ID,title,body,content_type,author,post_datetime,update_datetime,reading_time_minutes,daily_average_traffic,post_date,post_time,post_timestamp,some_int64,some_float64,some_float32,update_date,update_time,update_timestamp,some_nullable_int32,some_nullable_int64,some_nullable_float64,some_nullable_float32,some_nullable_blob
	42,lorem ipsum,lorem ipsum etc...,,John Doe,2009-04-10 00:00:00,2009-04-10 00:00:00,2.0,3000.46,2009-04-10,00:00:00,15cfpc0,4242,4242.4242,42.41999816894531,2009-04-10,00:00:00,1254614400,42,4242,4242.4242,42.42,lorem ipsum dolor etc...`)
	if bytes.EqualFold(csvBytes, expectedCSV) {
		t.Errorf("expected:\n%s\n\ngot:%s\n\n", string(expectedCSV), string(csvBytes))
	}
}
