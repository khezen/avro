package sqlavro

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/khezen/avro"
	"github.com/linkedin/goavro"
	"github.com/valyala/fastjson"
)

var unmarshaller fastjson.Parser

func TestQuery(t *testing.T) {
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
			[]driver.Value{"posts"},
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
			[]driver.Value{"blog", "ID", "INT", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			[]driver.Value{"blog", "title", "VARCHAR", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: true, Int64: 384}},
			[]driver.Value{"blog", "body", "LONGBLOB", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: true, Int64: 4294967295}},
			[]driver.Value{"blog", "content_type", "VARCHAR", "YES", sql.NullString{Valid: true, String: "text/markdown; charset=UTF-8"}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: true, Int64: 384}},
			[]driver.Value{"blog", "post_datetime", "DATETIME", "NO", sql.NullString{Valid: true, String: "CURRENT_TIMESTAMP"}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			[]driver.Value{"blog", "update_datetime", "DATETIME", "YES", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			[]driver.Value{"blog", "reading_time_minutes", "DECIMAL", "YES", sql.NullString{Valid: false}, sql.NullInt64{Valid: true, Int64: 3}, sql.NullInt64{Valid: true, Int64: 1}, sql.NullInt64{Valid: false}},
			[]driver.Value{"blog", "daily_average_traffic", "DECIMAL", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: true, Int64: 14}, sql.NullInt64{Valid: true, Int64: 2}, sql.NullInt64{Valid: false}},

			[]driver.Value{"blog", "post_date", "DATE", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			[]driver.Value{"blog", "post_time", "TIME", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			[]driver.Value{"blog", "post_timestamp", "TIMESTAMP", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			[]driver.Value{"blog", "some_int64", "BIGINT", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			[]driver.Value{"blog", "some_float64", "DOUBLE", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			[]driver.Value{"blog", "some_float32", "FLOAT", "NO", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
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
		`SELECT TABLE_SCHEMA,COLUMN_NAME,DATA_TYPE,IS_NULLABLE,COLUMN_DEFAULT,NUMERIC_PRECISION,NUMERIC_SCALE,CHARACTER_OCTET_LENGTH
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
		}
		mockPostsRows  = sqlmock.NewRows(postsColumns)
		postRowsValues = [][]driver.Value{
			[]driver.Value{
				42,
				"lorem ipsum",
				[]byte("lorem ipsum etc..."),
				sql.NullString{Valid: false},
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
			},
		}
	)
	for _, rowValues := range postRowsValues {
		mockPostsRows.AddRow(rowValues...)
	}
	mock.ExpectQuery(
		"SELECT (.+) FROM `blog`.`posts`(.*)",
	).WillReturnRows(mockPostsRows)
	avroBytes, err := Query(db, &schemas[0], 10, Criterion{
		FieldName: "post_date",
		Type:      avro.Type(avro.LogicalTypeTimestamp),
		RawLimit:  []byte("1970-01-01T00:00:00Z"),
	})
	if err != nil {
		t.Error(err)
	}
	resultSchema := avro.ArraySchema{
		Type:  avro.TypeArray,
		Items: &schemas[0],
	}
	resultSchemaBytes, err := json.Marshal(resultSchema)
	if err != nil {
		panic(err)
	}
	codec, err := goavro.NewCodec(string(resultSchemaBytes))
	if err != nil {
		t.Error(err)
	}
	// Convert binary Avro data back to native Go form
	native, _, err := codec.NativeFromBinary(avroBytes)
	if err != nil {
		fmt.Println(err)
	}
	// Convert native Go form to textual Avro data
	textual, err := codec.TextualFromNative(nil, native)
	if err != nil {
		fmt.Println(err)
	}
	expetedTextual := `[{"reading_time_minutes":{"bytes.decimal":"\u0014"},"update_datetime":{"int":1239321600},"ID":42,"post_time":2764800,"post_date":14344,"daily_average_traffic":"u4","some_float32":42.42,"post_datetime":1239321600,"post_timestamp":1254614400,"content_type":null,"some_float64":4242.4242,"title":"lorem ipsum","body":"lorem ipsum etc...","some_int64":4242}]`
	if !JSONArraysEquals([]byte(expetedTextual), textual) {
		t.Errorf("expected:\n%s\ngot:\n%s\n", string(expetedTextual), string(textual))
	}

}

func JSONArraysEquals(expected, given []byte) bool {
	var expectedArray []map[string]json.RawMessage
	err := json.Unmarshal(expected, &expectedArray)
	if err != nil {
		panic(err)
	}
	var givenArray []map[string]json.RawMessage
	err = json.Unmarshal(given, &givenArray)
	if err != nil {
		panic(err)
	}
	if len(givenArray) != len(expectedArray) {
		return false
	}
	for i := range expectedArray {
		if !JSONObjectEquals(expectedArray[i], givenArray[i]) {
			return false
		}
	}
	return true
}

func JSONObjectEquals(expectedObject, givenObject map[string]json.RawMessage) bool {
	for key := range expectedObject {
		if _, ok := givenObject[key]; !ok {
			return false
		}
		if !bytes.EqualFold(expectedObject[key], givenObject[key]) {
			return false
		}
	}
	for key := range givenObject {
		if _, ok := expectedObject[key]; !ok {
			return false
		}
		if !bytes.EqualFold(expectedObject[key], givenObject[key]) {
			return false
		}
	}
	return true
}
