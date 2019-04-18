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
			[]driver.Value{"blog", "post_date", "DATETIME", "NO", sql.NullString{Valid: true, String: "CURRENT_TIMESTAMP"}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			[]driver.Value{"blog", "update_date", "DATETIME", "YES", sql.NullString{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}, sql.NullInt64{Valid: false}},
			[]driver.Value{"blog", "reading_time_minutes", "DECIMAL", "YES", sql.NullString{Valid: false}, sql.NullInt64{Valid: true, Int64: 3}, sql.NullInt64{Valid: true, Int64: 1}, sql.NullInt64{Valid: false}},
		}
	)
	for _, rowValues := range infoRowsValues {
		mockInfoRows.AddRow(rowValues...)
	}
	mock.ExpectQuery(
		`SELECT TABLE_SCHEMA,COLUMN_NAME,DATA_TYPE,IS_NULLABLE,COLUMN_DEFAULT,NUMERIC_PRECISION,NUMERIC_SCALE,CHARACTER_OCTET_LENGTH
		FROM INFORMATION_SCHEMA.COLUMNS (.+)`,
	).WillReturnRows(mockInfoRows)
	schema, err := SQLTable2AVRO(db, "blog", "posts")
	if err != nil {
		t.Error(err)
	}
	var (
		postsColumns = []string{
			"ID",
			"title",
			"body",
			"content_type",
			"post_date",
			"update_date",
			"reading_time_minute",
		}
		mockPostsRows  = sqlmock.NewRows(postsColumns)
		postRowsValues = [][]driver.Value{
			[]driver.Value{42, "lorem ipsum", []byte("lorem ipsum etc..."), sql.NullString{Valid: false}, "2009-04-10 00:00:00", sql.NullString{Valid: false}, sql.NullString{Valid: true, String: "2.0"}},
		}
	)
	for _, rowValues := range postRowsValues {
		mockPostsRows.AddRow(rowValues...)
	}
	mock.ExpectQuery(
		"SELECT (.+) FROM `blog`.`posts`(.*)",
	).WillReturnRows(mockPostsRows)
	avroBytes, err := Query(db, schema, 10, Criterion{
		FieldName: "post_date",
		Type:      avro.Type(avro.LogicalTypeTimestamp),
		RawLimit:  []byte("1970-01-01T00:00:00Z"),
	})
	if err != nil {
		t.Error(err)
	}
	resultSchema := avro.ArraySchema{
		Type:  avro.TypeArray,
		Items: schema,
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
	expetedTextual := `[{"update_date":null,"reading_time_minutes":{"bytes.decimal":"\u0014"},"ID":42,"title":"lorem ipsum","body":"lorem ipsum etc...","content_type":null,"post_date":1239321600}]`
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
