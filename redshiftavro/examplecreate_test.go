package redshiftavro_test

import (
	"encoding/json"
	"fmt"

	"github.com/khezen/avro"
	"github.com/khezen/avro/redshiftavro"
)

// ExampleCreateTableStatement -
func ExampleCreateTableStatement() {
	schemaBytes := []byte(`
	{
        "type": "record",
        "namespace": "blog",
        "name": "posts",
        "fields": [
            {
                "name": "ID",
                "type": "int"
            },
            {
                "name": "title",
                "type": "string"
            },
            {
                "name": "body",
                "type": "bytes"
            },
            {
                "name": "content_type",
                "type": [
                    "string",
                    "null"
                ],
                "default": "text/markdown; charset=UTF-8"
            },
            {
                "name": "post_date",
                "type": {
                    "type": "int",
                    "doc":"datetime",
                    "logicalType": "timestamp"
                }
            },
            {
                "name": "update_date",
                "type": [
                    "null",
                    {
                        "type": "int",
                        "doc":"datetime",
                        "logicalType": "timestamp"
                    }
                ]
            },
            {
                "name": "reading_time_minutes",
                "type": [
                    "null",
                    {
                        "type": "bytes",
                        "logicalType": "decimal",
                        "precision": 3,
                        "scale": 1
                    }
                ]
            }
        ]
	}`)
	var anySchema avro.AnySchema
	err := json.Unmarshal(schemaBytes, &anySchema)
	if err != nil {
		panic(err)
	}
	schema := anySchema.Schema().(*avro.RecordSchema)
	cfg := redshiftavro.CreateConfig{
		Schema:      *schema,
		SortKeys:    []string{"post_date", "title"},
		IfNotExists: true,
	}
	statement, err := redshiftavro.CreateTableStatement(cfg)
	if err != nil {
		panic(err)
	}
	fmt.Println(statement)

	// CREATE TABLE IF NOT EXISTS posts(
	// 	ID INTEGER ENCODE LZO NOT NULL,
	// 	title VARCHAR(65535) ENCODE RAW NOT NULL,
	// 	body VARCHAR(65535) ENCODE ZSTD NOT NULL,
	// 	content_type VARCHAR(65535) ENCODE ZSTD NULL,
	// 	post_date TIMESTAMP WITHOUT TIME ZONE ENCODE RAW NOT NULL,
	// 	update_date TIMESTAMP WITHOUT TIME ZONE ENCODE LZO NULL,
	// 	reading_time_minutes DECIMAL(3,1) ENCODE RAW NULL
	// )
	// SORTKEY(
	// 	post_date,
	// 	title
	// )
}
