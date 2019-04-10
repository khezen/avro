package avro_test

import (
	"encoding/json"
	"fmt"

	"github.com/khezen/avro"
)

func ExampleSchema() {
	schemaBytes := []byte(
		`{
		"type": "record",
		"namespace": "test",
		"name": "LongList",
		"aliases": [
			"LinkedLongs"
		],
		"doc": "linked list of 64 bits integers",
		"fields": [
			{
				"name": "value",
				"type": "long"
			},
			{
				"name": "next",
				"type": [
					"null",
					"LongList"
				]
			}
		]
	}`)

	// Unmarshal JSON  bytes to Schema interface
	var anySchema avro.AnySchema
	err := json.Unmarshal(schemaBytes, &anySchema)
	if err != nil {
		panic(err)
	}
	schema := anySchema.Schema()

	// Marshal Schema interface to JSON bytes
	schemaBytes, err = json.Marshal(schema)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(schemaBytes))
}

// {"type":"record","namespace":"test","name":"LongList","aliases":["LinkedLongs"],"doc":"linked list of 64 bits integers","fields":[{"name":"value","type":"long"},{"name":"next","type":["null","LongList"]}]}
