package avro

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestMarshaling(t *testing.T) {
	cases := []struct {
		schemaBytes []byte
		expectedErr error
	}{
		{
			[]byte(`{"type":"record","name":"LongList","aliases":["LinkedLongs"],"fields":[{"name":"value","type":"long"}]}`),
			nil,
		}, {
			[]byte(`{"type":"record","name":"LongList","aliases":["LinkedLongs"],"fields":[{"name":"value","type":"long"},{"name":"next","type":["null","LongList"]}]}`),
			nil,
		},
		// {
		// 	[]byte(`{"type":"enum","name":"Suit","symbols":["SPADES","HEARTS","DIAMONDS","CLUBS"]}`),
		// 	nil,
		// },
		{
			[]byte(`{"type":"array","items":"string"}`),
			nil,
		},
		{
			[]byte(`{"type":"array","items":["null","string"]}`),
			nil,
		},
		{
			[]byte(`{"type":"map","values":"long"}`),
			nil,
		},
		{
			[]byte(`{"type":"map","values":["null","long"]}`),
			nil,
		},
	}
	var (
		anySchema        AnySchema
		underlyingSchema Schema
		schemaBytes      []byte
	)
	for _, c := range cases {
		err := json.Unmarshal(c.schemaBytes, &anySchema)
		if err != nil {
			panic(err)
		}
		underlyingSchema = anySchema.Schema()
		schemaBytes, err = json.Marshal(underlyingSchema)
		if err != nil {
			panic(err)
		}
		if !bytes.EqualFold(schemaBytes, c.schemaBytes) {
			t.Errorf("expected:\n%s\ngot:\n%s\n", c.schemaBytes, schemaBytes)
		}
	}
}
