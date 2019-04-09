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
		// {
		// 	[]byte(`{"type":"enum","name":"Suit","symbols":["SPADES","HEARTS","DIAMONDS","CLUBS"]}`),
		// 	nil,
		// },
		// {
		// 	[]byte(`{"type":"fixed","size":16,"name":"md5"}`),
		// 	nil,
		// },
		{
			[]byte(`["null","string"]`),
			nil,
		},
		{
			[]byte(`["something","string"]`),
			ErrUnsupportedType,
		},
		{
			[]byte(`{"type":"map","items":"long"}`),
			ErrInvalidSchema,
		},
		{
			[]byte(`{"type":"array","values":"long"}`),
			ErrInvalidSchema,
		},
		{
			[]byte(`{"type":"record","fields":[{"name":"value","type":"long"}]}`),
			ErrInvalidSchema,
		},
		{
			[]byte(`{"type":"record","name":"LongList","fields":[{"type":"long"}]}`),
			ErrInvalidSchema,
		},
		{
			[]byte(`{"type":"record","name":"LongList","aliases":"something","fields":[{"name":"value","type":"long"}]}`),
			ErrInvalidSchema,
		},
		{
			[]byte(`{"type":"record","name":"LongList","fields":[{"name":"value","aliases":"something","type":"long"}]}`),
			ErrInvalidSchema,
		},
	}
	var (
		anySchema        AnySchema
		underlyingSchema Schema
		schemaBytes      []byte
	)
	for _, c := range cases {
		err := json.Unmarshal(c.schemaBytes, &anySchema)
		if err != nil && err != c.expectedErr {
			panic(err)
		}
		if err != nil {
			continue
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
