package avro

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestMarshaling(t *testing.T) {
	cases := []struct {
		typeName    Type
		schemaBytes []byte
		expectedErr error
	}{
		{
			TypeRecord,
			[]byte(`{"type":"record","namespace":"test","name":"LongList","aliases":["LinkedLongs"],"doc":"list of 64 bits integers","fields":[{"name":"value","type":"long"}]}`),
			nil,
		},
		{
			TypeRecord,
			[]byte(`{"type":"record","name":"LongList","aliases":["LinkedLongs"],"fields":[{"name":"value","type":"long"}]}`),
			nil,
		},
		{
			TypeRecord,
			[]byte(`{"type":"record","name":"LongList","aliases":["LinkedLongs"],"fields":[{"name":"value","type":"long"},{"name":"next","type":["null","LongList"]}]}`),
			nil,
		},
		{
			TypeArray,
			[]byte(`{"type":"array","items":"string"}`),
			nil,
		},
		{
			TypeArray,
			[]byte(`{"type":"array","items":["null","string"]}`),
			nil,
		},
		{
			TypeMap,
			[]byte(`{"type":"map","values":"long"}`),
			nil,
		},
		{
			TypeMap,
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
			TypeUnion,
			[]byte(`["null","string"]`),
			nil,
		},
		{
			TypeUnion,
			[]byte(`["something","string"]`),
			ErrUnsupportedType,
		},
		{
			TypeMap,
			[]byte(`{"type":"map","items":"long"}`),
			ErrInvalidSchema,
		},
		{
			TypeArray,
			[]byte(`{"type":"array","values":"long"}`),
			ErrInvalidSchema,
		},
		{
			TypeRecord,
			[]byte(`{"type":"record","fields":[{"name":"value","type":"long"}]}`),
			ErrInvalidSchema,
		},
		{
			TypeRecord,
			[]byte(`{"type":"record","name":"LongList","fields":[{"type":"long"}]}`),
			ErrInvalidSchema,
		},
		{
			TypeRecord,
			[]byte(`{"type":"record","name":"LongList","aliases":"something","fields":[{"name":"value","type":"long"}]}`),
			ErrInvalidSchema,
		},
		{
			TypeRecord,
			[]byte(`{"type":"record","name":"LongList","fields":[{"name":"value","aliases":"something","type":"long"}]}`),
			ErrInvalidSchema,
		},
		{
			TypeRecord,
			[]byte(`{"type":"record","name":"LongList"}`),
			ErrInvalidSchema,
		},
		{
			TypeRecord,
			[]byte(`{"type":"record","name":"LongList","fields":"something"}`),
			ErrInvalidSchema,
		},
		{
			TypeRecord,
			[]byte(`{"type":"record","name":"LongList","aliases":[0],"fields":[{"name":"value","type":"long"}]}`),
			ErrInvalidSchema,
		},
	}
	var (
		anySchema        AnySchema
		underlyingSchema Schema
		schemaBytes      []byte
	)
	for i, c := range cases {
		err := json.Unmarshal(c.schemaBytes, &anySchema)
		if err != nil && err != c.expectedErr {
			panic(err)
		}
		if err != nil {
			continue
		}
		underlyingSchema = anySchema.Schema()
		if underlyingSchema.TypeName() != c.typeName {
			t.Errorf("case %d - expected:%s got:%s", i, c.typeName, underlyingSchema.TypeName())
		}
		schemaBytes, err = json.Marshal(underlyingSchema)
		if err != nil {
			panic(err)
		}
		if !bytes.EqualFold(schemaBytes, c.schemaBytes) {
			t.Errorf("case %d -\nexpected:\n%s\ngot:\n%s\n", i, c.schemaBytes, schemaBytes)
		}
	}
}
