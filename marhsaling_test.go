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
			t.Errorf("expected:\n%v\ngot:\n%v\n", c.schemaBytes, schemaBytes)
		}
	}
}
