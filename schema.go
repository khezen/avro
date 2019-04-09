package avro

import (
	"encoding/json"

	"github.com/valyala/fastjson"
)

var unmarshaller fastjson.Parser

// Schema -
type Schema interface {
	json.Marshaler
	TypeName() Type
}

// AnySchema -
type AnySchema struct {
	schema Schema
}

// UnmarshalJSON -
func (as *AnySchema) UnmarshalJSON(bytes []byte) error {
	value, err := unmarshaller.Parse(string(bytes))
	if err != nil {
		return err
	}
	schema, err := translateValue2AnySchema(value)
	if err != nil {
		return err
	}
	as.schema = schema
	return nil
}

func translateValue2AnySchema(value *fastjson.Value) (Schema, error) {

	return nil, nil
}

// Schema returns the unmarshalled schema
func (as *AnySchema) Schema() Schema {
	return as.schema
}
