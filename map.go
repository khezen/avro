package avro

import (
	"github.com/valyala/fastjson"
)

// MapSchema -
type MapSchema struct {
	Type  Type   `json:"type"`
	Value Schema `json:"value"`
}

// TypeName -
func (t *MapSchema) TypeName() Type {
	return TypeMap
}

func translateValueToMapSchema(value *fastjson.Value) (Schema, error) {
	if !value.Exists("value") {
		return nil, ErrInvalidSchema
	}
	valueVal := value.Get("value")
	valueSchema, err := translateValue2AnySchema(valueVal)
	if err != nil {
		return nil, err
	}
	return &MapSchema{
		Type:  TypeMap,
		Value: valueSchema,
	}, nil
}

// NewMapSchema -
func NewMapSchema(value Schema) *MapSchema {
	return &MapSchema{
		Type:  TypeMap,
		Value: value,
	}
}
