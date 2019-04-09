package avro

import (
	"github.com/valyala/fastjson"
)

// MapSchema -
type MapSchema struct {
	Type  Type   `json:"type"`
	Value Schema `json:"values"`
}

// TypeName -
func (t *MapSchema) TypeName() Type {
	return TypeMap
}

func translateValueToMapSchema(value *fastjson.Value, additionalTypes ...Type) (Schema, error) {
	if !value.Exists("values") {
		return nil, ErrInvalidSchema
	}
	valueVal := value.Get("values")
	valueSchema, err := translateValue2AnySchema(valueVal, additionalTypes...)
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
