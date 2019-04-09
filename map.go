package avro

import (
	"encoding/json"

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

// MarshalJSON -
func (t *MapSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(t)
}

func translateValueToMapSchema(value *fastjson.Value) (Schema, error) {

	return nil, nil
}

// NewMapSchema -
func NewMapSchema(value Schema) *MapSchema {
	return &MapSchema{
		Type:  TypeMap,
		Value: value,
	}
}
