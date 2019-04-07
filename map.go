package avro

import "encoding/json"

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

// NewMapSchema -
func NewMapSchema(value Schema) *MapSchema {
	return &MapSchema{
		Type:  TypeMap,
		Value: value,
	}
}
