package avro

import "encoding/json"

// UnionSchema - A JSON array, representing a union of embedded types.
type UnionSchema []Schema

// TypeName -
func (t *UnionSchema) TypeName() Type {
	return TypeUnion
}

// MarshalJSON -
func (t *UnionSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(t)
}
