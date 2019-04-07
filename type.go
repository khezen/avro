package avro

import "encoding/json"

// Type - primitive or derived type name as defined below
type Type string

const (
	// Primitve types

	// TypeNull -
	TypeNull Type = "null"
	// TypeBoolean -
	TypeBoolean Type = "boolean"
	// TypeInt32 -
	TypeInt32 Type = "int"
	// TypeInt64 -
	TypeInt64 Type = "long"
	// TypeFloat32 -
	TypeFloat32 Type = "float"
	// TypeFloat64 -
	TypeFloat64 Type = "double"
	// TypeBytes -
	TypeBytes Type = "bytes"
	// TypeString -
	TypeString Type = "string"

	// Complex types

	// TypeUnion -
	TypeUnion Type = "union"
	// TypeRecord -
	TypeRecord Type = "record"
	// TypeArray -
	TypeArray Type = "array"
	// TypeMap -
	TypeMap Type = "map"
	// TypeEnum -
	TypeEnum Type = "enum"
	// TypeFixed -
	TypeFixed Type = "fixed"
)

// Name -
func (t *Type) Name() Type {
	return *t
}

// MarshalJSON -
func (t *Type) MarshalJSON() ([]byte, error) {
	return json.Marshal(t)
}
