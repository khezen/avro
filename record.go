package avro

import (
	"encoding/json"

	"github.com/valyala/fastjson"
)

// RecordSchema has fields
type RecordSchema struct {
	Type          Type                `json:"type"`
	Name          string              `json:"name"`
	Namespace     string              `json:"namespace,omitempty"`
	Aliases       []string            `json:"aliases,omitempty"`
	Documentation string              `json:"doc,omitempty"`
	Fields        []RecordFieldSchema `json:"fields"`
}

// RecordFieldSchema -
type RecordFieldSchema struct {
	Name          string          `json:"name"`
	Aliases       []string        `json:"aliases,omitempty"`
	Documentation string          `json:"doc,omitempty"`
	Type          Schema          `json:"type"`
	Default       json.RawMessage `json:"default,omitmepty"`
	Order         Order           `json:"order,omitempty"`
}

// Order - specifies how this field impacts sort ordering of this record (optional).
// Valid values are "ascending" (the default), "descending", or "ignore".
type Order string

const (
	// Ascending -
	Ascending Order = "ascending"
	// Descending -
	Descending Order = "descending"
	// Ignore -
	Ignore Order = "ignore"
)

// TypeName -
func (t *RecordSchema) TypeName() Type {
	return TypeRecord
}

// MarshalJSON -
func (t *RecordSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(t)
}

func translateValueToRecordSchema(value *fastjson.Value) (Schema, error) {

	return nil, nil
}
