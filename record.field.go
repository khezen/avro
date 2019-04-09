package avro

import (
	"encoding/json"

	"github.com/valyala/fastjson"
)

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

func translateValueToRecordFieldSchema(value *fastjson.Value) (*RecordFieldSchema, error) {
	if !value.Exists("type") {
		return nil, ErrInvalidSchema
	}
	anySchema, err := translateValue2AnySchema(value)
	if err != nil {
		return nil, err
	}
	var (
		order        Order
		defaultValue []byte
	)
	if value.Exists("order") {
		order = Order(value.Get("order").String())
	}
	if value.Exists("default") {
		defaultValue = value.GetStringBytes("default")
	}
	_, name, documentation, aliases, err := translateValueToMetaFields(value)
	if err != nil {
		return nil, err
	}
	return &RecordFieldSchema{
		Name:          name,
		Aliases:       aliases,
		Documentation: documentation,
		Type:          anySchema,
		Default:       json.RawMessage(defaultValue),
		Order:         order,
	}, nil
}
