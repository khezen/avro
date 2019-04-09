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

// TypeName -
func (t *RecordSchema) TypeName() Type {
	return TypeRecord
}

// MarshalJSON -
func (t *RecordSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(t)
}

func translateValueToRecordSchema(value *fastjson.Value) (Schema, error) {
	if !value.Exists("fields") {
		return nil, ErrInvalidSchema
	}
	fieldValues, err := value.Get("fields").Array()
	if err != nil {
		return nil, ErrInvalidSchema
	}
	fieldSchemas := make([]RecordFieldSchema, 0, len(fieldValues))
	for _, fieldValue := range fieldValues {
		fieldSchema, err := translateValueToRecordFieldSchema(fieldValue)
		if err != nil {
			return nil, err
		}
		fieldSchemas = append(fieldSchemas, *fieldSchema)
	}
	namespace, name, documentation, aliases, err := translateValueToMetaFields(value)
	if err != nil {
		return nil, err
	}
	return &RecordSchema{
		Type:          TypeRecord,
		Namespace:     namespace,
		Name:          name,
		Aliases:       aliases,
		Documentation: documentation,
		Fields:        fieldSchemas,
	}, nil
}
