package avro

import (
	"github.com/valyala/fastjson"
)

// RecordSchema has fields
type RecordSchema struct {
	Type          Type                `json:"type"`
	Namespace     string              `json:"namespace,omitempty"`
	Name          string              `json:"name"`
	Aliases       []string            `json:"aliases,omitempty"`
	Documentation string              `json:"doc,omitempty"`
	Fields        []RecordFieldSchema `json:"fields"`
}

// TypeName -
func (t *RecordSchema) TypeName() Type {
	return TypeRecord
}

func translateValueToRecordSchema(value *fastjson.Value, additionalTypes ...Type) (Schema, error) {
	if !value.Exists("fields") {
		return nil, ErrInvalidSchema
	}
	fieldValues, err := value.Get("fields").Array()
	if err != nil {
		return nil, ErrInvalidSchema
	}
	namespace, name, documentation, aliases, err := translateValueToMetaFields(value)
	if err != nil {
		return nil, err
	}
	fieldSchemas := make([]RecordFieldSchema, 0, len(fieldValues))
	for _, fieldValue := range fieldValues {
		fieldSchema, err := translateValueToRecordFieldSchema(fieldValue, append(additionalTypes, Type(name))...)
		if err != nil {
			return nil, err
		}
		fieldSchemas = append(fieldSchemas, *fieldSchema)
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
