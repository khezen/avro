package avro

import (
	"github.com/valyala/fastjson"
)

// ArraySchema -
type ArraySchema struct {
	Type  Type   `json:"type"`
	Items Schema `json:"items"`
}

// TypeName -
func (t *ArraySchema) TypeName() Type {
	return TypeArray
}

func translateValue2ArraySchema(value *fastjson.Value, additionalTypes ...Type) (Schema, error) {
	if !value.Exists("items") {
		return nil, ErrInvalidSchema
	}
	itemsVal := value.Get("items")
	itemSchema, err := translateValue2AnySchema(itemsVal, additionalTypes...)
	if err != nil {
		return nil, err
	}
	return &ArraySchema{
		Type:  TypeArray,
		Items: itemSchema,
	}, nil
}

// NewArraySchema -
func NewArraySchema(items Schema) *ArraySchema {
	return &ArraySchema{
		Type:  TypeArray,
		Items: items,
	}
}
