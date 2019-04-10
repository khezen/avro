package avro

import (
	"github.com/valyala/fastjson"
)

// EnumSchema -
type EnumSchema struct {
	Type          Type     `json:"type"`
	Name          string   `json:"name"`
	Namespace     string   `json:"namespace,omitempty"`
	Aliases       []string `json:"aliases,omitempty"`
	Documentation string   `json:"doc,omitempty"`
	Symbols       []string `json:"symbols"`
}

// TypeName -
func (t *EnumSchema) TypeName() Type {
	return TypeEnum
}

func translateValueToEnumSchema(value *fastjson.Value, additionalTypes ...Type) (Schema, error) {
	if !value.Exists("symbols") {
		return nil, ErrInvalidSchema
	}
	symbolsValues, err := value.Get("symbols").Array()
	if err != nil {
		return nil, ErrInvalidSchema
	}
	symbolsSchemas := make([]string, 0, len(symbolsValues))
	for _, symbolValue := range symbolsValues {
		symbolSchema, err := symbolValue.StringBytes()
		if err != nil {
			return nil, ErrInvalidSchema
		}
		symbolsSchemas = append(symbolsSchemas, string(symbolSchema))
	}
	namespace, name, documentation, aliases, err := translateValueToMetaFields(value)
	if err != nil {
		return nil, err
	}
	return &EnumSchema{
		Type:          TypeEnum,
		Namespace:     namespace,
		Name:          name,
		Aliases:       aliases,
		Documentation: documentation,
		Symbols:       symbolsSchemas,
	}, nil
}
