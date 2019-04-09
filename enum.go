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

	return nil, nil
}
