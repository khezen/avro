package avro

import (
	"github.com/valyala/fastjson"
)

// FixedSchema -
type FixedSchema struct {
	Type          Type     `json:"type"`
	Name          string   `json:"name"`
	Namespace     string   `json:"namespace,omitempty"`
	Aliases       []string `json:"aliases,omitempty"`
	Documentation string   `json:"doc,omitempty"`
	Size          int      `json:"size"`
}

// TypeName -
func (t *FixedSchema) TypeName() Type {
	return TypeFixed
}

func translateValueToFixedSchema(value *fastjson.Value, additionalTypes ...Type) (Schema, error) {
	if !value.Exists("size") {
		return nil, ErrInvalidSchema
	}
	size, err := value.Get("size").Int()
	if err != nil {
		return nil, ErrInvalidSchema
	}
	namespace, name, documentation, aliases, err := translateValueToMetaFields(value)
	if err != nil {
		return nil, err
	}
	return &FixedSchema{
		Type:          TypeFixed,
		Namespace:     namespace,
		Name:          name,
		Aliases:       aliases,
		Documentation: documentation,
		Size:          size,
	}, nil
}
