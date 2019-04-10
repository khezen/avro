package avro

import (
	"github.com/valyala/fastjson"
)

// FixedSchema -
type FixedSchema struct {
	Type          Type        `json:"type"`
	LogicalType   LogicalType `json:"logicalType,omitempty"`
	Name          string      `json:"name"`
	Namespace     string      `json:"namespace,omitempty"`
	Aliases       []string    `json:"aliases,omitempty"`
	Documentation string      `json:"doc,omitempty"`
	Size          int         `json:"size"`
}

// TypeName -
func (t *FixedSchema) TypeName() Type {
	return TypeFixed
}

func translateValueToFixedSchema(value *fastjson.Value) (Schema, error) {
	if !value.Exists("size") {
		return nil, ErrInvalidSchema
	}
	size, err := value.Get("size").Int()
	if err != nil {
		return nil, ErrInvalidSchema
	}
	if size < 0 {
		return nil, ErrInvalidSchema
	}
	namespace, name, documentation, aliases, err := translateValueToMetaFields(value)
	if err != nil {
		return nil, err
	}
	var logicalType LogicalType
	if value.Exists("logicalType") {
		logicalType = LogicalType(value.GetStringBytes("logicalType"))
		if logicalType != LogialTypeDuration || size != 12 {
			return nil, ErrInvalidSchema
		}
	}
	return &FixedSchema{
		Type:          TypeFixed,
		LogicalType:   logicalType,
		Namespace:     namespace,
		Name:          name,
		Aliases:       aliases,
		Documentation: documentation,
		Size:          size,
	}, nil
}
