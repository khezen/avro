package avro

import (
	"encoding/json"

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

// MarshalJSON -
func (t *FixedSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(t)
}

func translateValueToFixedSchema(value *fastjson.Value) (Schema, error) {

	return nil, nil
}
