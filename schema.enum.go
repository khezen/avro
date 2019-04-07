package avro

import "encoding/json"

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

// MarshalJSON -
func (t *EnumSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(t)
}
