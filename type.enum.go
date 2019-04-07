package avro

import "encoding/json"

// Enum -
type Enum struct {
	Type          Type     `json:"type"`
	Name          string   `json:"name"`
	Namespace     string   `json:"namespace,omitempty"`
	Aliases       []string `json:"aliases,omitempty"`
	Documentation string   `json:"doc,omitempty"`
	Symbols       []string `json:"symbols"`
}

// TypeName -
func (t *Enum) TypeName() Type {
	return TypeEnum
}

// MarshalJSON -
func (t *Enum) MarshalJSON() ([]byte, error) {
	return json.Marshal(t)
}
