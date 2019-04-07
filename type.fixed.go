package avro

import "encoding/json"

// Fixed -
type Fixed struct {
	Type          Type     `json:"type"`
	Name          string   `json:"name"`
	Namespace     string   `json:"namespace,omitempty"`
	Aliases       []string `json:"aliases,omitempty"`
	Documentation string   `json:"doc,omitempty"`
	Size          int      `json:"size"`
}

// TypeName -
func (t *Fixed) TypeName() Type {
	return TypeFixed
}

// MarshalJSON -
func (t *Fixed) MarshalJSON() ([]byte, error) {
	return json.Marshal(t)
}
