package avro

import "encoding/json"

// Schema -
type Schema interface {
	json.Marshaler
	TypeName() Type
}
