package avro

import "errors"

var (
	// ErrUnsupportedType - Avro doesn't support the given type
	ErrUnsupportedType = errors.New("ErrUnsupportedType - AVRO doesn't support the given type")
	// ErrInvalidSchema - Avro doesn't support the given type
	ErrInvalidSchema = errors.New("ErrInvalidSchema - Given schema is not AVRO")
)
