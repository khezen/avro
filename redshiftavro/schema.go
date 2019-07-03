package redshiftavro

import "github.com/khezen/avro"

// CreateConfig -
type CreateConfig struct {
	Schema      avro.RecordSchema
	SortKeys    []string
	SortStyle   SortStyle
	DistKey     *string
	DistStyle   DistStyle
	IfNotExists bool
}

// DistStyle -
type DistStyle string

// SortStyle -
type SortStyle string

// RedshiftType -
type RedshiftType string

// RedshiftEncoding -
type RedshiftEncoding string
