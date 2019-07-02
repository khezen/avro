package redshiftavro

import "github.com/khezen/avro"

// CreateConfig -
type CreateConfig struct {
	Schema      avro.RecordSchema
	SortKeys    []SortKey
	distKey     *DistKey
	IfNotExists bool
}

// DistKey - Distribution key
type DistKey struct {
	Column    string
	distStyle DistStyle
}

// DistStyle -
type DistStyle string

// SortKey -
type SortKey struct {
	Column    string
	sortStyle SortStyle
}

// SortStyle -
type SortStyle string

// RedshiftType -
type RedshiftType string
