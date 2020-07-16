package sqlavro

import (
	"github.com/khezen/avro"
)

// EnsureCriterionTypes - search the given schema to find & set criteria types
func EnsureCriterionTypes(schema *avro.RecordSchema, criteria []Criterion) (err error) {
	var (
		i     int
		match bool
		field avro.RecordFieldSchema
	)
	for i = range criteria {
		match = false
		for _, field = range schema.Fields {
			match = criteria[i].FieldName == field.Name || (len(field.Aliases) > 0 && field.Aliases[0] == criteria[i].FieldName)
			if match {
				criteria[i].setSchema(field)
				break
			}
		}
		if !match {
			return ErrCriterionUnknownField
		}
	}
	return nil
}
