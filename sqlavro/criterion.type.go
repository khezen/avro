package sqlavro

import (
	"github.com/khezen/avro"
)

func ensureCriterionTypes(schema *avro.RecordSchema, criteria []Criterion) (err error) {
	var (
		i     int
		match bool
		field avro.RecordFieldSchema
	)
	for i = range criteria {
		match = false
		for _, field = range schema.Fields {
			match = criteria[i].FieldName == field.Name || (len(field.Aliases) > 0 && field.Aliases[0] == field.Name)
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
