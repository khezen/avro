package sqlavro

import "github.com/khezen/avro"

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
				err = ensureCriterionType(&field, &criteria[i])
				if err != nil {
					return err
				}
				break
			}
		}
		if !match {
			return ErrCriterionUnknownField
		}
	}
	return nil
}

func ensureCriterionType(field *avro.RecordFieldSchema, criterion *Criterion) (err error) {
	var schema = field.Type
	if schema.TypeName() == avro.TypeUnion {
		union := schema.(avro.UnionSchema)
		schema, err = underlyingType(union)
		if err != nil {
			return err
		}
	}
	switch schema.TypeName() {
	case avro.TypeInt64, avro.TypeInt32,
		avro.TypeFloat64, avro.TypeFloat32,
		avro.TypeString,
		avro.Type(avro.LogicalTypeDate),
		avro.Type(avro.LogicalTypeTime):
		criterion.setType(schema.TypeName())
		break
	case avro.Type(avro.LogicalTypeTimestamp):
		switch schema.(*avro.DerivedPrimitiveSchema).Documentation {
		case string(DateTime):
			criterion.setType(avro.Type(avro.LogicalTypeTimestamp))
			break
		case "", string(Timestamp):
			criterion.setType(avro.Type(avro.TypeInt64))
			break
		default:
			return ErrUnsupportedTypeForCriterion
		}
		break
	default:
		return ErrUnsupportedTypeForCriterion
	}
	return nil
}
