package sqlavro

import "github.com/khezen/avro"

func sqlRow2String(schema *avro.RecordSchema, sqlFields []interface{}) (map[string]string, error) {
	stringFields := make(map[string]string)
	for i, field := range schema.Fields {
		stringField, err := sqlField2String(field.Type, sqlFields[i])
		if err != nil {
			return nil, err
		}
		stringFields[field.Name] = stringField
	}
	return stringFields, nil
}

func sqlField2String(schema avro.Schema, sqlField interface{}) (string, error) {
	if schema.TypeName() == avro.TypeUnion {
		return sql2StringFieldNullable(schema, sqlField)
	}
	return sql2StringFieldNotNull(schema, sqlField)
}
