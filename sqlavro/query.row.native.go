package sqlavro

import (
	"github.com/khezen/avro"
)

func sqlRow2native(schema *avro.RecordSchema, sqlFields []interface{}) (map[string]interface{}, error) {
	nativeFields := make(map[string]interface{})
	for i, field := range schema.Fields {
		nativeField, err := sqlField2native(field.Type, sqlFields[i])
		if err != nil {
			return nil, err
		}
		nativeFields[field.Name] = nativeField
	}
	return nativeFields, nil
}

func sqlField2native(schema avro.Schema, sqlField interface{}) (interface{}, error) {
	if schema.TypeName() == avro.TypeUnion {
		return sql2NativeFieldNullable(schema, sqlField)
	}
	return sql2NativeFieldNotNull(schema, sqlField)
}
