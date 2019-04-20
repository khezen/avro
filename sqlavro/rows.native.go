package sqlavro

import (
	"github.com/khezen/avro"
)

func renderNativeRecord(schema *avro.RecordSchema, sqlFields []interface{}) (map[string]interface{}, error) {
	nativeFields := make(map[string]interface{})
	for i, field := range schema.Fields {
		nativeField, err := renderNativeField(field.Type, sqlFields[i])
		if err != nil {
			return nil, err
		}
		nativeFields[field.Name] = nativeField
	}
	return nativeFields, nil
}

func renderNativeField(schema avro.Schema, sqlField interface{}) (interface{}, error) {
	if schema.TypeName() == avro.TypeUnion {
		return renderNativeFieldNullable(schema, sqlField)
	}
	return renderNativeFieldNotNull(schema, sqlField)
}
