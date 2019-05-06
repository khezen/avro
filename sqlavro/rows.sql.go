package sqlavro

import (
	"github.com/khezen/avro"
)

func renderSQLFields(schema *avro.RecordSchema) ([]interface{}, error) {
	sqlFields := make([]interface{}, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		sqlField, err := renderSQLField(field.Type)
		if err != nil {
			return nil, err
		}
		sqlFields = append(sqlFields, sqlField)
	}
	return sqlFields, nil
}

func renderSQLField(schema avro.Schema) (interface{}, error) {
	if schema.TypeName() == avro.TypeUnion {
		return renderSQLFieldNullable(schema)
	}
	return renderSQLFieldNotNull(schema)
}
