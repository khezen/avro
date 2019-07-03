package sqlavro

import (
	"database/sql"

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

func renderSQLFieldNotNull(schema avro.Schema) (interface{}, error) {
	switch schema.TypeName() {
	case avro.TypeInt64:
		var field int64
		return &field, nil
	case avro.TypeInt32:
		var field int32
		return &field, nil
	case avro.TypeFloat64:
		var field float64
		return &field, nil
	case avro.TypeFloat32:
		var field float32
		return &field, nil
	case avro.TypeString, avro.Type(avro.LogicalTypeDate), avro.Type(avro.LogicalTypeTime):
		var field string
		return &field, nil
	case avro.Type(avro.LogicalTypeTimestamp):
		return renderSQLTimestamp(schema)
	case avro.TypeBytes, avro.TypeFixed, avro.Type(avro.LogicalTypeDecimal):
		var field []byte
		return &field, nil
	}
	return nil, ErrUnsupportedTypeForSQL
}

func renderSQLTimestamp(schema avro.Schema) (interface{}, error) {
	switch schema.(*avro.DerivedPrimitiveSchema).Documentation {
	case string(DateTime):
		var field string
		return &field, nil
	case "", string(Timestamp):
		var field int32
		return &field, nil
	default:
		return nil, ErrUnsupportedTypeForSQL
	}
}

func renderSQLFieldNullable(schema avro.Schema) (interface{}, error) {
	union := schema.(avro.UnionSchema)
	subSchema, err := UnderlyingType(union)
	if err != nil {
		return nil, err
	}
	switch subSchema.TypeName() {
	case avro.TypeFloat32, avro.TypeFloat64:
		var field sql.NullFloat64
		return &field, nil
	case avro.TypeInt32, avro.TypeInt64:
		var field sql.NullInt64
		return &field, nil
	case avro.TypeString, avro.Type(avro.LogicalTypeDate), avro.Type(avro.LogicalTypeTime):
		var field sql.NullString
		return &field, nil
	case avro.Type(avro.LogicalTypeTimestamp):
		return renderSQLTimestampNullable(subSchema)
	case avro.TypeBytes, avro.TypeFixed, avro.Type(avro.LogicalTypeDecimal):
		var field []byte
		return &field, nil
	}
	return nil, ErrUnsupportedTypeForSQL
}

func renderSQLTimestampNullable(schema avro.Schema) (interface{}, error) {
	switch schema.(*avro.DerivedPrimitiveSchema).Documentation {
	case string(DateTime):
		var field sql.NullString
		return &field, nil
	case "", string(Timestamp):
		var field sql.NullInt64
		return &field, nil
	default:
		return nil, ErrUnsupportedTypeForSQL
	}
}
