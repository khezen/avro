package sqlavro

import (
	"database/sql"

	"github.com/khezen/avro"
)

func renderSQLFieldNullable(schema avro.Schema) (interface{}, error) {
	union := schema.(avro.UnionSchema)
	subSchema, err := underlyingType(union)
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
