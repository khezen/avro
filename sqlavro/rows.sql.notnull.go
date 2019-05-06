package sqlavro

import "github.com/khezen/avro"

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
