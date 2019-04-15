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
	case avro.TypeString, avro.Type(avro.LogicalTypeDate), avro.Type(avro.LogicalTypeTime), avro.Type(avro.LogicalTypeTimestamp):
		var field string
		return &field, nil
	case avro.TypeBytes, avro.TypeFixed, avro.Type(avro.LogicalTypeDecimal):
		var field []byte
		return &field, nil
	case avro.TypeUnion:
		types := schema.(avro.UnionSchema)
		isNullable := false
		var typeName avro.Type
		if len(types) > 2 {
			return nil, ErrUnsupportedTypeForSQL
		}
		for _, t := range types {
			if t.TypeName() == avro.TypeNull {
				isNullable = true
			} else {
				typeName = t.TypeName()
			}
		}
		if !isNullable {
			return nil, ErrUnsupportedTypeForSQL
		}
		switch typeName {
		case avro.TypeFloat32, avro.TypeFloat64:
			var field sql.NullFloat64
			return &field, nil
		case avro.TypeInt32, avro.TypeInt64:
			var field sql.NullInt64
			return &field, nil
		case avro.TypeString, avro.Type(avro.LogicalTypeDate), avro.Type(avro.LogicalTypeTime), avro.Type(avro.LogicalTypeTimestamp):
			var field sql.NullString
			return &field, nil
		case avro.TypeBytes, avro.TypeFixed, avro.Type(avro.LogicalTypeDecimal):
			var field []byte
			return &field, nil
		}
		return nil, ErrUnsupportedTypeForSQL
	}
	return nil, ErrUnsupportedTypeForSQL
}

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
	switch schema.TypeName() {
	case avro.TypeInt64:
		return *sqlField.(*int64), nil
	case avro.TypeInt32:
		return *sqlField.(*int32), nil
	case avro.Type(avro.LogicalTypeDate):
		// TODO
	case avro.Type(avro.LogicalTypeTime):
		// TODO
	case avro.Type(avro.LogicalTypeTimestamp):
		// TODO
	case avro.TypeFloat64:
		return *sqlField.(*float64), nil
	case avro.TypeFloat32:
		return *sqlField.(*float32), nil
	case avro.TypeString:
		return *sqlField.(*string), nil
	case avro.TypeBytes, avro.TypeFixed, avro.Type(avro.LogicalTypeDecimal):
		return *sqlField.(*[]byte), nil
	case avro.TypeUnion:
		types := schema.(avro.UnionSchema)
		isNullable := false
		var typeName avro.Type
		if len(types) > 2 {
			return nil, ErrUnsupportedTypeForSQL
		}
		for _, t := range types {
			if t.TypeName() == avro.TypeNull {
				isNullable = true
			} else {
				typeName = t.TypeName()
			}
		}
		if !isNullable {
			return nil, ErrUnsupportedTypeForSQL
		}
		switch typeName {
		case avro.TypeInt64:
			nullableField := sqlField.(*sql.NullInt64)
			if nullableField.Valid {
				return map[string]interface{}{string(typeName): nullableField.Int64}, nil
			}
			return nil, nil
		case avro.TypeInt32:
			nullableField := sqlField.(*sql.NullInt64)
			if nullableField.Valid {
				return map[string]interface{}{string(typeName): int32(nullableField.Int64)}, nil
			}
			return nil, nil
		case avro.TypeFloat64:
			nullableField := sqlField.(*sql.NullFloat64)
			if nullableField.Valid {
				return map[string]interface{}{string(typeName): nullableField.Float64}, nil
			}
			return nil, nil
		case avro.TypeFloat32:
			nullableField := sqlField.(*sql.NullFloat64)
			if nullableField.Valid {
				return map[string]interface{}{string(typeName): float32(nullableField.Float64)}, nil
			}
			return nil, nil
		case avro.TypeString:
			nullableField := sqlField.(*sql.NullString)
			if nullableField.Valid {
				return map[string]interface{}{string(typeName): nullableField.String}, nil
			}
			return nil, nil
		case avro.TypeBytes, avro.TypeFixed, avro.Type(avro.LogicalTypeDecimal):
			field := *sqlField.(*[]byte)
			if field != nil {
				return map[string]interface{}{string(typeName): field}, nil
			}
			return nil, nil
		}
		return nil, ErrUnsupportedTypeForSQL
	}
	return nil, ErrUnsupportedTypeForSQL
}
