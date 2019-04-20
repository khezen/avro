package sqlavro

import (
	"database/sql"
	"fmt"
	"math/big"
	"time"

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
		return renderSQLFieldUnion(schema)
	}
	return renderSQLFieldSingle(schema)
}

func renderSQLFieldSingle(schema avro.Schema) (interface{}, error) {
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
	case avro.TypeBytes, avro.TypeFixed, avro.Type(avro.LogicalTypeDecimal):
		var field []byte
		return &field, nil
	}
	return nil, ErrUnsupportedTypeForSQL
}

func renderSQLFieldUnion(schema avro.Schema) (interface{}, error) {
	types := schema.(avro.UnionSchema)
	isNullable := false
	var subSchema avro.Schema
	if len(types) > 2 {
		return nil, ErrUnsupportedTypeForSQL
	}
	for _, t := range types {
		if t.TypeName() == avro.TypeNull {
			isNullable = true
		} else {
			subSchema = t
		}
	}
	if !isNullable {
		return nil, ErrUnsupportedTypeForSQL
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
		switch subSchema.(*avro.DerivedPrimitiveSchema).Documentation {
		case string(DateTime):
			var field sql.NullString
			return &field, nil
		case "", string(Timestamp):
			var field sql.NullInt64
			return &field, nil
		default:
			return nil, ErrUnsupportedTypeForSQL
		}
	case avro.TypeBytes, avro.TypeFixed, avro.Type(avro.LogicalTypeDecimal):
		var field []byte
		return &field, nil
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
		timeStr := *sqlField.(*string)
		t, err := time.Parse(SQLDateFormat, timeStr)
		if err != nil {
			return nil, err
		}
		return t, nil
	case avro.Type(avro.LogicalTypeTime):
		timeStr := *sqlField.(*string)
		t, err := time.Parse(SQLTimeFormat, timeStr)
		if err != nil {
			return nil, err
		}
		t = t.AddDate(1970, 1, 1)
		return int32(t.Unix()), nil
	case avro.Type(avro.LogicalTypeTimestamp):
		switch schema.(*avro.DerivedPrimitiveSchema).Documentation {
		case string(DateTime):
			timeStr := *sqlField.(*string)
			t, err := time.Parse(SQLDateTimeFormat, timeStr)
			if err != nil {
				return nil, err
			}
			return int32(t.Unix()), nil
		case "", string(Timestamp):
			return *sqlField.(*int32), nil
		default:
			return nil, ErrUnsupportedTypeForSQL
		}
	case avro.TypeFloat64:
		return *sqlField.(*float64), nil
	case avro.TypeFloat32:
		return *sqlField.(*float32), nil
	case avro.TypeString:
		return *sqlField.(*string), nil
	case avro.TypeBytes, avro.TypeFixed:
		return *sqlField.(*[]byte), nil
	case avro.Type(avro.LogicalTypeDecimal):
		field := *sqlField.(*[]byte)
		r := new(big.Rat)
		_, err := fmt.Sscan(string(field), r)
		if err != nil {
			return nil, err
		}
		return r, nil
	case avro.TypeUnion:
		types := schema.(avro.UnionSchema)
		isNullable := false
		var subSchema avro.Schema
		if len(types) > 2 {
			return nil, ErrUnsupportedTypeForSQL
		}
		for _, t := range types {
			if t.TypeName() == avro.TypeNull {
				isNullable = true
			} else {
				subSchema = t
			}
		}
		if !isNullable {
			return nil, ErrUnsupportedTypeForSQL
		}
		switch subSchema.TypeName() {
		case avro.TypeInt64:
			nullableField := sqlField.(*sql.NullInt64)
			if nullableField.Valid {
				return map[string]interface{}{string(subSchema.TypeName()): nullableField.Int64}, nil
			}
			return nil, nil
		case avro.TypeInt32:
			nullableField := sqlField.(*sql.NullInt64)
			if nullableField.Valid {
				return map[string]interface{}{string(subSchema.TypeName()): int32(nullableField.Int64)}, nil
			}
			return nil, nil
		case avro.Type(avro.LogicalTypeDate):
			nullableField := sqlField.(*sql.NullString)
			if nullableField.Valid {
				t, err := time.Parse(SQLDateFormat, nullableField.String)
				if err != nil {
					return nil, err
				}
				return map[string]interface{}{"int.date": t}, nil
			}
			return nil, nil
		case avro.Type(avro.LogicalTypeTime):
			nullableField := sqlField.(*sql.NullString)
			if nullableField.Valid {
				t, err := time.Parse(SQLTimeFormat, nullableField.String)
				if err != nil {
					return nil, err
				}
				t = t.AddDate(1970, 1, 1)
				return map[string]interface{}{"int": int32(t.Unix())}, nil
			}
			return nil, nil
		case avro.Type(avro.LogicalTypeTimestamp):
			switch subSchema.(*avro.DerivedPrimitiveSchema).Documentation {
			case string(DateTime):
				nullableField := sqlField.(*sql.NullString)
				if nullableField.Valid {
					t, err := time.Parse(SQLDateTimeFormat, nullableField.String)
					if err != nil {
						return nil, err
					}
					return map[string]interface{}{"int": int32(t.Unix())}, nil
				}
				return nil, nil
			case "", string(Timestamp):
				nullableField := sqlField.(*sql.NullInt64)
				if nullableField.Valid {
					return map[string]interface{}{"int": int32(nullableField.Int64)}, nil
				}
				return nil, nil
			default:
				return nil, ErrUnsupportedTypeForSQL
			}
		case avro.TypeFloat64:
			nullableField := sqlField.(*sql.NullFloat64)
			if nullableField.Valid {
				return map[string]interface{}{string(subSchema.TypeName()): nullableField.Float64}, nil
			}
			return nil, nil
		case avro.TypeFloat32:
			nullableField := sqlField.(*sql.NullFloat64)
			if nullableField.Valid {
				return map[string]interface{}{string(subSchema.TypeName()): float32(nullableField.Float64)}, nil
			}
			return nil, nil
		case avro.TypeString:
			nullableField := sqlField.(*sql.NullString)
			if nullableField.Valid {
				return map[string]interface{}{string(subSchema.TypeName()): nullableField.String}, nil
			}
			return nil, nil
		case avro.TypeBytes, avro.TypeFixed:
			field := *sqlField.(*[]byte)
			if field != nil {
				return map[string]interface{}{string(subSchema.TypeName()): field}, nil
			}
			return nil, nil
		case avro.Type(avro.LogicalTypeDecimal):
			field := *sqlField.(*[]byte)
			if field != nil {
				r := new(big.Rat)
				_, err := fmt.Sscan(string(field), r)
				if err != nil {
					return nil, err
				}
				return map[string]interface{}{string("bytes.decimal"): r}, nil
			}
			return nil, nil
		}
		return nil, ErrUnsupportedTypeForSQL
	}
	return nil, ErrUnsupportedTypeForSQL
}
