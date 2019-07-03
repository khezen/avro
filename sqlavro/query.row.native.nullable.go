package sqlavro

import (
	"database/sql"
	"fmt"
	"math/big"
	"time"

	"github.com/khezen/avro"
)

func sql2NativeFieldNullable(schema avro.Schema, sqlField interface{}) (interface{}, error) {
	union := schema.(avro.UnionSchema)
	subSchema, err := UnderlyingType(union)
	if err != nil {
		return nil, err
	}
	switch subSchema.TypeName() {
	case avro.TypeInt64:
		return sql2NativeInt64Nullable(sqlField)
	case avro.TypeInt32:
		return sql2NativeInt32Nullable(sqlField)
	case avro.Type(avro.LogicalTypeDate):
		return sql2NativeDateNullable(sqlField)
	case avro.Type(avro.LogicalTypeTime):
		return sql2NativeTimeNullable(sqlField)
	case avro.Type(avro.LogicalTypeTimestamp):
		return sql2NativeTimestampNullable(subSchema, sqlField)
	case avro.TypeFloat64:
		return sql2NativeFloat64Nullable(sqlField)
	case avro.TypeFloat32:
		return sql2NativeFloat32Nullable(sqlField)
	case avro.TypeString:
		return sql2NativeStringNullable(sqlField)
	case avro.TypeBytes, avro.TypeFixed:
		return sql2NativeBytesNFixedNullable(subSchema, sqlField)
	case avro.Type(avro.LogicalTypeDecimal):
		return sql2NativeDecimalNullable(sqlField)
	}
	return nil, ErrUnsupportedTypeForSQL
}

func sql2NativeInt64Nullable(sqlField interface{}) (interface{}, error) {
	nullableField := sqlField.(*sql.NullInt64)
	if nullableField.Valid {
		return map[string]interface{}{string(avro.TypeInt64): nullableField.Int64}, nil
	}
	return nil, nil
}

func sql2NativeInt32Nullable(sqlField interface{}) (interface{}, error) {
	nullableField := sqlField.(*sql.NullInt64)
	if nullableField.Valid {
		return map[string]interface{}{string(avro.TypeInt32): int32(nullableField.Int64)}, nil
	}
	return nil, nil
}

func sql2NativeFloat64Nullable(sqlField interface{}) (interface{}, error) {
	nullableField := sqlField.(*sql.NullFloat64)
	if nullableField.Valid {
		return map[string]interface{}{string(avro.TypeFloat64): nullableField.Float64}, nil
	}
	return nil, nil
}

func sql2NativeFloat32Nullable(sqlField interface{}) (interface{}, error) {
	nullableField := sqlField.(*sql.NullFloat64)
	if nullableField.Valid {
		return map[string]interface{}{string(avro.TypeFloat32): float32(nullableField.Float64)}, nil
	}
	return nil, nil
}

func sql2NativeStringNullable(sqlField interface{}) (interface{}, error) {
	nullableField := sqlField.(*sql.NullString)
	if nullableField.Valid {
		return map[string]interface{}{string(avro.TypeString): nullableField.String}, nil
	}
	return nil, nil
}

func sql2NativeBytesNFixedNullable(schema avro.Schema, sqlField interface{}) (interface{}, error) {
	field := *sqlField.(*[]byte)
	if field != nil {
		return map[string]interface{}{string(schema.TypeName()): field}, nil
	}
	return nil, nil
}

func sql2NativeDateNullable(sqlField interface{}) (interface{}, error) {
	nullableField := sqlField.(*sql.NullString)
	if nullableField.Valid {
		t, err := time.Parse(SQLDateFormat, nullableField.String)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{"int.date": t}, nil
	}
	return nil, nil
}

func sql2NativeTimeNullable(sqlField interface{}) (interface{}, error) {
	nullableField := sqlField.(*sql.NullString)
	if nullableField.Valid {
		t, err := time.Parse(SQLTimeFormat, nullableField.String)
		if err != nil {
			return nil, err
		}
		t = t.AddDate(1970, 1, 1)
		return map[string]interface{}{string(avro.TypeInt32): int32(t.Unix())}, nil
	}
	return nil, nil
}

func sql2NativeTimestampNullable(schema avro.Schema, sqlField interface{}) (interface{}, error) {
	switch schema.(*avro.DerivedPrimitiveSchema).Documentation {
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
}

func sql2NativeDecimalNullable(sqlField interface{}) (interface{}, error) {
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
