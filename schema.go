package avro

import (
	"encoding/json"

	"github.com/valyala/fastjson"
)

var unmarshaller fastjson.Parser

// Schema -
type Schema interface {
	json.Marshaler
	TypeName() Type
}

// AnySchema -
type AnySchema struct {
	schema Schema
}

// UnmarshalJSON -
func (as *AnySchema) UnmarshalJSON(bytes []byte) error {
	value, err := unmarshaller.Parse(string(bytes))
	if err != nil {
		return err
	}
	schema, err := translateValue2AnySchema(value)
	if err != nil {
		return err
	}
	as.schema = schema
	return nil
}

func translateValue2AnySchema(value *fastjson.Value) (Schema, error) {
	isComplex := value.Exists("type")
	if isComplex {
		typeName := Type(value.Get("type").String())
		switch typeName {
		case TypeArray:
			return translateValue2ArraySchema(value)
		case TypeMap:
			return translateValueToMapSchema(value)
		case TypeEnum:
			return translateValueToEnumSchema(value)
		case TypeFixed:
			return translateValueToFixedSchema(value)
		case TypeRecord:
			return translateValueToRecordSchema(value)
		default:
			return nil, ErrUnsupportedType
		}
	}
	array, err := value.Array()
	if err != nil {
		return nil, ErrInvalidSchema
	}
	isUnion := err == nil
	if isUnion {
		return translateValues2UnionSchema(array)
	}
	typeName := Type(value.String())
	switch typeName {
	case TypeNull, TypeBoolean, TypeFloat32, TypeFloat64, TypeInt32, TypeInt64, TypeString, TypeBytes:
		return typeName, nil
	default:
		return nil, ErrUnsupportedType
	}
}

// Schema returns the unmarshalled schema
func (as *AnySchema) Schema() Schema {
	return as.schema
}
