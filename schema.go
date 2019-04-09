package avro

import (
	"github.com/valyala/fastjson"
)

var unmarshaller fastjson.Parser

// Schema -
type Schema interface {
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
	union, err := value.Array()
	isUnion := err == nil
	if isUnion {
		return translateValues2UnionSchema(union)
	}
	isComplex := value.Exists("type")
	if isComplex {
		stringBytes, err := value.Get("type").StringBytes()
		if err != nil {
			return nil, ErrInvalidSchema
		}
		typeName := Type(stringBytes)
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
	stringBytes, err := value.StringBytes()
	if err != nil {
		return nil, ErrInvalidSchema
	}
	typeName := Type(stringBytes)
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
