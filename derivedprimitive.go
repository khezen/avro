package avro

import "github.com/valyala/fastjson"

// DerivedPrimitiveSchema -
type DerivedPrimitiveSchema struct {
	Type        Type        `json:"type"`
	LogicalType LogicalType `json:"logicalType"`
	Precision   *int        `json:"precision,omitempty"`
	Scale       *int        `json:"scale,omitempty"`
}

// TypeName -
func (t *DerivedPrimitiveSchema) TypeName() Type {
	return t.Type
}

func translateValue2DerivedPrimitiveSchema(typeName Type, value *fastjson.Value) (Schema, error) {
	if !value.Exists("logicalType") {
		return nil, ErrInvalidSchema
	}
	logicalType := LogicalType(value.GetStringBytes("logicalType"))
	switch logicalType {
	case LogicalTypeDate, LogicalTypeTime, LogicalTypeTimestamp:
		switch typeName {
		case TypeInt32, TypeInt64:
			return &DerivedPrimitiveSchema{
				Type:        typeName,
				LogicalType: logicalType,
			}, nil
		default:
			return nil, ErrInvalidSchema
		}
	case LogicalTypeDecimal:
		if !value.Exists("precision") {
			return nil, ErrInvalidSchema
		}
		precision, err := value.Get("precision").Int()
		if err != nil {
			return nil, ErrInvalidSchema
		}
		if precision < 0 {
			return nil, ErrInvalidSchema
		}
		var scale *int
		if value.Exists("scale") {
			scaleInt, err := value.Get("scale").Int()
			if err != nil {
				return nil, ErrInvalidSchema
			}
			if scaleInt < 0 {
				return nil, ErrInvalidSchema
			}
			scale = &scaleInt
		}
		return &DerivedPrimitiveSchema{
			Type:        typeName,
			LogicalType: logicalType,
			Precision:   &precision,
			Scale:       scale,
		}, nil
	default:
		return nil, ErrInvalidSchema
	}
}
