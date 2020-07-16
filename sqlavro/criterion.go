package sqlavro

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/khezen/avro"
)

// Criterion -
type Criterion struct {
	FieldName   string `json:"field"`
	fieldSchema *avro.RecordFieldSchema
	RawLimit    *json.RawMessage `json:"limit,omitempty"`
	Order       avro.Order       `json:"order,omitempty"` // default: Ascending
}

func (c *Criterion) setSchema(field avro.RecordFieldSchema) {
	c.fieldSchema = &field
}

// Limit -
func (c *Criterion) Limit() (interface{}, error) {
	if c.RawLimit == nil {
		return nil, nil
	}
	var (
		schema avro.Schema
		err    error
	)
	if c.fieldSchema.Type.TypeName() == avro.TypeUnion {
		schema, err = UnderlyingType(c.fieldSchema.Type.(avro.UnionSchema))
		if err != nil {
			return nil, err
		}
	} else {
		schema = c.fieldSchema.Type
	}
	return c.limit(schema)
}

func (c *Criterion) limit(schema avro.Schema) (interface{}, error) {
	typeName := schema.TypeName()
	switch typeName {
	case avro.TypeFloat32, avro.TypeFloat64:
		return strconv.ParseFloat(string(*c.RawLimit), 64)
	case avro.TypeInt32, avro.TypeInt64:
		return strconv.ParseInt(string(*c.RawLimit), 10, 64)
	case avro.TypeString:
		return string(*c.RawLimit)[1 : len(*c.RawLimit)-1], nil
	case avro.Type(avro.LogicalTypeTimestamp):
		dst := string(*c.RawLimit)[1 : len(*c.RawLimit)-1]
		t, err := time.Parse(time.RFC3339Nano, dst)
		if err != nil {
			return nil, err
		}
		return t.Format(SQLDateTimeFormat), nil
	case avro.Type(avro.LogicalTypeDate):
		dst := string(*c.RawLimit)[1 : len(*c.RawLimit)-1]
		_, err := time.Parse(SQLDateFormat, dst)
		if err != nil {
			return nil, err
		}
		return dst, nil
	case avro.Type(avro.LogicalTypeTime):
		dst := string(*c.RawLimit)[1 : len(*c.RawLimit)-1]
		_, err := time.Parse(SQLTimeFormat, dst)
		if err != nil {
			return nil, err
		}
		return dst, nil
	default:
		return nil, ErrUnsupportedTypeForCriterion
	}
}

// SetLimit - from native go
func (c *Criterion) SetLimit(limit interface{}) error {
	if limit == nil {
		return nil
	}
	var (
		schema avro.Schema
		err    error
	)
	if c.fieldSchema.Type.TypeName() == avro.TypeUnion {
		schema, err = UnderlyingType(c.fieldSchema.Type.(avro.UnionSchema))
		if err != nil {
			return err
		}
		var (
			typeName      = schema.TypeName()
			primitiveType string
		)
		switch typeName {
		case avro.Type(avro.LogicalTypeTimestamp), avro.Type(avro.LogicalTypeTime):
			primitiveType = string(avro.TypeInt32)
		case avro.Type(avro.LogicalTypeDate):
			primitiveType = "int.date"
		case avro.Type(avro.LogicalTypeDecimal):
			primitiveType = "bytes.decimal"
		default:
			primitiveType = string(typeName)
		}
		limit = limit.(map[string]interface{})[primitiveType]
	} else {
		schema = c.fieldSchema.Type
	}
	rawLimit, err := rawLimit2Native(schema, limit)
	if err != nil {
		return err
	}
	c.RawLimit = &rawLimit
	return nil
}

func (c *Criterion) setLimitFromString(limit string) error {
	if limit == "" {
		return nil
	}
	var (
		schema avro.Schema
		err    error
	)
	if c.fieldSchema.Type.TypeName() == avro.TypeUnion {
		schema, err = UnderlyingType(c.fieldSchema.Type.(avro.UnionSchema))
		if err != nil {
			return err
		}
	} else {
		schema = c.fieldSchema.Type
	}
	rawLimit, err := rawLimit2String(schema, limit)
	if err != nil {
		return err
	}
	c.RawLimit = &rawLimit
	return nil
}

func rawLimit2String(schema avro.Schema, limit string) (json.RawMessage, error) {
	var (
		rawLimit json.RawMessage
		typeName = schema.TypeName()
	)
	switch typeName {
	case avro.TypeFloat32, avro.TypeFloat64,
		avro.TypeInt32, avro.TypeInt64:
		rawLimit = json.RawMessage(limit)
		return rawLimit, nil
	case avro.TypeString,
		avro.Type(avro.LogicalTypeDate),
		avro.Type(avro.LogicalTypeTime):
		rawLimit = json.RawMessage(fmt.Sprintf(`"%s"`, limit))
		return rawLimit, nil
	case avro.Type(avro.LogicalTypeTimestamp):
		t, err := time.Parse(SQLDateTimeFormat, limit)
		if err != nil {
			return nil, err
		}
		rawLimit = json.RawMessage(fmt.Sprintf(`"%s"`, t.Format(time.RFC3339Nano)))
		return rawLimit, nil
	default:
		return nil, ErrUnsupportedTypeForCriterion
	}
}

func rawLimit2Native(schema avro.Schema, limit interface{}) (json.RawMessage, error) {
	var (
		rawLimit json.RawMessage
		typeName = schema.TypeName()
	)
	switch typeName {
	case avro.TypeFloat32, avro.TypeFloat64:
		rawLimit = json.RawMessage(strconv.FormatFloat(limit.(float64), 'f', -1, 64))
		return rawLimit, nil
	case avro.TypeInt32, avro.TypeInt64:
		rawLimit = json.RawMessage(strconv.FormatInt(limit.(int64), 10))
		return rawLimit, nil
	case avro.TypeString:
		rawLimit = json.RawMessage(fmt.Sprintf(`"%s"`, limit.(string)))
		return rawLimit, nil
	case avro.Type(avro.LogicalTypeTimestamp),
		avro.Type(avro.LogicalTypeTime):
		var t time.Time
		var ok bool
		if t, ok = limit.(time.Time); !ok {
			t = time.Date(1970, 1, 1, 0, 0, int(limit.(int32)), 0, time.UTC)
		}
		rawLimit = json.RawMessage(fmt.Sprintf(`"%s"`, t.Format(time.RFC3339Nano)))
		return rawLimit, nil
	case avro.Type(avro.LogicalTypeDate):
		var t time.Time
		var ok bool
		if t, ok = limit.(time.Time); !ok {
			t = time.Date(1970, 1, 1, 0, 0, int(limit.(int32)), 0, time.UTC)
		}
		rawLimit = json.RawMessage(fmt.Sprintf(`"%s"`, t.Format(SQLDateFormat)))
		return rawLimit, nil
	default:
		return nil, ErrUnsupportedTypeForCriterion
	}
}

// OrderOperand -
func (c *Criterion) OrderOperand() (string, error) {
	switch c.Order {
	case avro.Descending:
		return "<=", nil
	case "", avro.Ascending:
		return ">=", nil
	default:
		return "", ErrCannotIgnoreOrder
	}
}

// OrderSort -
func (c *Criterion) OrderSort() (string, error) {
	switch c.Order {
	case avro.Descending:
		return "DESC", nil
	case "", avro.Ascending:
		return "ASC", nil
	default:
		return "", ErrCannotIgnoreOrder
	}
}
