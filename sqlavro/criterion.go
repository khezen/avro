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
	FieldName string `json:"field"`
	typeName  avro.Type
	RawLimit  *json.RawMessage `json:"limit,omitempty"`
	Order     avro.Order       `json:"order,omitempty"` // default: Ascending
}

func (c *Criterion) setType(typeName avro.Type) {
	c.typeName = typeName
}

// Limit -
func (c *Criterion) Limit() (interface{}, error) {
	if c.RawLimit == nil {
		return nil, nil
	}
	switch c.typeName {
	case avro.TypeFloat32, avro.TypeFloat64:
		return strconv.ParseFloat(string(*c.RawLimit), 64)
	case avro.TypeInt32, avro.TypeInt64:
		return strconv.ParseInt(string(*c.RawLimit), 10, 64)
	case avro.TypeString,
		avro.Type(avro.LogicalTypeTimestamp),
		avro.Type(avro.LogicalTypeDate),
		avro.Type(avro.LogicalTypeTime):
		dst := string(*c.RawLimit)[1 : len(*c.RawLimit)-1]
		switch c.typeName {
		case avro.TypeString:
			return dst, nil
		case avro.Type(avro.LogicalTypeTimestamp):
			_, err := time.Parse(time.RFC3339Nano, dst)
			if err != nil {
				return nil, err
			}
			return dst, nil
		case avro.Type(avro.LogicalTypeDate):
			_, err := time.Parse(SQLDateFormat, dst)
			if err != nil {
				return nil, err
			}
			return dst, nil
		case avro.Type(avro.LogicalTypeTime):
			_, err := time.Parse(SQLTimeFormat, dst)
			if err != nil {
				return nil, err
			}
			return dst, nil
		default:
			return nil, ErrUnsupportedTypeForCriterion
		}
	default:
		return nil, ErrUnsupportedTypeForCriterion
	}
}

func (c *Criterion) setLimit(limit interface{}) error {
	if limit == nil {
		return nil
	}
	var rawLimit json.RawMessage
	switch c.typeName {
	case avro.TypeFloat32, avro.TypeFloat64:
		rawLimit = json.RawMessage(strconv.FormatFloat(limit.(float64), 'f', -1, 64))
		break
	case avro.TypeInt32, avro.TypeInt64:
		rawLimit = json.RawMessage(strconv.FormatInt(limit.(int64), 10))
		break
	case avro.TypeString:
		rawLimit = json.RawMessage(fmt.Sprintf(`"%s"`, limit.(string)))
		break
	case avro.Type(avro.LogicalTypeTimestamp):
		t := time.Date(0, 0, 0, 0, 0, limit.(int), 0, time.UTC)
		rawLimit = json.RawMessage(fmt.Sprintf(`"%s"`, t.Format(time.RFC3339Nano)))
		break
	case avro.Type(avro.LogicalTypeDate):
		t := time.Date(0, 0, 0, 0, 0, limit.(int), 0, time.UTC)
		rawLimit = json.RawMessage(fmt.Sprintf(`"%s"`, t.Format(SQLDateFormat)))
		break
	case avro.Type(avro.LogicalTypeTime):
		t := limit.(time.Time)
		rawLimit = json.RawMessage(fmt.Sprintf(`"%s"`, t.Format(time.RFC3339Nano)))
		break
	default:
		return ErrUnsupportedTypeForCriterion
	}
	c.RawLimit = &rawLimit
	return nil
}

// OrderOperand -
func (c *Criterion) OrderOperand() (string, error) {
	switch c.Order {
	case avro.Descending:
		return "<", nil
	case "", avro.Ascending:
		return ">", nil
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
