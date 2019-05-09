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
			t, err := time.Parse(time.RFC3339Nano, dst)
			if err != nil {
				return nil, err
			}
			return t.Format(SQLDateTimeFormat), nil
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

// NewCriterionFloat64 -
func NewCriterionFloat64(fieldName string, limit *float64, order avro.Order) *Criterion {
	var limitBytes *json.RawMessage
	if limit != nil {
		limitBytes = new(json.RawMessage)
		*limitBytes = []byte(strconv.FormatFloat(*limit, 'f', -1, 32))
	}
	return &Criterion{
		FieldName: fieldName,
		typeName:  avro.TypeFloat64,
		RawLimit:  limitBytes,
		Order:     order,
	}
}

// NewCriterionInt64 -
func NewCriterionInt64(fieldName string, limit *int64, order avro.Order) *Criterion {
	var limitBytes *json.RawMessage
	if limit != nil {
		limitBytes = new(json.RawMessage)
		*limitBytes = []byte(strconv.FormatInt(*limit, 10))
	}
	return &Criterion{
		FieldName: fieldName,
		typeName:  avro.TypeInt64,
		RawLimit:  limitBytes,
		Order:     order,
	}
}

// NewCriterionString -
func NewCriterionString(fieldName string, limit *string, order avro.Order) *Criterion {
	var limitBytes *json.RawMessage
	if limit != nil {
		limitBytes = new(json.RawMessage)
		*limitBytes = []byte(fmt.Sprintf(`"%s"`, *limit))
	}
	return &Criterion{
		FieldName: fieldName,
		typeName:  avro.TypeString,
		RawLimit:  limitBytes,
		Order:     order,
	}
}

// NewCriterionDateTime -
func NewCriterionDateTime(fieldName string, limit *time.Time, order avro.Order) *Criterion {
	var limitBytes *json.RawMessage
	if limit != nil {
		limitBytes = new(json.RawMessage)
		*limitBytes = []byte(fmt.Sprintf(`"%s"`, limit.Format(time.RFC3339Nano)))
	}
	return &Criterion{
		FieldName: fieldName,
		typeName:  avro.Type(avro.LogicalTypeTimestamp),
		RawLimit:  limitBytes,
		Order:     order,
	}
}

// NewCriterionDate -
func NewCriterionDate(fieldName string, limit *time.Time, order avro.Order) *Criterion {
	var limitBytes *json.RawMessage
	if limit != nil {
		limitBytes = new(json.RawMessage)
		*limitBytes = []byte(fmt.Sprintf(`"%s"`, limit.Format(SQLDateFormat)))
	}
	return &Criterion{
		FieldName: fieldName,
		typeName:  avro.Type(avro.LogicalTypeDate),
		RawLimit:  limitBytes,
		Order:     order,
	}
}

// NewCriterionTime -
func NewCriterionTime(fieldName string, limit *time.Time, order avro.Order) *Criterion {
	var limitBytes *json.RawMessage
	if limit != nil {
		limitBytes = new(json.RawMessage)
		*limitBytes = []byte(fmt.Sprintf(`"%s"`, limit.Format(SQLTimeFormat)))
	}
	return &Criterion{
		FieldName: fieldName,
		typeName:  avro.Type(avro.LogicalTypeTime),
		RawLimit:  limitBytes,
		Order:     order,
	}
}
