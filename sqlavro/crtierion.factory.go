package sqlavro

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/khezen/avro"
)

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
