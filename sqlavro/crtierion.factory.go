package sqlavro

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/khezen/avro"
)

// NewCriterionFromNative -
func NewCriterionFromNative(field *avro.RecordFieldSchema, value interface{}, order avro.Order) (*Criterion, error) {
	criterion := Criterion{
		Order: order,
	}
	if len(field.Aliases) > 0 {
		criterion.FieldName = field.Aliases[0]
	} else {
		criterion.FieldName = field.Name
	}
	criterion.setSchema(*field)
	err := criterion.SetLimit(value)
	if err != nil {
		return nil, err
	}
	return &criterion, nil
}

// NewCriterionFromString -
func NewCriterionFromString(field *avro.RecordFieldSchema, value string, order avro.Order) (*Criterion, error) {
	criterion := Criterion{
		Order: order,
	}
	if len(field.Aliases) > 0 {
		criterion.FieldName = field.Aliases[0]
	} else {
		criterion.FieldName = field.Name
	}
	criterion.setSchema(*field)
	err := criterion.setLimitFromString(value)
	if err != nil {
		return nil, err
	}
	return &criterion, nil
}

// NewCriterionFloat64 -
func NewCriterionFloat64(fieldName string, limit *float64, order avro.Order) *Criterion {
	var limitBytes *json.RawMessage
	if limit != nil {
		limitBytes = new(json.RawMessage)
		*limitBytes = []byte(strconv.FormatFloat(*limit, 'f', -1, 64))
	}
	return &Criterion{
		FieldName: fieldName,
		fieldSchema: &avro.RecordFieldSchema{
			Name:  fieldName,
			Type:  avro.TypeFloat64,
			Order: order,
		},
		RawLimit: limitBytes,
		Order:    order,
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
		fieldSchema: &avro.RecordFieldSchema{
			Name:  fieldName,
			Type:  avro.TypeInt64,
			Order: order,
		},
		RawLimit: limitBytes,
		Order:    order,
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
		fieldSchema: &avro.RecordFieldSchema{
			Name:  fieldName,
			Type:  avro.TypeString,
			Order: order,
		},
		RawLimit: limitBytes,
		Order:    order,
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
		fieldSchema: &avro.RecordFieldSchema{
			Name: fieldName,
			Type: &avro.DerivedPrimitiveSchema{
				Documentation: "datetime",
				LogicalType:   avro.LogicalTypeTimestamp,
				Type:          avro.TypeInt32,
			},
			Order: order,
		},
		RawLimit: limitBytes,
		Order:    order,
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
		fieldSchema: &avro.RecordFieldSchema{
			Name: fieldName,
			Type: &avro.DerivedPrimitiveSchema{
				Documentation: "datetime",
				LogicalType:   avro.LogicalTypeDate,
				Type:          avro.TypeInt32,
			},
			Order: order,
		},
		RawLimit: limitBytes,
		Order:    order,
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
		fieldSchema: &avro.RecordFieldSchema{
			Name: fieldName,
			Type: &avro.DerivedPrimitiveSchema{
				Documentation: "datetime",
				LogicalType:   avro.LogicalTypeTime,
				Type:          avro.TypeInt32,
			},
			Order: order,
		},
		RawLimit: limitBytes,
		Order:    order,
	}
}
