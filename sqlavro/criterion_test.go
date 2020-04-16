package sqlavro

import (
	"strings"
	"testing"
	"time"

	"github.com/khezen/avro"
)

func TestCriterion(t *testing.T) {
	var (
		nilInterface interface{}
		nilString    *string
		str                = "A"
		long         int64 = 30
		double             = 30.30
		date               = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		datetime           = time.Date(1970, 1, 1, 19, 7, 0, 0, time.UTC)
		clock              = time.Date(0, 0, 0, 19, 7, 0, 0, time.UTC)
	)
	cases := []struct {
		fieldName        string
		Type             avro.Type
		limit            interface{}
		expectedLimit    interface{}
		expectedLimitErr error
		order            avro.Order
		expectedOperand  string
		expectedSort     string
		expectedOrderErr error
	}{
		{"test", avro.TypeInt64, &long, long, nil, avro.Ascending, ">=", "ASC", nil},
		{"test", avro.TypeFloat64, &double, double, nil, avro.Descending, "<=", "DESC", nil},
		{"test", avro.TypeString, nilString, nilInterface, nil, "", ">=", "ASC", nil},
		{"test", avro.TypeString, nilString, nilInterface, nil, "Ignore", "", "", ErrCannotIgnoreOrder},
		{"test", avro.TypeString, nilString, nilInterface, nil, "Something", "", "", ErrCannotIgnoreOrder},
		{"test", avro.TypeString, &str, str, nil, "", ">=", "ASC", nil},
		{"test", avro.Type(avro.LogicalTypeDate), &date, date.Format(SQLDateFormat), nil, "", ">=", "ASC", nil},
		{"test", avro.Type(avro.LogicalTypeTimestamp), &datetime, datetime.Format(SQLDateTimeFormat), nil, "", ">=", "ASC", nil},
		{"test", avro.Type(avro.LogicalTypeTime), &clock, clock.Format(SQLTimeFormat), nil, "", ">=", "ASC", nil},
	}
	for _, c := range cases {
		var criterion *Criterion
		switch c.Type {
		case avro.TypeInt64:
			criterion = NewCriterionInt64(c.fieldName, c.limit.(*int64), c.order)
		case avro.TypeFloat64:
			criterion = NewCriterionFloat64(c.fieldName, c.limit.(*float64), c.order)
		case avro.TypeString:
			criterion = NewCriterionString(c.fieldName, c.limit.(*string), c.order)
		case avro.Type(avro.LogicalTypeDate):
			criterion = NewCriterionDate(c.fieldName, c.limit.(*time.Time), c.order)
		case avro.Type(avro.LogicalTypeTimestamp):
			criterion = NewCriterionDateTime(c.fieldName, c.limit.(*time.Time), c.order)
		case avro.Type(avro.LogicalTypeTime):
			criterion = NewCriterionTime(c.fieldName, c.limit.(*time.Time), c.order)
		default:
			t.Errorf("unsupported type")
		}
		limit, err := criterion.Limit()
		if err != c.expectedLimitErr {
			t.Errorf("expected %v, got %v", c.expectedLimitErr, err)
		}
		if limit != c.expectedLimit {
			t.Errorf("expected %v, got %v", c.limit, limit)
		}
		operand, err := criterion.OrderOperand()
		if err != c.expectedOrderErr {
			t.Errorf("expected %v, got %v", c.expectedOrderErr, err)
		}
		if !strings.EqualFold(c.expectedOperand, operand) {
			t.Errorf("expected %v, got %v", c.expectedOperand, operand)
		}
		sort, err := criterion.OrderSort()
		if err != c.expectedOrderErr {
			t.Errorf("expected %v, got %v", c.expectedOrderErr, err)
		}
		if !strings.EqualFold(c.expectedSort, sort) {
			t.Errorf("expected %v, got %v", c.expectedSort, sort)
		}
	}
}
