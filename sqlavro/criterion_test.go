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
		fieldName       string
		Type            avro.Type
		limit           interface{}
		expectedLimit   interface{}
		order           avro.Order
		expectedOperand string
		expectedSort    string
		expectedErr     error
	}{
		{"test", avro.TypeInt64, &long, long, avro.Ascending, ">", "ASC", nil},
		{"test", avro.TypeFloat64, &double, double, avro.Descending, "<", "DESC", nil},
		{"test", avro.TypeString, nilString, nilInterface, "", ">", "ASC", nil},
		{"test", avro.TypeString, &str, str, "", ">", "ASC", nil},
		{"test", avro.Type(avro.LogicalTypeDate), &date, date.Format(SQLDateFormat), "", ">", "ASC", nil},
		{"test", avro.Type(avro.LogicalTypeTimestamp), &datetime, datetime.Format(SQLDateTimeFormat), "", ">", "ASC", nil},
		{"test", avro.Type(avro.LogicalTypeTime), &clock, clock.Format(SQLTimeFormat), "", ">", "ASC", nil},
	}
	for _, c := range cases {
		var criterion *Criterion
		switch c.Type {
		case avro.TypeInt64:
			criterion = NewCriterionInt64(c.fieldName, c.limit.(*int64), c.order)
			break
		case avro.TypeFloat64:
			criterion = NewCriterionFloat64(c.fieldName, c.limit.(*float64), c.order)
			break
		case avro.TypeString:
			criterion = NewCriterionString(c.fieldName, c.limit.(*string), c.order)
			break
		case avro.Type(avro.LogicalTypeDate):
			criterion = NewCriterionDate(c.fieldName, c.limit.(*time.Time), c.order)
		case avro.Type(avro.LogicalTypeTimestamp):
			criterion = NewCriterionDateTime(c.fieldName, c.limit.(*time.Time), c.order)
		case avro.Type(avro.LogicalTypeTime):
			criterion = NewCriterionTime(c.fieldName, c.limit.(*time.Time), c.order)
		default:
			t.Errorf("unsupported type")
			break
		}
		limit, err := criterion.Limit()
		if err != c.expectedErr {
			t.Errorf("expected %v, got %v", c.expectedErr, err)
		}
		if err != nil {
			continue
		}
		if limit != c.expectedLimit {
			t.Errorf("expected %v, got %v", c.limit, limit)
		}
		operand, err := criterion.OrderOperand()
		if err != c.expectedErr {
			t.Errorf("expected %v, got %v", c.expectedErr, err)
		}
		if err != nil {
			continue
		}
		if !strings.EqualFold(c.expectedOperand, operand) {
			t.Errorf("expected %v, got %v", c.expectedOperand, operand)
		}
		sort, err := criterion.OrderSort()
		if err != c.expectedErr {
			t.Errorf("expected %v, got %v", c.expectedErr, err)
		}
		if err != nil {
			continue
		}
		if !strings.EqualFold(c.expectedSort, sort) {
			t.Errorf("expected %v, got %v", c.expectedSort, sort)
		}
	}
}
