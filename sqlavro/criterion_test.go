package sqlavro

// func TestCriterion(t *testing.T) {
// 	cases := []struct {
// 		fieldName       string
// 		Type            avro.Type
// 		limit           interface{}
// 		order           avro.Order
// 		expectedOperand string
// 		expectedSort    string
// 		expectedErr     error
// 	}{
// 		{"test", avro.TypeInt64, int64(30), avro.Ascending, ">", "ASC", nil},
// 		{"test", avro.TypeFloat64, float64(30), avro.Descending, "<", "DESC", nil},
// 	}
// 	for _, c := range cases {
// 		var criterion *Criterion
// 		switch c.Type {
// 		case avro.TypeInt64:
// 			criterion = NewCriterionInt64(c.fieldName, c.limit.(int64), c.order)
// 			break
// 		case avro.TypeFloat64:
// 			criterion = NewCriterionFloat64(c.fieldName, c.limit.(float64), c.order)
// 			break
// 		case avro.TypeString:
// 			criterion = NewCriterionString(c.fieldName, c.limit.(*string), c.order)
// 			break
// 		default:
// 			t.Errorf("unsupported type")
// 			break
// 		}
// 		limit, err := criterion.Limit()
// 		if err != c.expectedErr {
// 			t.Errorf("expected %v, got %v", c.expectedErr, err)
// 		}
// 		if err != nil {
// 			continue
// 		}
// 		if limit != c.limit {
// 			t.Errorf("expected %v, got %v", c.limit, limit)
// 		}
// 		operand, err := criterion.OrderOperand()
// 		if err != c.expectedErr {
// 			t.Errorf("expected %v, got %v", c.expectedErr, err)
// 		}
// 		if err != nil {
// 			continue
// 		}
// 		if !strings.EqualFold(c.expectedOperand, operand) {
// 			t.Errorf("expected %v, got %v", c.expectedOperand, operand)
// 		}
// 		sort, err := criterion.OrderSort()
// 		if err != c.expectedErr {
// 			t.Errorf("expected %v, got %v", c.expectedErr, err)
// 		}
// 		if err != nil {
// 			continue
// 		}
// 		if !strings.EqualFold(c.expectedSort, sort) {
// 			t.Errorf("expected %v, got %v", c.expectedSort, sort)
// 		}
// 	}
// }
