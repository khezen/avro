package sqlavro

import (
	"bytes"

	"github.com/khezen/avro"
)

func renderQuery(dbName string, schema *avro.RecordSchema, limit int, criteria []Criterion) (statement string, params []interface{}, err error) {
	fieldsLen := len(schema.Fields)
	if fieldsLen == 0 {
		return "", nil, ErrExpectRecordSchema
	}
	var criteriaLen int
	if criteria != nil {
		criteriaLen = len(criteria)
		err = ensureCriterionTypes(schema, criteria)
		if err != nil {
			return "", nil, err
		}
	}
	params = make([]interface{}, 0, criteriaLen+2)
	qBuf := bytes.NewBufferString("SELECT ")
	var fieldName string
	for i := 0; i < fieldsLen-1; i++ {
		if len(schema.Fields[i].Aliases) > 0 {
			fieldName = schema.Fields[i].Aliases[0]
		} else {
			fieldName = schema.Fields[i].Name
		}
		qBuf.WriteRune('`')
		qBuf.WriteString(SQLEscape(fieldName))
		qBuf.WriteString("`,")
	}
	lastIndex := fieldsLen - 1
	if len(schema.Fields[lastIndex].Aliases) > 0 {
		fieldName = schema.Fields[lastIndex].Aliases[0]
	} else {
		fieldName = schema.Fields[lastIndex].Name
	}
	qBuf.WriteRune('`')
	qBuf.WriteString(SQLEscape(fieldName))
	qBuf.WriteString("` FROM `")
	if len(schema.Namespace) > 0 {
		qBuf.WriteString(SQLEscape(dbName))
		qBuf.WriteString("`.`")
	}
	var tableName string
	if len(schema.Aliases) > 0 {
		tableName = schema.Aliases[0]
	} else {
		tableName = schema.Name
	}
	qBuf.WriteString(SQLEscape(tableName))
	qBuf.WriteRune('`')
	if criteriaLen == 0 {
		return qBuf.String(), params, nil
	}
	var limitCriteriaLen int
	for _, criterion := range criteria {
		critLimit, err := criterion.Limit()
		if err != nil {
			return "", nil, err
		}
		if critLimit != nil {
			limitCriteriaLen++
		}
	}
	qBuf.WriteString(" WHERE")
	for i, criterion := range criteria {
		critLimit, err := criterion.Limit()
		if err != nil {
			return "", nil, err
		}
		if critLimit == nil {
			continue
		}
		operand, err := criterion.OrderOperand()
		if err != nil {
			return "", nil, err
		}
		qBuf.WriteString(" `")
		qBuf.WriteString(SQLEscape(criterion.FieldName))
		qBuf.WriteRune('`')
		qBuf.WriteString(operand)
		qBuf.WriteString("?")
		if i < limitCriteriaLen-1 {
			qBuf.WriteString(" AND")
		}
		params = append(params, critLimit)
	}
	qBuf.WriteString(" ORDER BY")
	for i, criterion := range criteria {
		qBuf.WriteString(" `")
		qBuf.WriteString(SQLEscape(criterion.FieldName))
		qBuf.WriteRune('`')
		if i < criteriaLen-1 {
			qBuf.WriteString(",")
		}
	}
	sort, err := criteria[0].OrderSort()
	if err != nil {
		return "", nil, err
	}
	qBuf.WriteRune(' ')
	qBuf.WriteString(sort)
	qBuf.WriteString(" LIMIT ?")
	params = append(params, limit)
	return qBuf.String(), params, nil
}
