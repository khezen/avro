package sqlavro

import (
	"bytes"
	"database/sql"
	"encoding/json"

	"github.com/khezen/avro"
	"github.com/linkedin/goavro"
)

// Query -
func Query(cfg QueryConfig) (avroBytes []byte, newCriteria []Criterion, err error) {
	err = cfg.Verify()
	if err != nil {
		return nil, nil, err
	}
	records, err := query2Native(cfg.DB, cfg.DBName, cfg.Schema, cfg.Limit, cfg.Criteria)
	if err != nil {
		return nil, nil, err
	}
	recordsLen := len(records)
	if recordsLen > 0 && cfg.Criteria != nil {
		newCriteria, err = updateCriteria(cfg.Schema, records[recordsLen-1], cfg.Criteria)
		if err != nil {
			return nil, nil, err
		}
	} else {
		newCriteria = cfg.Criteria
	}
	SchemaBytes, err := json.Marshal(cfg.Schema)
	if err != nil {
		return nil, nil, err
	}
	avroBuf := new(bytes.Buffer)
	fileWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:               avroBuf,
		Schema:          string(SchemaBytes),
		CompressionName: cfg.Compression,
	})
	if err != nil {
		return nil, nil, err
	}
	err = fileWriter.Append(records)
	if err != nil {
		return nil, nil, err
	}
	return avroBuf.Bytes(), newCriteria, nil
}

func query2Native(db *sql.DB, dbName string, schema *avro.RecordSchema, limit int, criteria []Criterion) ([]map[string]interface{}, error) {
	statement, params, err := renderQuery(dbName, schema, limit, criteria)
	if err != nil {
		return nil, err
	}
	rows, err := db.Query(statement, params...)
	if err != nil {
		return nil, err
	}
	records := make([]map[string]interface{}, 0, limit)
	for rows.Next() {
		sqlFields, err := renderSQLFields(schema)
		if err != nil {
			return nil, err
		}
		err = rows.Scan(sqlFields...)
		if err != nil {
			return nil, err
		}
		record, err := renderNativeRecord(schema, sqlFields)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

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
		qBuf.WriteString(sqlEscape(fieldName))
		qBuf.WriteString("`,")
	}
	lastIndex := fieldsLen - 1
	if len(schema.Fields[lastIndex].Aliases) > 0 {
		fieldName = schema.Fields[lastIndex].Aliases[0]
	} else {
		fieldName = schema.Fields[lastIndex].Name
	}
	qBuf.WriteRune('`')
	qBuf.WriteString(sqlEscape(fieldName))
	qBuf.WriteString("` FROM `")
	if len(schema.Namespace) > 0 {
		qBuf.WriteString(sqlEscape(dbName))
		qBuf.WriteString("`.`")
	}
	var tableName string
	if len(schema.Aliases) > 0 {
		tableName = schema.Aliases[0]
	} else {
		tableName = schema.Name
	}
	qBuf.WriteString(sqlEscape(tableName))
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
		qBuf.WriteString(sqlEscape(criterion.FieldName))
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
		qBuf.WriteString(sqlEscape(criterion.FieldName))
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

func updateCriteria(schema *avro.RecordSchema, record map[string]interface{}, criteria []Criterion) (newCriteria []Criterion, err error) {
	newCriteria = make([]Criterion, 0, len(criteria))
	var newCrit *Criterion
	for _, criterion := range criteria {
		if criterion.RawLimit == nil {
			continue
		}
		for _, field := range schema.Fields {
			if criterion.FieldName == field.Name ||
				(len(field.Aliases) > 0 && criterion.FieldName == field.Aliases[0]) {
				newCrit, err = NewCriterion(&field, record[criterion.FieldName], criterion.Order)
				if err != nil {
					return nil, err
				}
				newCriteria = append(newCriteria, *newCrit)
				break
			}
		}
	}
	return newCriteria, nil
}
