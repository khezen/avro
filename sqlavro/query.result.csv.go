package sqlavro

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"time"

	"github.com/golang/snappy"

	"github.com/khezen/avro"
)

func query2CSV(cfg QueryConfig) (csvBytes []byte, newCriteria []Criterion, err error) {
	statement, params, err := renderQuery(cfg.DBName, cfg.Schema, cfg.Limit, cfg.Criteria)
	if err != nil {
		return nil, nil, err
	}
	rows, err := cfg.DB.Query(statement, params...)
	if err != nil {
		return nil, nil, err
	}
	records := make([]map[string]string, 0, cfg.Limit)
	for rows.Next() {
		sqlFields, err := renderSQLFields(cfg.Schema)
		if err != nil {
			return nil, nil, err
		}
		err = rows.Scan(sqlFields...)
		if err != nil {
			return nil, nil, err
		}
		record, err := sqlRow2String(cfg.Schema, sqlFields)
		if err != nil {
			return nil, nil, err
		}
		records = append(records, record)
	}
	return strings2CSV(cfg, records)
}

func strings2CSV(cfg QueryConfig, records []map[string]string) (csvBytes []byte, newCriteria []Criterion, err error) {
	recordsLen := len(records)
	if recordsLen > 0 && cfg.Criteria != nil {
		newCriteria, err = criteriaFromString(cfg.Schema, records[recordsLen-1], cfg.Criteria)
		if err != nil {
			return nil, nil, err
		}
	} else {
		newCriteria = cfg.Criteria
	}
	var (
		buf       = new(bytes.Buffer)
		fieldsLen = len(cfg.Schema.Fields)
		i, j      int
		fieldName string
	)
	for i = 0; i < fieldsLen-1; i++ {
		buf.WriteString(cfg.Schema.Fields[i].Name)
		buf.WriteRune(',')
	}
	buf.WriteString(cfg.Schema.Fields[fieldsLen-1].Name)
	buf.WriteRune('\n')
	for i = 0; i < recordsLen; i++ {
		for j = 0; j < fieldsLen-1; j++ {
			fieldName = cfg.Schema.Fields[j].Name
			buf.WriteString(records[i][fieldName])
			buf.WriteRune(cfg.Separator)
		}
		fieldName = cfg.Schema.Fields[fieldsLen-1].Name
		buf.WriteString(records[i][fieldName])
		buf.WriteRune('\n')
	}
	csvBytes = buf.Bytes()
	switch cfg.Compression {
	case "", "null":
		break
	case avro.CompressionDeflate:
		buf.Reset()
		zw := gzip.NewWriter(buf)
		zw.Name = fmt.Sprintf("%s.csv", cfg.Schema.Name)
		zw.ModTime = time.Now().UTC()
		if _, err := zw.Write(csvBytes); err != nil {
			return nil, nil, err
		}
		if err := zw.Close(); err != nil {
			return nil, nil, err
		}
		csvBytes = buf.Bytes()
		break
	case avro.CompressionSnappy:
		csvBytes = snappy.Encode(nil, csvBytes)
		break
	}
	return csvBytes, newCriteria, nil
}

func criteriaFromString(schema *avro.RecordSchema, record map[string]string, criteria []Criterion) (newCriteria []Criterion, err error) {
	newCriteria = make([]Criterion, 0, len(criteria))
	var newCrit *Criterion
	for _, criterion := range criteria {
		if criterion.RawLimit == nil {
			continue
		}
		for _, field := range schema.Fields {
			if criterion.FieldName == field.Name ||
				(len(field.Aliases) > 0 && criterion.FieldName == field.Aliases[0]) {
				newCrit, err = NewCriterionFromString(&field, record[criterion.FieldName], criterion.Order)
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
