package sqlavro

import (
	"bytes"
	"encoding/json"

	"github.com/khezen/avro"
	"github.com/linkedin/goavro"
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
	return native2CSV(cfg, records)
}

func native2CSV(cfg QueryConfig, records []map[string]string) (avroBytes []byte, newCriteria []Criterion, err error) {
	recordsLen := len(records)
	if recordsLen > 0 && cfg.Criteria != nil {
		newCriteria, err = criteriaFromString(cfg.Schema, records[recordsLen-1], cfg.Criteria)
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
