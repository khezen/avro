package sqlavro

import (
	"bytes"
	"encoding/json"

	"github.com/linkedin/goavro/v2"
)

func query2Parquet(cfg QueryConfig) (parquetBytes []byte, newCriteria []Criterion, err error) {
	statement, params, err := renderQuery(cfg.DBName, cfg.Schema, cfg.Limit, cfg.Criteria)
	if err != nil {
		return nil, nil, err
	}
	rows, err := cfg.DB.Query(statement, params...)
	if err != nil {
		return nil, nil, err
	}
	records := make([]map[string]interface{}, 0, cfg.Limit)
	for rows.Next() {
		sqlFields, err := renderSQLFields(cfg.Schema)
		if err != nil {
			return nil, nil, err
		}
		err = rows.Scan(sqlFields...)
		if err != nil {
			return nil, nil, err
		}
		record, err := sqlRow2native(cfg.Schema, sqlFields)
		if err != nil {
			return nil, nil, err
		}
		records = append(records, record)
	}
	return native2parquet(cfg, records)
}

func native2parquet(cfg QueryConfig, records []map[string]interface{}) (avroBytes []byte, newCriteria []Criterion, err error) {
	recordsLen := len(records)
	if recordsLen > 0 && cfg.Criteria != nil {
		newCriteria, err = criteriaFromNative(cfg.Schema, records[recordsLen-1], cfg.Criteria)
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
