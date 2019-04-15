package sqlavro

import (
	"bytes"
	"database/sql"
	"encoding/json"

	"github.com/khezen/avro"
	"github.com/linkedin/goavro"
)

// Query -
func Query(db *sql.DB, schema *avro.RecordSchema, limit int, criteria ...Criterion) (avroBytes []byte, err error) {
	native, err := Query2Native(db, schema, limit, criteria...)
	if err != nil {
		return nil, err
	}
	resultSchema := avro.ArraySchema{
		Type:  avro.TypeArray,
		Items: schema,
	}
	resultSchemaBytes, err := json.Marshal(resultSchema)
	if err != nil {
		return nil, err
	}
	codec, err := goavro.NewCodec(string(resultSchemaBytes))
	if err != nil {
		return nil, err
	}
	avroBytes, err = codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, err
	}
	return avroBytes, nil
}

// Query2Native -
func Query2Native(db *sql.DB, schema *avro.RecordSchema, limit int, criteria ...Criterion) ([]map[string]interface{}, error) {
	statement, params, err := renderQuery(schema, limit, criteria...)
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

func renderQuery(schema *avro.RecordSchema, limit int, criteria ...Criterion) (statement string, params []interface{}, err error) {
	if len(schema.Fields) == 0 {
		return "", nil, ErrExpectRecordSchema
	}
	params = make([]interface{}, 0, len(schema.Fields)+4*len(criteria)+4)
	qBuf := bytes.NewBufferString("SELECT")
	for _, field := range schema.Fields {
		qBuf.WriteString(" ?")
		params = append(params, field.Name)
	}
	qBuf.WriteString(" FROM ?.?")
	params = append(params, schema.Namespace, schema.Name)
	criteriaLen := len(criteria)
	if criteriaLen == 0 {
		return qBuf.String(), params, nil
	}
	qBuf.WriteString("WHERE")
	for i, criterion := range criteria {
		qBuf.WriteString(" ???")
		if i < criteriaLen-1 {
			qBuf.WriteString(" AND")
		}
		critLimit, err := criterion.Limit()
		if err != nil {
			return "", nil, err
		}
		operand, err := criterion.OrderOperand()
		if err != nil {
			return "", nil, err
		}
		params = append(params, criterion.FieldName, operand, critLimit)
	}
	qBuf.WriteString(" ORDER BY")
	for i, criterion := range criteria {
		qBuf.WriteString(" ?")
		if i < criteriaLen-1 {
			qBuf.WriteString(",")
		}
		params = append(params, criterion.FieldName)
	}
	qBuf.WriteString(" ?")
	sort, err := criteria[0].OrderSort()
	if err != nil {
		return "", nil, err
	}
	params = append(params, sort)
	qBuf.WriteString(" LIMIT ?")
	params = append(params, limit)
	return qBuf.String(), params, nil
}