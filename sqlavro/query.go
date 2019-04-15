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
	var (
		records = make([]map[string]interface{}, 0, limit)
		field   avro.RecordFieldSchema
		i       int
	)
	for rows.Next() {
		nativeFields, err := renderNativeFields(schema)
		if err != nil {
			return nil, err
		}
		err = rows.Scan(nativeFields...)
		if err != nil {
			return nil, err
		}
		record := make(map[string]interface{})
		for i, field = range schema.Fields {
			record[field.Name] = nativeFields[i]
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

func renderNativeFields(schema *avro.RecordSchema) ([]interface{}, error) {
	nativeFields := make([]interface{}, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		nativeField, err := renderNativeField(field.Type)
		if err != nil {
			return nil, err
		}
		nativeFields = append(nativeFields, nativeField)
	}
	return nativeFields, nil
}

func renderNativeField(schema avro.Schema) (interface{}, error) {
	switch schema.TypeName() {
	case avro.TypeInt64:
		var field int64
		return &field, nil
	case avro.TypeInt32, avro.Type(avro.LogicalTypeDate), avro.Type(avro.LogicalTypeTime), avro.Type(avro.LogicalTypeTimestamp):
		var field int32
		return &field, nil
	case avro.TypeFloat64:
		var field float64
		return &field, nil
	case avro.TypeFloat32:
		var field float32
		return &field, nil
	case avro.TypeString:
		var field string
		return &field, nil
	case avro.TypeBytes, avro.TypeFixed, avro.Type(avro.LogicalTypeDecimal), avro.Type(avro.LogialTypeDuration):
		var field []byte
		return &field, nil
	case avro.TypeUnion:
		types := schema.(avro.UnionSchema)
		for _, t := range types {
			switch t.TypeName() {
			case avro.TypeNull:
				continue
			case avro.TypeUnion:
				return nil, ErrUnsupportedTypeForSQL
			default:
				return renderNativeField(t)
			}
		}
	}
	return nil, ErrUnsupportedTypeForSQL
}
