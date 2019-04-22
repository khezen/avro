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
	native, err := query2Native(db, schema, limit, criteria)
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

func query2Native(db *sql.DB, schema *avro.RecordSchema, limit int, criteria []Criterion) ([]map[string]interface{}, error) {
	statement, params, err := renderQuery(schema, limit, criteria)
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

func renderQuery(schema *avro.RecordSchema, limit int, criteria []Criterion) (statement string, params []interface{}, err error) {
	fieldsLen := len(schema.Fields)
	if fieldsLen == 0 {
		return "", nil, ErrExpectRecordSchema
	}
	err = ensureCriterionTypes(schema, criteria)
	if err != nil {
		return "", nil, err
	}
	params = make([]interface{}, 0, len(criteria)+2)
	qBuf := bytes.NewBufferString("SELECT ")
	for i := 0; i < fieldsLen-1; i++ {
		qBuf.WriteRune('`')
		qBuf.WriteString(sqlEscape(schema.Fields[i].Name))
		qBuf.WriteString("`,")
	}
	qBuf.WriteRune('`')
	qBuf.WriteString(sqlEscape(schema.Fields[fieldsLen-1].Name))
	qBuf.WriteString("` FROM `")
	if len(schema.Namespace) > 0 {
		qBuf.WriteString(sqlEscape(schema.Namespace))
		qBuf.WriteString("`.`")
	}
	qBuf.WriteString(sqlEscape(schema.Name))
	qBuf.WriteRune('`')
	criteriaLen := len(criteria)
	if criteriaLen == 0 {
		return qBuf.String(), params, nil
	}
	qBuf.WriteString("WHERE")
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
		if i < criteriaLen-1 {
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

func ensureCriterionTypes(schema *avro.RecordSchema, criteria []Criterion) (err error) {
	var (
		i     int
		match bool
		field avro.RecordFieldSchema
	)
	for i = range criteria {
		match = false
		for _, field = range schema.Fields {
			match = criteria[i].FieldName == field.Name
			if match {
				err = ensureCriterionType(&field, &criteria[i])
				if err != nil {
					return err
				}
				break
			}
		}
		if !match {
			return ErrCriterionUnknownField
		}
	}
	return nil
}

func ensureCriterionType(field *avro.RecordFieldSchema, criterion *Criterion) (err error) {
	var schema = field.Type
	if schema.TypeName() == avro.TypeUnion {
		union := schema.(avro.UnionSchema)
		schema, err = underlyingType(union)
		if err != nil {
			return err
		}
	}
	switch schema.TypeName() {
	case avro.TypeInt64, avro.TypeInt32,
		avro.TypeFloat64, avro.TypeFloat32,
		avro.TypeString,
		avro.Type(avro.LogicalTypeDate),
		avro.Type(avro.LogicalTypeTime):
		criterion.setType(schema.TypeName())
		break
	case avro.Type(avro.LogicalTypeTimestamp):
		switch schema.(*avro.DerivedPrimitiveSchema).Documentation {
		case string(DateTime):
			criterion.setType(avro.Type(avro.LogicalTypeTimestamp))
			break
		case "", string(Timestamp):
			criterion.setType(avro.Type(avro.TypeInt64))
			break
		default:
			return ErrUnsupportedTypeForCriterion
		}
		break
	default:
		return ErrUnsupportedTypeForCriterion
	}
	return nil
}
