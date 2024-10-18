package sqlavro

import (
	"bytes"
	"fmt"
	"log"
	"reflect"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/khezen/avro"
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
	records := make([][]interface{}, 0, len(cfg.Schema.Fields))
	for _ = range cfg.Schema.Fields {
		records = append(records, make([]interface{}, 0, cfg.Limit))
	}
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
		for i, field := range cfg.Schema.Fields {
			records[i] = append(records[i], record[field.Name])
		}
	}
	return native2parquet(cfg, records)
}

func native2parquet(cfg QueryConfig, records [][]interface{}) (parquetBytes []byte, newCriteria []Criterion, err error) {
	// recordsLen := len(records)
	// if recordsLen > 0 && cfg.Criteria != nil {
	// 	newCriteria, err = criteriaFromNative(cfg.Schema, records[recordsLen-1], cfg.Criteria)
	// 	if err != nil {
	// 		return nil, nil, err
	// 	}
	// } else {
	// 	newCriteria = cfg.Criteria
	// }
	arrowSchema, err := avroSchema2arrowSchema(cfg.Schema)
	if err != nil {
		return nil, nil, err
	}
	parquetBytes, err = nativeRecordToArrowRecord(cfg.Schema, arrowSchema, records)
	if err != nil {
		return nil, nil, err
	}
	return parquetBytes, nil, nil

}

func avroSchema2arrowSchema(avroSchema *avro.RecordSchema) (*arrow.Schema, error) {
	nodes := make([]arrow.Field, 0, len(avroSchema.Fields))
	for _, field := range avroSchema.Fields {
		node, err := avroField2arrowField(field.Name, field.Type)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	arrowSchema := arrow.NewSchema(nodes, nil)
	return arrowSchema, nil
}

func avroField2arrowField(name string, field avro.Schema) (node arrow.Field, err error) {
	switch field.TypeName() {
	case avro.TypeBoolean:
		node = arrow.Field{Name: name, Type: arrow.FixedWidthTypes.Boolean}
	case avro.TypeInt32, avro.TypeInt64:
		node = arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int32}
	case avro.TypeFloat32:
		node = arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float32}
	case avro.TypeFloat64:
		node = arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float64}
	case avro.TypeString, avro.TypeEnum:
		node = arrow.Field{Name: name, Type: arrow.BinaryTypes.String}
	case avro.TypeBytes:
		node = arrow.Field{Name: name, Type: arrow.BinaryTypes.Binary}
	case avro.TypeFixed:
		length := field.(*avro.FixedSchema).Size
		node = arrow.Field{Name: name, Type: &arrow.FixedSizeBinaryType{ByteWidth: length}}
	case avro.TypeUnion:
		union := *field.(*avro.UnionSchema)
		valid := false
		for i := range union {
			valid = union[i].TypeName() != avro.TypeNull
			if valid {
				node, err = avroField2arrowField(name, union[i])
				break
			}
		}
		if !valid {
			err = fmt.Errorf("unsupported type %s for field %s", field.TypeName(), name)
		}
	default:
		err = fmt.Errorf("unsupported type %s for field %s", field.TypeName(), name)
	}
	return
}

func nativeRecordToArrowRecord(avroSchema *avro.RecordSchema, arrowSchema *arrow.Schema, records [][]interface{}) (parquetBytes []byte, err error) {
	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, arrowSchema)
	defer builder.Release()
	for i := range records {
		err := nativeField2arrowField(avroSchema.Fields[i].Type, builder.Fields()[i], records[i])
		if err != nil {
			return nil, err
		}
	}
	record := builder.NewRecord()
	defer record.Release()
	var buf bytes.Buffer
	pw, err := pqarrow.NewFileWriter(
		arrowSchema,
		&buf,
		nil,
		pqarrow.DefaultWriterProps(),
	)
	if err != nil {
		log.Fatalf("failed to create parquet writer: %v", err)
	}
	defer pw.Close()
	err = pw.Write(record)
	if err != nil {
		return nil, err
	}
	err = pw.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func nativeField2arrowField(avroField avro.Schema, builder array.Builder, records []interface{}) (err error) {
	switch avroField.TypeName() {
	case avro.TypeBoolean:
		for i := range records {
			if isNil(records[i]) {
				builder.AppendNull()
			} else {
				builder.(*array.BooleanBuilder).Append(records[i].(bool))
			}
		}
	case avro.TypeInt32, avro.TypeInt64:
		for i := range records {
			if isNil(records[i]) {
				builder.AppendNull()
			} else {
				builder.(*array.Int32Builder).Append(records[i].(int32))
			}
		}
	case avro.TypeFloat32:
		for i := range records {
			if isNil(records[i]) {
				builder.AppendNull()
			} else {
				builder.(*array.Float32Builder).Append(records[i].(float32))
			}
		}
	case avro.TypeFloat64:
		for i := range records {
			if isNil(records[i]) {
				builder.AppendNull()
			} else {
				builder.(*array.Float64Builder).Append(records[i].(float64))
			}
		}
	case avro.TypeString, avro.TypeEnum:
		for i := range records {
			if isNil(records[i]) {
				builder.AppendNull()
			} else {
				builder.(*array.StringBuilder).Append(records[i].(string))
			}
		}
	case avro.TypeBytes:
		for i := range records {
			if isNil(records[i]) {
				builder.AppendNull()
			} else {
				builder.(*array.BinaryBuilder).Append(records[i].([]byte))
			}
		}
	case avro.TypeFixed:
		for i := range records {
			if isNil(records[i]) {
				builder.AppendNull()
			} else {
				builder.(*array.FixedSizeBinaryBuilder).Append(records[i].([]byte))
			}
		}
	case avro.TypeUnion:
		union := *avroField.(*avro.UnionSchema)
		valid := false
		for i := range union {
			valid = union[i].TypeName() != avro.TypeNull
			if valid {
				err = nativeField2arrowField(union[i], builder, records)
				break
			}
		}
		if !valid {
			err = fmt.Errorf("unsupported type %s", avroField.TypeName())
		}
	default:
		err = fmt.Errorf("unsupported type %s", avroField.TypeName())
	}
	return err
}

func isNil(i interface{}) bool {
	if i == nil {
		return true
	}
	v := reflect.ValueOf(i)
	switch v.Kind() {
	case reflect.Ptr, reflect.Slice, reflect.Map, reflect.Chan, reflect.Interface, reflect.Func:
		return v.IsNil()
	}
	return false
}
