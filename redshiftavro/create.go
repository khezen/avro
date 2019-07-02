package redshiftavro

import (
	"bytes"

	"github.com/khezen/avro"

	"github.com/khezen/avro/sqlavro"
)

// CreateTableStatement -
func CreateTableStatement(cfg CreateConfig) (string, error) {
	buf := bytes.NewBufferString("CREATE TABLE ")
	if cfg.IfNotExists {
		buf.WriteString("IF NOT EXISTS ")
	}
	tableName := sqlavro.SQLEscape(cfg.Schema.Name)
	buf.WriteString(tableName)
	buf.WriteRune('(')
	var (
		fieldsLen  = len(cfg.Schema.Fields)
		i          int
		field      avro.RecordFieldSchema
		columnStmt string
		err        error
	)
	for i, field = range cfg.Schema.Fields {
		columnStmt, err = createColumnStatement(field)
		if err != nil {
			return "", err
		}
		buf.WriteString(columnStmt)
		if i < fieldsLen-1 {
			buf.WriteRune(',')
		}
	}
	buf.WriteRune(')')
	return buf.String(), nil
}

func createColumnStatement(field avro.RecordFieldSchema) (string, error) {
	var (
		columnName = sqlavro.SQLEscape(field.Name)
		buf        = bytes.NewBufferString(columnName)
	)

	return buf.String(), nil
}

func avro2RedshiftType(field avro.RecordFieldSchema)() {

}
