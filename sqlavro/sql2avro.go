package sqlavro

import (
	"database/sql"

	"github.com/khezen/avro"
)

// SQLDatabase2AVRO -
func SQLDatabase2AVRO(db *sql.DB) ([]avro.Schema, error) {
	return nil, nil
}

// SQLTable2AVRO -
func SQLTable2AVRO(db *sql.DB, tableName string) (avro.Schema, error) {
	return nil, nil
}
