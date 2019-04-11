package sqlavro

import (
	"database/sql"

	"github.com/khezen/avro"
)

// SQLDatabase2AVRO -
func SQLDatabase2AVRO(db *sql.DB, dbName string) ([]avro.Schema, error) {
	tables, err := getTables(db, dbName)
	if err != nil {
		return nil, err
	}
	var (
		tableName string
		schema    avro.Schema
		schemas   = make([]avro.Schema, 0, len(tables))
	)
	for _, tableName = range tables {
		schema, err = SQLTable2AVRO(db, dbName, tableName)
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, schema)
	}
	return schemas, nil
}

func getTables(db *sql.DB, dbName string) ([]string, error) {
	rows, err := db.Query(
		`SELECT TABLE_NAME 
		 FROM INFORMATION_SCHEMA.TABLES 
		 WHERE TABLE_SCHEMA=?`,
		dbName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var (
		tableName string
		tables    = make([]string, 0, 50)
	)
	for rows.Next() {
		err = rows.Scan(&tableName)
		if err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}
	return tables, nil
}
