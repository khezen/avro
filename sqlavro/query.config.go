package sqlavro

import (
	"database/sql"

	"github.com/khezen/avro"
)

// QueryConfig -
type QueryConfig struct {
	// DB - Required SQL connection pool used to access the database.
	DB *sql.DB
	// DBName - Required name of the database to select
	DBName string
	// Schema - Required avro Record Schema matching the table to query data from.
	Schema *avro.RecordSchema
	// Limit - Optional limit in the number of record to be retrieved.
	// 10000 is used qs defqult if not set
	Limit int
	// Criteria - Optional list of criterion to retreve data from.
	Criteria []Criterion
	// Compression -  Optional name of the compression codec used to compress blocks
	// "null", "deflate" qnd snappy are accepted.
	// If the value is empty, it is assumed to be "null"
	Compression string
}

// Verify and ensure the config is valid
func (qc *QueryConfig) Verify() error {
	if qc.DB == nil {
		return ErrQueryConfigMissingDB
	}
	if qc.DBName == "" {
		return ErrQueryConfigMissingDBName
	}
	if qc.Schema == nil {
		return ErrQueryConfigMissingSchema
	}
	if qc.Compression != "" && qc.Compression != avro.CompressionNull && qc.Compression != avro.CompressionDeflate && qc.Compression != avro.CompressionSnappy {
		return avro.ErrUnsupportedCompression
	}
	return nil
}
