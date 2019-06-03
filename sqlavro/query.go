package sqlavro

import (
	"database/sql"

	"github.com/khezen/avro"
)

// Query -
func Query(cfg QueryConfig) (avroBytes []byte, newCriteria []Criterion, err error) {
	err = cfg.Verify()
	if err != nil {
		return nil, nil, err
	}
	switch cfg.Output {
	case outputAVRO, "":
		avroBytes, newCriteria, err = query2AVRO(cfg)
	case outputCSV:
		avroBytes, newCriteria, err = query2CSV(cfg)
	}
	return avroBytes, newCriteria, err
}

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
	// "null", "deflate" and snappy are accepted.
	// If the value is empty, it is assumed to be "null"
	Compression string
	// Output - define the desired format for the output
	// AVRO and CSV are supported
	// if not set, then AVRO is the default choice
	Output string
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
	if qc.Output != "" {
		qc.Output = outputAVRO
	}
	if qc.Output != "" && qc.Output != outputAVRO && qc.Output != outputCSV {
		return ErrUnsupportedOutput
	}
	return nil
}

var (
	outputAVRO = "avro"
	outputCSV  = "csv"
)
