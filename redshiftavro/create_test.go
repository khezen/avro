package redshiftavro

import (
	"encoding/json"
	"testing"

	"github.com/khezen/avro"
)

func TestCreate(t *testing.T) {
	schemaBytes := []byte(`{"type":"record","namespace":"dbTest","name":"table1","fields":[{"name":"some_char","type":{"type":"fixed","name":"some_char","size":108}},{"name":"some_varchar","type":"string"},{"name":"some_bolb","type":["null","bytes"]},{"name":"some_int","type":"int","default":18},{"name":"some_bigint","type":"long"},{"name":"some_float","type":"float"},{"name":"some_double","type":"double"},{"name":"some_decimal","type":{"type":"bytes","logicalType":"decimal","precision":8,"scale":8}},{"name":"date","type":{"type":"int","logicalType":"date"}},{"name":"time","type":{"type":"int","logicalType":"time"}},{"name":"datetime","type":{"type":"int","doc":"datetime","logicalType":"timestamp"}},{"name":"date_default","type":{"type":"int","logicalType":"date"},"default":0},{"name":"time_default","type":{"type":"int","logicalType":"time"},"default":0},{"name":"timestamp","type":{"type":"int","doc":"timestamp","logicalType":"timestamp"}},{"name":"ID","type":"int"},{"name":"title","type":"string"},{"name":"body","type":"bytes"},{"name":"content_type","type":["string","null"],"default":"text/markdown; charset=UTF-8"},{"name":"post_date","type":{"type":"int","doc":"datetime","logicalType":"timestamp"}},{"name":"update_date","type":["null",{"type":"int","doc":"datetime","logicalType":"timestamp"}]},{"name":"reading_time_minutes","type":["null",{"type":"bytes","logicalType":"decimal","precision":3,"scale":1}]}]}`)
	var anySchema avro.AnySchema
	err := json.Unmarshal(schemaBytes, &anySchema)
	if err != nil {
		t.Fatal(err)
	}
	schema := anySchema.Schema().(*avro.RecordSchema)
	cfg := CreateConfig{
		Schema: *schema,
		SortKeys: []string{
			"timestamp",
		},
		IfNotExists: true,
	}
	statement, err := CreateTableStatement(cfg)
	if err != nil {
		t.Fatal(err)
	}
	expectedStatement := `CREATE TABLE IF NOT EXISTS table1(some_char CHAR(108) ENCODE ZSTD NOT NULL,some_varchar VARCHAR(65535) ENCODE ZSTD NOT NULL,some_bolb VARCHAR(65535) ENCODE ZSTD NULL,some_int INTEGER ENCODE LZO NOT NULL,some_bigint BIGINT ENCODE LZO NOT NULL,some_float REAL ENCODE RAW NOT NULL,some_double DOUBLE PRECISION ENCODE RAW NOT NULL,some_decimal DECIMAL(8,8) ENCODE RAW NOT NULL,date Date ENCODE LZO NOT NULL,time TIMESTAMP WITHOUT TIME ZONE ENCODE LZO NOT NULL,datetime TIMESTAMP WITHOUT TIME ZONE ENCODE LZO NOT NULL,date_default Date ENCODE LZO NOT NULL,time_default TIMESTAMP WITHOUT TIME ZONE ENCODE LZO NOT NULL,timestamp TIMESTAMP WITHOUT TIME ZONE ENCODE RAW NOT NULL,ID INTEGER ENCODE LZO NOT NULL,title VARCHAR(65535) ENCODE ZSTD NOT NULL,body VARCHAR(65535) ENCODE ZSTD NOT NULL,content_type VARCHAR(65535) ENCODE ZSTD NULL,post_date TIMESTAMP WITHOUT TIME ZONE ENCODE LZO NOT NULL,update_date TIMESTAMP WITHOUT TIME ZONE ENCODE LZO NULL,reading_time_minutes DECIMAL(3,1) ENCODE RAW NULL) SORTKEY(timestamp);`
	if statement != expectedStatement {
		t.Errorf("expected:\n%s\n\ngot:\n%s", expectedStatement, statement)
	}
}
