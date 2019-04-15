package sqlavro_test

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/khezen/avro/sqlavro"
)

func exampleSQLDatabase2Avro() {
	db, err := sql.Open("mysql", "root@/blog")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	_, err = db.Exec(
		`CREATE TABLE posts(
			ID INT NOT NULL,
			title VARCHAR(128) NOT NULL,
			body LONGBLOB NOT NULL,
			content_type VARCHAR(128) DEFAULT 'text/markdown; charset=UTF-8',
			post_date DATETIME NOT NULL DEFAULT NOW(),
			update_date DATETIME,
			reading_time_minutes DECIMAL(3,1),
			PRIMARY KEY(ID)
		)`,
	)
	if err != nil {
		panic(err)
	}
	schemas, err := sqlavro.SQLDatabase2AVRO(db, "blog")
	if err != nil {
		panic(err)
	}
	schemasBytes, err := json.Marshal(schemas)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(schemasBytes))
}

// [
//     {
//         "type": "record",
//         "namespace": "blog",
//         "name": "posts",
//         "fields": [
//             {
//                 "name": "ID",
//                 "type": "int"
//             },
//             {
//                 "name": "title",
//                 "type": "string"
//             },
//             {
//                 "name": "body",
//                 "type": "bytes"
//             },
//             {
//                 "name": "content_type",
//                 "type": [
//                     "null",
//                     "string"
//                 ],
//                 "default": "text/markdown; charset=UTF-8"
//             },
//             {
//                 "name": "post_date",
//                 "type": {
//                     "type": "int",
//                     "doc":"datetime",
//                     "logicalType": "timestamp"
//                 }
//             },
//             {
//                 "name": "update_date",
//                 "type": [
//                     "null",
//                     {
//                         "type": "int",
//                         "doc":"datetime",
//                         "logicalType": "timestamp"
//                     }
//                 ]
//             },
//             {
//                 "name": "reading_time_minutes",
//                 "type": [
//                     "null",
//                     {
//                         "type": "bytes",
//                         "logicalType": "decimal",
//                         "precision": 3,
//                         "scale": 1
//                     }
//                 ]
//             }
//         ]
//     }
// ]
