# *avro*

[![Build Status](http://img.shields.io/travis/khezen/avro.svg?style=flat-square)](https://travis-ci.org/khezen/avro) [![codecov](https://img.shields.io/codecov/c/github/khezen/avro/master.svg?style=flat-square)](https://codecov.io/gh/khezen/avro)
[![Go Report Card](https://goreportcard.com/badge/github.com/khezen/avro?style=flat-square)](https://goreportcard.com/report/github.com/khezen/avro)

The purpose of this package is to facilitate use of AVRO with `go` strong typing.

## Features:

### `github.com/khezen/avro`

[![GoDoc](https://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](https://godoc.org/github.com/khezen/avro)

* Marshal/Unmarshal AVRO schema

### `github.com/khezen/avro/sqlavro`

[![GoDoc](https://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](https://godoc.org/github.com/khezen/avro/sqlavro)

* Discover SQL tables 
* Convert SQL tables to AVRO schemas

## What is AVRO

[Apache AVRO](http://avro.apache.org/docs/current/spec.html) is a data serialization system which relies on JSON schemas.

It provides:

* Rich data structures
* A compact, fast, binary data format
* A container file, to store persistent data
* Remote procedure call (RPC)

AVRO binary encoded data comes together with its schema and therefore is fully self-describing.

When AVRO data is read, the schema used when writing it is always present. This permits each datum to be written with no per-value overheads, making serialization both fast and small.

When AVRO data is stored in a file, its schema is stored with it, so that files may be processed later by any program. If the program reading the data expects a different schema this can be easily resolved, since both schemas are present.

## Examples

### Schema Marshal/Unmarshal

```golang
import (
  "encoding/json"
  "fmt"

  "github.com/khezen/avro"
)

func main() {
  schemaBytes := []byte(
    `{
      "type": "record",
      "namespace": "test",
      "name": "LongList",
      "aliases": [
        "LinkedLongs"
      ],
      "doc": "linked list of 64 bits integers",
      "fields": [
        {
          "name": "value",
          "type": "long"
        },
        {
          "name": "next",
          "type": [
            "null",
            "LongList"
          ]
        }
      ]
    }`,
  )

  // Unmarshal JSON  bytes to Schema interface
  var anySchema avro.AnySchema
  err := json.Unmarshal(schemaBytes, &anySchema)
  if err != nil {
    panic(err)
  }
  schema := anySchema.Schema()  
  // Marshal Schema interface to JSON bytes
  schemaBytes, err = json.Marshal(schema)
  if err != nil {
    panic(err)
  }
  fmt.Println(string(schemaBytes))
}
// {
//     "type": "record",
//     "namespace": "test",
//     "name": "LongList",
//     "aliases": [
//         "LinkedLongs"
//     ],
//     "doc": "linked list of 64 bits integers",
//     "fields": [
//         {
//             "name": "value",
//             "type": "long"
//         },
//         {
//             "name": "next",
//             "type": [
//                 "null",
//                 "LongList"
//             ]
//         }
//     ]
// }
```

### Convert SQL Table to AVRO Schema

```golang
import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/khezen/avro/sqlavro"
)

func main() {
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
//                     "logicalType": "timestamp"
//                 }
//             },
//             {
//                 "name": "update_date",
//                 "type": [
//                     "null",
//                     {
//                         "type": "int",
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
```

## Issues

If you have any problems or questions, please ask for help through a [GitHub issue](https://github.com/khezen/avro/issues).

## Contributions

Help is always welcome! For example, documentation (like the text you are reading now) can always use improvement. There's always code that can be improved. If you ever see something you think should be fixed, you should own it. If you have no idea what to start on, you can browse the issues labeled with [help wanted](https://github.com/khezen/avro/labels/help%20wanted).

As a potential contributor, your changes and ideas are welcome at any hour of the day or night, weekdays, weekends, and holidays. Please do not ever hesitate to ask a question or send a pull request.

[Code of conduct](https://github.com/khezen/avro/blob/master/CODE_OF_CONDUCT.md).
