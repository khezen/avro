package sqlavro_test

import (
	"database/sql"
	"io/ioutil"
	"time"

	"github.com/khezen/avro"

	"github.com/khezen/avro/sqlavro"
)

func ExampleQuery() {
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
			post_date DATETIME NOT NULL,
			update_date DATETIME,
			reading_time_minutes DECIMAL(3,1),
			PRIMARY KEY(ID)
		)`,
	)
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(
		// statement
		`INSERT INTO posts(ID,title,body,content_type,post_date,update_date,reading_time_minutes)
		 VALUES (?,?,?,?,?,?,?)`,
		// values
		42,
		"lorem ispum",
		[]byte(`Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.`),
		"text/markdown; charset=UTF-8",
		"2009-04-10 00:00:00",
		"2009-04-10 00:00:00",
		"4.2",
	)
	if err != nil {
		panic(err)
	}
	schema, err := sqlavro.SQLTable2AVRO(db, "blog", "posts")
	if err != nil {
		panic(err)
	}
	limit := 1000
	order := avro.Ascending
	from, err := time.Parse("2006-02-01 15:04:05", "2009-04-10 00:00:00")
	if err != nil {
		panic(err)
	}
	avroBytes, err := sqlavro.Query(db, "blog", schema, limit, *sqlavro.NewCriterionDateTime("post_date", &from, order))
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile("/tmp/blog_posts.avro", avroBytes, 0644)
	if err != nil {
		panic(err)
	}
}
