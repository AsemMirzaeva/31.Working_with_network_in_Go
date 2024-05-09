package main

import (
    "context"
    "database/sql"
    "fmt"
    "time"
	"log"

    _ "github.com/lib/pq"
)

type Data struct {
    ID        int
    Generated int
}

func main() {
  db, err := sql.Open("postgres", "user=postgres dbname=nt password=1234 host=localhost port=5432 sslmode=disable")
  if err != nil {
    log.Fatal(err)
  }
  defer db.Close()

  err = db.Ping()
  if err != nil {
    log.Fatal(err)
  }

  _, err = db.Exec(`
  CREATE TABLE large_dataset (
    id SERIAL PRIMARY KEY,
    generated INT
);`)
  if err != nil {
    log.Fatal(err)
  }

  _, err = db.Exec(`
  INSERT INTO large_dataset (generated)
  SELECT generate_series(1, 10000000);
  `)
  if err != nil {
    log.Fatal(err)
  }

  ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
  defer cancel()

  rows, err := db.QueryContext(ctx, "SELECT id, generated FROM large_dataset")
  if err != nil {
	  panic(err)
  }
  defer rows.Close()

  for rows.Next() {
	  var data Data
	  err := rows.Scan(&data.ID, &data.Generated)
	  if err != nil {
		  panic(err)
	  }
	  fmt.Printf("ID: %d, Generated: %d\n", data.ID, data.Generated)
  }
}
