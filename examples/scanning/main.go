package main

import (
	"context"
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"

	"github.com/sphireinc/quarry"
	"github.com/sphireinc/quarry/scan"
)

type User struct {
	ID     int    `db:"id"`
	Email  string `db:"email"`
	Status string `db:"status"`
}

func main() {
	ctx := context.Background()
	db, err := sql.Open("sqlite", "file:quarry-example?mode=memory&cache=shared")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, `
CREATE TABLE users (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	email TEXT NOT NULL,
	status TEXT NOT NULL
)`); err != nil {
		panic(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO users (email, status) VALUES (?, ?)`, "a@example.com", "active"); err != nil {
		panic(err)
	}

	qq := quarry.New(quarry.SQLite)
	users, err := scan.All[User](ctx, db, qq.Select("id", "email", "status").From("users").OrderBy("id ASC"))
	if err != nil {
		panic(err)
	}

	fmt.Println(users)
}
