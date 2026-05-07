// Package main prints a minimal Quarry SELECT query.
package main

import (
	"fmt"

	"github.com/sphireinc/quarry"
)

func main() {
	qq := quarry.New(quarry.Postgres)

	sqlText, args, err := qq.Select("id", "email").
		From("users").
		Where(quarry.Eq("status", "active")).
		ToSQL()
	if err != nil {
		panic(err)
	}

	fmt.Println(sqlText)
	fmt.Println(args)
}
