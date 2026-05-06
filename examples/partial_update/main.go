package main

import (
	"fmt"

	"github.com/sphireinc/quarry"
)

func main() {
	type UpdateUser struct {
		ID      int
		Name    string
		Email   string
		Enabled *bool
	}

	params := UpdateUser{
		ID:    7,
		Name:  "Quarry User",
		Email: "user@example.com",
	}
	enabled := true
	params.Enabled = &enabled

	qq := quarry.New(quarry.Postgres)
	sqlText, args, err := qq.Update("users").
		SetOptional("name", params.Name).
		SetOptional("email", params.Email).
		SetIf(params.Enabled != nil, "enabled", *params.Enabled).
		Where(quarry.Eq("id", params.ID)).
		Returning("id").
		ToSQL()
	if err != nil {
		panic(err)
	}

	fmt.Println(sqlText)
	fmt.Println(args)
}
