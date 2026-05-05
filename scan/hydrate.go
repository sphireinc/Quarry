package scan

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/sphireinc/quarry"
)

func One[T any](ctx context.Context, db Queryer, q quarry.SQLer) (T, error) {
	rows, err := Query(ctx, db, q)
	if err != nil {
		var zero T
		return zero, err
	}
	defer rows.Close()

	values, err := collectAll[T](rows)
	if err != nil {
		var zero T
		return zero, err
	}
	if len(values) == 0 {
		var zero T
		return zero, fmt.Errorf("quarry scan: no rows")
	}
	if len(values) > 1 {
		var zero T
		return zero, fmt.Errorf("quarry scan: expected exactly one row")
	}
	return values[0], nil
}

func MaybeOne[T any](ctx context.Context, db Queryer, q quarry.SQLer) (*T, error) {
	rows, err := Query(ctx, db, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	values, err := collectAll[T](rows)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) > 1 {
		return nil, fmt.Errorf("quarry scan: expected at most one row")
	}
	return &values[0], nil
}

func All[T any](ctx context.Context, db Queryer, q quarry.SQLer) ([]T, error) {
	rows, err := Query(ctx, db, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	values, err := collectAll[T](rows)
	if err != nil {
		return nil, err
	}
	if values == nil {
		return []T{}, nil
	}
	return values, nil
}

func collectAll[T any](rows *sql.Rows) ([]T, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("quarry scan: columns: %w", err)
	}

	var out []T
	for rows.Next() {
		value, err := scanRow[T](rows, columns)
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("quarry scan: rows: %w", err)
	}
	return out, nil
}

func scanRow[T any](rows *sql.Rows, columns []string) (T, error) {
	var zero T
	targetType := reflect.TypeOf(zero)
	if targetType == nil {
		return zero, fmt.Errorf("quarry scan: unsupported nil target")
	}

	if targetType.Kind() != reflect.Struct {
		return scanIntoValue[T](rows)
	}

	dest, err := structDestinations(targetType, columns)
	if err != nil {
		return zero, err
	}
	target := reflect.New(targetType).Elem()
	for i, idx := range dest.indices {
		destPtr := target.FieldByIndex(idx).Addr().Interface()
		dest.destinations[i] = destPtr
	}
	if err := rows.Scan(dest.destinations...); err != nil {
		return zero, fmt.Errorf("quarry scan: scan row: %w", err)
	}
	return target.Interface().(T), nil
}

func scanIntoValue[T any](rows *sql.Rows) (T, error) {
	var out T
	if err := rows.Scan(&out); err != nil {
		var zero T
		return zero, fmt.Errorf("quarry scan: scan row: %w", err)
	}
	return out, nil
}

type structScanPlan struct {
	destinations []any
	indices      [][]int
}

func structDestinations(targetType reflect.Type, columns []string) (structScanPlan, error) {
	fieldMap := make(map[string][]int)
	buildFieldMap(targetType, nil, fieldMap)

	plan := structScanPlan{
		destinations: make([]any, len(columns)),
		indices:      make([][]int, len(columns)),
	}
	for i, column := range columns {
		idx, ok := fieldMap[strings.ToLower(column)]
		if !ok {
			return structScanPlan{}, fmt.Errorf(`quarry scan: hydrate row: missing destination for column %q`, column)
		}
		plan.indices[i] = idx
	}
	return plan, nil
}

func buildFieldMap(t reflect.Type, prefix []int, fieldMap map[string][]int) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" && !field.Anonymous {
			continue
		}
		idx := append(append([]int(nil), prefix...), i)
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			buildFieldMap(field.Type, idx, fieldMap)
			continue
		}
		if name, ok := fieldName(field); ok {
			fieldMap[name] = idx
		}
	}
}

func fieldName(field reflect.StructField) (string, bool) {
	if tag := field.Tag.Get("db"); tag != "" {
		if tag == "-" {
			return "", false
		}
		return strings.ToLower(tag), true
	}
	if tag := field.Tag.Get("json"); tag != "" {
		if tag == "-" {
			return "", false
		}
		return strings.ToLower(strings.Split(tag, ",")[0]), true
	}
	return strings.ToLower(toSnakeCase(field.Name)), true
}

func toSnakeCase(s string) string {
	var out strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			out.WriteByte('_')
		}
		out.WriteRune(r)
	}
	return strings.ToLower(out.String())
}
