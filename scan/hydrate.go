package scan

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/sphireinc/quarry"
)

var (
	scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	timeType    = reflect.TypeOf(time.Time{})
)

// ScanOne is a compatibility alias for One.
func ScanOne[T any](ctx context.Context, db Queryer, q quarry.SQLer) (T, error) {
	return One[T](ctx, db, q)
}

// ScanAll is a compatibility alias for All.
func ScanAll[T any](ctx context.Context, db Queryer, q quarry.SQLer) ([]T, error) {
	return All[T](ctx, db, q)
}

// One renders q and scans exactly one row, returning an error when the result is empty or ambiguous.
// This keeps the row-count contract explicit instead of silently picking a row.
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
	switch len(values) {
	case 0:
		var zero T
		return zero, fmt.Errorf("quarry scan: no rows")
	case 1:
		return values[0], nil
	default:
		var zero T
		return zero, fmt.Errorf("quarry scan: expected exactly one row")
	}
}

// MaybeOne renders q and returns nil when no rows are present.
// Callers still get an error if more than one row is returned.
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
	switch len(values) {
	case 0:
		return nil, nil
	case 1:
		return &values[0], nil
	default:
		return nil, fmt.Errorf("quarry scan: expected at most one row")
	}
}

// All renders q and scans every row into a slice.
// It returns an empty slice, not nil, for empty result sets.
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

// collectAll reads every row from the result set and hydrates them into T values.
// The helper keeps row iteration separate from the hydration logic so the behavior is easy to test.
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

// scanRow hydrates a single row into either a struct or a scalar value.
// Structs are matched by column name; scalars are scanned directly.
func scanRow[T any](rows *sql.Rows, columns []string) (T, error) {
	var zero T
	targetType := reflect.TypeOf(zero)
	if targetType == nil {
		return zero, fmt.Errorf("quarry scan: unsupported nil target")
	}
	if targetType.Kind() == reflect.Pointer {
		return zero, fmt.Errorf("quarry scan: unsupported pointer target %s", targetType)
	}

	if targetType.Kind() != reflect.Struct {
		return scanIntoValue[T](rows)
	}

	bindings, err := buildStructBindings(targetType, columns)
	if err != nil {
		return zero, err
	}

	target := reflect.New(targetType).Elem()
	destinations := make([]any, len(columns))
	for i, binding := range bindings {
		switch binding.mode {
		case bindUnknown:
			destinations[i] = new(any)
		case bindDirect:
			destinations[i] = target.FieldByIndex(binding.index).Addr().Interface()
		case bindPointer:
			destinations[i] = new(any)
		default:
			return zero, fmt.Errorf("quarry scan: unsupported binding mode")
		}
	}

	if err := rows.Scan(destinations...); err != nil {
		return zero, fmt.Errorf("quarry scan: scan row: %w", err)
	}

	for i, binding := range bindings {
		if binding.mode != bindPointer {
			continue
		}
		field := target.FieldByIndex(binding.index)
		raw := *(destinations[i].(*any))
		if err := assignRawToField(field, raw); err != nil {
			return zero, err
		}
	}

	return target.Interface().(T), nil
}

// scanIntoValue scans a row into a non-struct destination.
func scanIntoValue[T any](rows *sql.Rows) (T, error) {
	var out T
	if err := rows.Scan(&out); err != nil {
		var zero T
		return zero, fmt.Errorf("quarry scan: scan row: %w", err)
	}
	return out, nil
}

type bindingMode int

const (
	bindUnknown bindingMode = iota
	bindDirect
	bindPointer
)

type columnBinding struct {
	index []int
	mode  bindingMode
}

// buildStructBindings maps columns onto struct fields while ignoring unknown columns.
// A column may target only one field; duplicate mappings are treated as ambiguous.
func buildStructBindings(targetType reflect.Type, columns []string) ([]columnBinding, error) {
	fieldMap := make(map[string][]int)
	if err := buildFieldMap(targetType, nil, fieldMap); err != nil {
		return nil, err
	}

	bindings := make([]columnBinding, len(columns))
	seen := make(map[string]string)
	for i, column := range columns {
		idx, ok := fieldMap[strings.ToLower(column)]
		if !ok {
			bindings[i] = columnBinding{mode: bindUnknown}
			continue
		}
		key := indexKey(idx)
		if prev, exists := seen[key]; exists {
			return nil, fmt.Errorf("quarry scan: duplicate column mapping for %q and %q", prev, column)
		}
		seen[key] = column

		fieldType, err := fieldTypeByIndex(targetType, idx)
		if err != nil {
			return nil, err
		}
		mode, err := scanModeForType(fieldType)
		if err != nil {
			return nil, err
		}
		bindings[i] = columnBinding{index: idx, mode: mode}
	}
	return bindings, nil
}

// buildFieldMap indexes exported fields, including anonymous embedded structs.
// db tags win first, then json tags, then snake_case names.
func buildFieldMap(t reflect.Type, prefix []int, fieldMap map[string][]int) error {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" && !field.Anonymous {
			continue
		}
		idx := append(append([]int(nil), prefix...), i)
		ft := field.Type
		for ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		if field.Anonymous && ft.Kind() == reflect.Struct {
			if err := buildFieldMap(ft, idx, fieldMap); err != nil {
				return err
			}
			continue
		}
		if name, ok := fieldName(field); ok {
			key := strings.ToLower(name)
			if existing, exists := fieldMap[key]; exists && !sameIndex(existing, idx) {
				return fmt.Errorf("quarry scan: duplicate field mapping for %q", name)
			}
			fieldMap[key] = idx
		}
	}
	return nil
}

// fieldName resolves the scan name for a field using db, json, then snake_case.
// The tag resolution stays intentionally simple so the behavior is obvious in docs and tests.
func fieldName(field reflect.StructField) (string, bool) {
	if tag := field.Tag.Get("db"); tag != "" {
		if tag == "-" {
			return "", false
		}
		return strings.Split(tag, ",")[0], true
	}
	if tag := field.Tag.Get("json"); tag != "" {
		if tag == "-" {
			return "", false
		}
		return strings.Split(tag, ",")[0], true
	}
	return toSnakeCase(field.Name), true
}

// toSnakeCase converts CamelCase identifiers into snake_case bindings.
func toSnakeCase(s string) string {
	runes := []rune(s)
	var out strings.Builder
	for i, r := range runes {
		if unicode.IsUpper(r) {
			if i > 0 {
				prev := runes[i-1]
				nextLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
				if unicode.IsLower(prev) || unicode.IsDigit(prev) || nextLower {
					out.WriteByte('_')
				}
			}
			out.WriteRune(unicode.ToLower(r))
			continue
		}
		out.WriteRune(r)
	}
	return out.String()
}

// fieldTypeByIndex resolves the concrete field type for a scan index path.
func fieldTypeByIndex(t reflect.Type, index []int) (reflect.Type, error) {
	return t.FieldByIndex(index).Type, nil
}

// indexKey turns a reflection index path into a stable string key.
func indexKey(index []int) string {
	if len(index) == 0 {
		return ""
	}
	var b strings.Builder
	for i, n := range index {
		if i > 0 {
			b.WriteByte('.')
		}
		b.WriteString(strconv.Itoa(n))
	}
	return b.String()
}

// sameIndex reports whether two reflection index paths refer to the same field.
func sameIndex(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// scanModeForType determines whether a field should scan directly or via a temporary pointer.
// Pointer targets are staged through an intermediate value so nil database values can be handled cleanly.
func scanModeForType(t reflect.Type) (bindingMode, error) {
	if t.Kind() == reflect.Pointer {
		if !isSupportedScanType(t.Elem()) {
			return bindUnknown, fmt.Errorf("quarry scan: unsupported field type %s", t)
		}
		return bindPointer, nil
	}
	if !isSupportedScanType(t) {
		return bindUnknown, fmt.Errorf("quarry scan: unsupported field type %s", t)
	}
	return bindDirect, nil
}

// isSupportedScanType keeps the scanner honest without forcing ORM-style magic.
func isSupportedScanType(t reflect.Type) bool {
	if t == nil {
		return false
	}
	if t == timeType {
		return true
	}
	if reflect.PointerTo(t).Implements(scannerType) {
		return true
	}
	switch t.Kind() {
	case reflect.Bool,
		reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.Slice:
		return t.Kind() != reflect.Slice || (t.Elem().Kind() == reflect.Uint8)
	default:
		return false
	}
}

// assignRawToField applies a scanned raw value to a pointer or scalar field.
// The pointer case keeps nil database values as nil pointers instead of zero values.
func assignRawToField(field reflect.Value, raw any) error {
	if !field.CanSet() {
		return fmt.Errorf("quarry scan: unsupported field type %s", field.Type())
	}
	if field.Kind() == reflect.Pointer {
		if raw == nil {
			field.SetZero()
			return nil
		}
		elem := reflect.New(field.Type().Elem()).Elem()
		if err := assignRawToValue(elem, raw); err != nil {
			return err
		}
		ptr := reflect.New(field.Type().Elem())
		ptr.Elem().Set(elem)
		field.Set(ptr)
		return nil
	}
	return assignRawToValue(field, raw)
}

// assignRawToValue converts a raw database value into the target field.
// The conversion logic is intentionally conservative so unsupported types fail loudly.
func assignRawToValue(dst reflect.Value, raw any) error {
	if !dst.CanSet() {
		return fmt.Errorf("quarry scan: unsupported field type %s", dst.Type())
	}
	if scanner, ok := dst.Addr().Interface().(sql.Scanner); ok {
		return scanner.Scan(raw)
	}
	if raw == nil {
		dst.SetZero()
		return nil
	}

	rv := reflect.ValueOf(raw)
	if rv.Type().AssignableTo(dst.Type()) {
		dst.Set(rv)
		return nil
	}
	if rv.Type().ConvertibleTo(dst.Type()) {
		dst.Set(rv.Convert(dst.Type()))
		return nil
	}

	switch dst.Kind() {
	case reflect.String:
		switch x := raw.(type) {
		case string:
			dst.SetString(x)
		case []byte:
			dst.SetString(string(x))
		default:
			dst.SetString(fmt.Sprint(x))
		}
		return nil
	case reflect.Bool:
		switch x := raw.(type) {
		case bool:
			dst.SetBool(x)
		case int64:
			dst.SetBool(x != 0)
		case float64:
			dst.SetBool(x != 0)
		case []byte:
			v, err := strconv.ParseBool(string(x))
			if err != nil {
				return fmt.Errorf("quarry scan: convert %T to bool: %w", raw, err)
			}
			dst.SetBool(v)
		case string:
			v, err := strconv.ParseBool(x)
			if err != nil {
				return fmt.Errorf("quarry scan: convert %T to bool: %w", raw, err)
			}
			dst.SetBool(v)
		default:
			return fmt.Errorf("quarry scan: unsupported field type %s", dst.Type())
		}
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch x := raw.(type) {
		case int64:
			dst.SetInt(x)
		case float64:
			dst.SetInt(int64(x))
		case []byte:
			v, err := strconv.ParseInt(string(x), 10, 64)
			if err != nil {
				return fmt.Errorf("quarry scan: convert %T to int: %w", raw, err)
			}
			dst.SetInt(v)
		case string:
			v, err := strconv.ParseInt(x, 10, 64)
			if err != nil {
				return fmt.Errorf("quarry scan: convert %T to int: %w", raw, err)
			}
			dst.SetInt(v)
		default:
			return fmt.Errorf("quarry scan: unsupported field type %s", dst.Type())
		}
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch x := raw.(type) {
		case int64:
			dst.SetUint(uint64(x))
		case float64:
			dst.SetUint(uint64(x))
		case []byte:
			v, err := strconv.ParseUint(string(x), 10, 64)
			if err != nil {
				return fmt.Errorf("quarry scan: convert %T to uint: %w", raw, err)
			}
			dst.SetUint(v)
		case string:
			v, err := strconv.ParseUint(x, 10, 64)
			if err != nil {
				return fmt.Errorf("quarry scan: convert %T to uint: %w", raw, err)
			}
			dst.SetUint(v)
		default:
			return fmt.Errorf("quarry scan: unsupported field type %s", dst.Type())
		}
		return nil
	case reflect.Float32, reflect.Float64:
		switch x := raw.(type) {
		case int64:
			dst.SetFloat(float64(x))
		case float64:
			dst.SetFloat(x)
		case []byte:
			v, err := strconv.ParseFloat(string(x), 64)
			if err != nil {
				return fmt.Errorf("quarry scan: convert %T to float: %w", raw, err)
			}
			dst.SetFloat(v)
		case string:
			v, err := strconv.ParseFloat(x, 64)
			if err != nil {
				return fmt.Errorf("quarry scan: convert %T to float: %w", raw, err)
			}
			dst.SetFloat(v)
		default:
			return fmt.Errorf("quarry scan: unsupported field type %s", dst.Type())
		}
		return nil
	case reflect.Slice:
		if dst.Type().Elem().Kind() == reflect.Uint8 {
			switch x := raw.(type) {
			case []byte:
				dst.SetBytes(append([]byte(nil), x...))
				return nil
			case string:
				dst.SetBytes([]byte(x))
				return nil
			}
		}
	case reflect.Struct:
		if dst.Type() == timeType {
			if tt, ok := raw.(time.Time); ok {
				dst.Set(reflect.ValueOf(tt))
				return nil
			}
		}
	}

	return fmt.Errorf("quarry scan: unsupported field type %s", dst.Type())
}
