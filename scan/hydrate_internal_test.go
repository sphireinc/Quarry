package scan

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/sphireinc/quarry"
)

type fieldMapInner struct {
	TenantID int `db:"tenant_id"`
}

type fieldMapOuter struct {
	*fieldMapInner
	Name   string `json:"name,omitempty"`
	Title  string
	Ignore string `db:"-"`
}

type duplicateFieldMap struct {
	First  int `db:"name"`
	Second int `db:"name"`
}

type myInt int64

type myString string

func TestFieldMappingHelpers(t *testing.T) {
	fieldMap := make(map[string][]int)
	if err := buildFieldMap(reflect.TypeOf(fieldMapOuter{}), nil, fieldMap); err != nil {
		t.Fatalf("build field map: %v", err)
	}
	if got := fieldMap["tenant_id"]; !reflect.DeepEqual(got, []int{0, 0}) {
		t.Fatalf("unexpected embedded index: %#v", got)
	}
	if got := fieldMap["name"]; !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("unexpected json index: %#v", got)
	}
	if got := fieldMap["title"]; !reflect.DeepEqual(got, []int{2}) {
		t.Fatalf("unexpected snake index: %#v", got)
	}
	if _, ok := fieldMap["ignore"]; ok {
		t.Fatal("expected ignored field to be omitted")
	}

	if err := buildFieldMap(reflect.TypeOf(duplicateFieldMap{}), nil, make(map[string][]int)); err == nil || !strings.Contains(err.Error(), "duplicate field mapping") {
		t.Fatalf("expected duplicate field error, got %v", err)
	}

	dbField, _ := reflect.TypeOf(fieldMapOuter{}).FieldByName("Title")
	if got, ok := fieldName(dbField); !ok || got != "title" {
		t.Fatalf("unexpected field name: %q %v", got, ok)
	}
	jsonField, _ := reflect.TypeOf(fieldMapOuter{}).FieldByName("Name")
	if got, ok := fieldName(jsonField); !ok || got != "name" {
		t.Fatalf("unexpected field name: %q %v", got, ok)
	}
	ignoredField, _ := reflect.TypeOf(fieldMapOuter{}).FieldByName("Ignore")
	if got, ok := fieldName(ignoredField); ok || got != "" {
		t.Fatalf("expected ignored field, got %q %v", got, ok)
	}

	if got := toSnakeCase("HTTPRequestID"); got != "http_request_id" {
		t.Fatalf("unexpected snake case: %s", got)
	}
	if got := indexKey([]int{0, 1, 2}); got != "0.1.2" {
		t.Fatalf("unexpected index key: %s", got)
	}
	if got := indexKey(nil); got != "" {
		t.Fatalf("unexpected empty index key: %q", got)
	}
	if !sameIndex([]int{1, 2}, []int{1, 2}) || sameIndex([]int{1, 2}, []int{2, 1}) || sameIndex([]int{1}, []int{1, 2}) {
		t.Fatal("unexpected sameIndex behavior")
	}
}

func TestScanModeAndTypeSupport(t *testing.T) {
	if !isSupportedScanType(reflect.TypeOf("")) {
		t.Fatal("expected string to be supported")
	}
	if !isSupportedScanType(reflect.TypeOf(time.Time{})) {
		t.Fatal("expected time.Time to be supported")
	}
	if !isSupportedScanType(reflect.TypeOf(sql.NullString{})) {
		t.Fatal("expected sql.NullString to be supported")
	}
	if isSupportedScanType(reflect.TypeOf(struct{}{})) {
		t.Fatal("expected struct to be unsupported")
	}
	if isSupportedScanType(nil) {
		t.Fatal("expected nil type to be unsupported")
	}

	if mode, err := scanModeForType(reflect.TypeOf(sql.NullString{})); err != nil || mode != bindDirect {
		t.Fatalf("unexpected scan mode: %v %v", mode, err)
	}
	if mode, err := scanModeForType(reflect.TypeOf((*sql.NullString)(nil))); err != nil || mode != bindPointer {
		t.Fatalf("unexpected scan mode: %v %v", mode, err)
	}
	if _, err := scanModeForType(reflect.TypeOf(struct{ Ch chan int }{}).Field(0).Type); err == nil || !strings.Contains(err.Error(), "unsupported field type") {
		t.Fatalf("expected unsupported field type error, got %v", err)
	}
}

func TestAssignRawHelpers(t *testing.T) {
	t.Run("assign_value", func(t *testing.T) {
		tests := []struct {
			name string
			dst  any
			raw  any
			want any
		}{
			{name: "string_from_bytes", dst: new(string), raw: []byte("hello"), want: "hello"},
			{name: "string_from_string", dst: new(string), raw: "hello", want: "hello"},
			{name: "string_from_float", dst: new(string), raw: 7.5, want: "7.5"},
			{name: "assignable_int64", dst: new(int64), raw: int64(9), want: int64(9)},
			{name: "convertible_myint", dst: new(myInt), raw: int64(11), want: myInt(11)},
			{name: "convertible_mystring", dst: new(myString), raw: "hello", want: myString("hello")},
			{name: "bool_from_bool", dst: new(bool), raw: true, want: true},
			{name: "bool_from_int", dst: new(bool), raw: int64(1), want: true},
			{name: "bool_from_bytes", dst: new(bool), raw: []byte("false"), want: false},
			{name: "int_from_float", dst: new(int), raw: float64(42), want: 42},
			{name: "int_from_string", dst: new(int), raw: "42", want: 42},
			{name: "uint_from_int", dst: new(uint), raw: int64(7), want: uint(7)},
			{name: "uint_from_string", dst: new(uint), raw: "7", want: uint(7)},
			{name: "float_from_int", dst: new(float64), raw: int64(7), want: 7.0},
			{name: "float_from_string", dst: new(float64), raw: "1.5", want: 1.5},
			{name: "bytes_from_string", dst: new([]byte), raw: "abc", want: []byte("abc")},
			{name: "scanner", dst: new(sql.NullString), raw: "hello", want: sql.NullString{String: "hello", Valid: true}},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				dst := reflect.ValueOf(tc.dst).Elem()
				if err := assignRawToValue(dst, tc.raw); err != nil {
					t.Fatalf("assign: %v", err)
				}
				got := dst.Interface()
				if b, ok := got.([]byte); ok {
					if !reflect.DeepEqual(b, tc.want) {
						t.Fatalf("unexpected bytes: %#v", b)
					}
					return
				}
				if !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("unexpected value\nwant: %#v\ngot:  %#v", tc.want, got)
				}
			})
		}
	})

	t.Run("nil_and_unsupported", func(t *testing.T) {
		dst := reflect.ValueOf(new(int)).Elem()
		if err := assignRawToValue(dst, nil); err != nil {
			t.Fatalf("assign nil: %v", err)
		}
		if got := dst.Int(); got != 0 {
			t.Fatalf("expected zero value, got %d", got)
		}
		if err := assignRawToValue(reflect.ValueOf(new(bool)).Elem(), struct{}{}); err == nil || !strings.Contains(err.Error(), "unsupported field type") {
			t.Fatalf("expected bool unsupported type error, got %v", err)
		}
		if err := assignRawToValue(reflect.ValueOf(new(int)).Elem(), true); err == nil || !strings.Contains(err.Error(), "unsupported field type") {
			t.Fatalf("expected int unsupported type error, got %v", err)
		}
		if err := assignRawToValue(reflect.ValueOf(new(uint)).Elem(), true); err == nil || !strings.Contains(err.Error(), "unsupported field type") {
			t.Fatalf("expected uint unsupported type error, got %v", err)
		}
		if err := assignRawToValue(reflect.ValueOf(new(float64)).Elem(), true); err == nil || !strings.Contains(err.Error(), "unsupported field type") {
			t.Fatalf("expected float unsupported type error, got %v", err)
		}
		if err := assignRawToValue(reflect.ValueOf(new([]byte)).Elem(), 10); err == nil || !strings.Contains(err.Error(), "unsupported field type") {
			t.Fatalf("expected slice unsupported type error, got %v", err)
		}
		if err := assignRawToValue(reflect.ValueOf(new(struct{})).Elem(), 1); err == nil || !strings.Contains(err.Error(), "unsupported field type") {
			t.Fatalf("expected unsupported field type error, got %v", err)
		}
	})
}

func TestAssignRawToField(t *testing.T) {
	var ptr *string
	field := reflect.ValueOf(&ptr).Elem()
	if err := assignRawToField(field, nil); err != nil {
		t.Fatalf("assign nil pointer: %v", err)
	}
	if ptr != nil {
		t.Fatalf("expected nil pointer, got %#v", ptr)
	}
	if err := assignRawToField(field, "hello"); err != nil {
		t.Fatalf("assign pointer: %v", err)
	}
	if ptr == nil || *ptr != "hello" {
		t.Fatalf("unexpected pointer value: %#v", ptr)
	}
}

func TestScalarScanning(t *testing.T) {
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_")))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE numbers (n INTEGER NOT NULL, label TEXT NOT NULL)`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO numbers (n, label) VALUES (7, 'seven'), (8, 'eight')`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}

	ctx := context.Background()
	rows, err := db.QueryContext(ctx, `SELECT n FROM numbers ORDER BY n ASC`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	values, err := collectAll[int](rows)
	_ = rows.Close()
	if err != nil {
		t.Fatalf("collect all: %v", err)
	}
	if !reflect.DeepEqual(values, []int{7, 8}) {
		t.Fatalf("unexpected scalar values: %#v", values)
	}

	rows, err = db.QueryContext(ctx, `SELECT label FROM numbers WHERE n = 7`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !rows.Next() {
		t.Fatal("expected a row")
	}
	value, err := scanIntoValue[string](rows)
	_ = rows.Close()
	if err != nil {
		t.Fatalf("scan into value: %v", err)
	}
	if value != "seven" {
		t.Fatalf("unexpected scalar value: %q", value)
	}

	rows, err = db.QueryContext(ctx, `SELECT n, label FROM numbers WHERE n = 7`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !rows.Next() {
		t.Fatal("expected a row")
	}
	if _, err := scanIntoValue[string](rows); err == nil {
		t.Fatal("expected scan error")
	}
	_ = rows.Close()
}

func TestRowCountAPIs(t *testing.T) {
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_")))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE numbers (n INTEGER NOT NULL)`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO numbers (n) VALUES (1), (2)`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}

	ctx := context.Background()
	qq := quarry.New(quarry.SQLite)

	values, err := All[int](ctx, db, qq.Select("n").From("numbers").OrderBy("n ASC"))
	if err != nil {
		t.Fatalf("all: %v", err)
	}
	if !reflect.DeepEqual(values, []int{1, 2}) {
		t.Fatalf("unexpected all rows: %#v", values)
	}

	if _, err := One[int](ctx, db, qq.Select("n").From("numbers").OrderBy("n ASC")); err == nil || !strings.Contains(err.Error(), "expected exactly one row") {
		t.Fatalf("expected one-row error, got %v", err)
	}
	if _, err := MaybeOne[int](ctx, db, qq.Select("n").From("numbers").OrderBy("n ASC")); err == nil || !strings.Contains(err.Error(), "expected at most one row") {
		t.Fatalf("expected maybe-one error, got %v", err)
	}
}
