package rawsql

import (
	"fmt"
	"strings"
	"testing"
)

func TestRewriteQuestionPlaceholders(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		args    []any
		wantSQL string
		want    []any
		wantErr string
	}{
		{
			name:    "basic",
			sql:     "a = ? AND b = ?",
			args:    []any{1, 2},
			wantSQL: "a = $1 AND b = $2",
			want:    []any{1, 2},
		},
		{
			name:    "quoted_and_comments_are_ignored",
			sql:     "note = 'what?' AND \"what?name\" = ? -- why?\n/* keep ? */ body = $$what?$$ AND op ?| array['role'] AND flag ?& array['x']",
			args:    []any{7},
			wantSQL: "note = 'what?' AND \"what?name\" = $1 -- why?\n/* keep ? */ body = $$what?$$ AND op ?| array['role'] AND flag ?& array['x']",
			want:    []any{7},
		},
		{
			name:    "too_few_args",
			sql:     "a = ? AND b = ?",
			args:    []any{1},
			wantErr: "raw placeholder count does not match args count",
		},
		{
			name:    "too_many_args",
			sql:     "a = ?",
			args:    []any{1, 2},
			wantErr: "raw placeholder count does not match args count",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotSQL, gotArgs, err := RewriteQuestionPlaceholders(tc.sql, tc.args, func(n int) string {
				return fmt.Sprintf("$%d", n)
			})
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotSQL != tc.wantSQL {
				t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", tc.wantSQL, gotSQL)
			}
			if fmt.Sprint(gotArgs) != fmt.Sprint(tc.want) {
				t.Fatalf("args mismatch\nwant: %#v\ngot:  %#v", tc.want, gotArgs)
			}
		})
	}
}

func TestRewriteNamedPlaceholders(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		sqlText, args, err := RewriteNamedPlaceholders(
			`SELECT ':status' AS literal, now()::date AS today, id
FROM users
WHERE tenant_id = :tenant_id
  AND status = :status
  AND name = :status`,
			map[string]any{
				"tenant_id": 42,
				"status":    "active",
				"unused":    "ignored",
			},
			func(n int) string { return fmt.Sprintf("$%d", n) },
			false,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := `SELECT ':status' AS literal, now()::date AS today, id
FROM users
WHERE tenant_id = $1
  AND status = $2
  AND name = $3`
		if sqlText != want {
			t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want, sqlText)
		}
		if fmt.Sprint(args) != fmt.Sprint([]any{42, "active", "active"}) {
			t.Fatalf("args mismatch: %#v", args)
		}
	})

	t.Run("strict_unused", func(t *testing.T) {
		_, _, err := RewriteNamedPlaceholders(
			`SELECT * FROM users WHERE id = :id`,
			map[string]any{"id": 10, "extra": true},
			func(n int) string { return fmt.Sprintf("$%d", n) },
			true,
		)
		if err == nil || !strings.Contains(err.Error(), `named parameter "extra" unused`) {
			t.Fatalf("expected strict unused error, got %v", err)
		}
	})

	t.Run("comments_and_dollar_quotes", func(t *testing.T) {
		sqlText, args, err := RewriteNamedPlaceholders(
			`-- :ignored
/* :ignored */
SELECT $$:ignored$$ AS body, $tag$:ignored$tag$ AS tagged, x = :x, y = :x`,
			map[string]any{
				"x": 10,
			},
			func(n int) string { return fmt.Sprintf("$%d", n) },
			false,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := `-- :ignored
/* :ignored */
SELECT $$:ignored$$ AS body, $tag$:ignored$tag$ AS tagged, x = $1, y = $2`
		if sqlText != want {
			t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want, sqlText)
		}
		if fmt.Sprint(args) != fmt.Sprint([]any{10, 10}) {
			t.Fatalf("args mismatch: %#v", args)
		}
	})

	t.Run("missing_parameter", func(t *testing.T) {
		_, _, err := RewriteNamedPlaceholders(
			`SELECT * FROM users WHERE id = :id AND status = :status`,
			map[string]any{"id": 10},
			func(n int) string { return fmt.Sprintf("$%d", n) },
			false,
		)
		if err == nil || !strings.Contains(err.Error(), `named parameter "status" missing`) {
			t.Fatalf("expected missing parameter error, got %v", err)
		}
	})

	t.Run("invalid_syntax", func(t *testing.T) {
		_, _, err := RewriteNamedPlaceholders(
			`SELECT * FROM users WHERE id = :1bad`,
			map[string]any{"1bad": 10},
			func(n int) string { return fmt.Sprintf("$%d", n) },
			false,
		)
		if err == nil || !strings.Contains(err.Error(), "invalid named parameter syntax") {
			t.Fatalf("expected invalid syntax error, got %v", err)
		}
	})
}

func TestOffsetDollarPlaceholders(t *testing.T) {
	sqlText, err := OffsetDollarPlaceholders(
		`SELECT $1, '$2', "--$3", /* $4 */ $$ $5 $$, body = $6, tag = $tag$ $7 $tag$`,
		3,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `SELECT $4, '$2', "--$3", /* $4 */ $$ $5 $$, body = $9, tag = $tag$ $7 $tag$`
	if sqlText != want {
		t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want, sqlText)
	}

	unchanged, err := OffsetDollarPlaceholders("select $1", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if unchanged != "select $1" {
		t.Fatalf("expected no change, got %s", unchanged)
	}
}

func TestRawsqlHelpers(t *testing.T) {
	if delim, width, ok := parseDollarQuoteStart("$$body$$"); !ok || delim != "$$" || width != 2 {
		t.Fatalf("unexpected dollar-quote parse: %q %d %v", delim, width, ok)
	}
	if delim, width, ok := parseDollarQuoteStart("$tag$body$tag$"); !ok || delim != "$tag$" || width != len("$tag$") {
		t.Fatalf("unexpected tagged dollar-quote parse: %q %d %v", delim, width, ok)
	}
	if _, _, ok := parseDollarQuoteStart("$1"); ok {
		t.Fatal("expected invalid dollar quote to fail")
	}
	if !isDollarTagStart('A') || !isDollarTagPart('9') {
		t.Fatal("expected tag helpers to accept letters and digits")
	}
	if isDollarTagStart('1') || isDollarTagPart('-') {
		t.Fatal("expected tag helpers to reject invalid bytes")
	}
	if !isIdentStart('_') || !isIdentPart('7') {
		t.Fatal("expected ident helpers to accept valid bytes")
	}
	if isIdentStart('1') || isIdentPart('-') {
		t.Fatal("expected ident helpers to reject invalid bytes")
	}
	if got := intToString(0); got != "0" {
		t.Fatalf("unexpected intToString(0): %s", got)
	}
	if got := intToString(42); got != "42" {
		t.Fatalf("unexpected intToString(42): %s", got)
	}
}
