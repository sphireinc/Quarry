package rawsql

import (
	"fmt"
	"strings"
)

// RewriteQuestionPlaceholders rewrites raw `?` bind tokens while leaving quoted
// strings, identifiers, comments, and dollar-quoted bodies untouched.
func RewriteQuestionPlaceholders(sql string, args []any, placeholder func(int) string) (string, []any, error) {
	var b strings.Builder
	out := make([]any, 0, len(args))
	state := scanNormal
	dollar := ""
	argIndex := 0

	for i := 0; i < len(sql); {
		switch state {
		case scanNormal:
			switch sql[i] {
			case '\'':
				b.WriteByte(sql[i])
				i++
				state = scanSingleQuote
			case '"':
				b.WriteByte(sql[i])
				i++
				state = scanDoubleQuote
			case '-':
				if i+1 < len(sql) && sql[i+1] == '-' {
					b.WriteByte(sql[i])
					b.WriteByte(sql[i+1])
					i += 2
					state = scanLineComment
					continue
				}
				b.WriteByte(sql[i])
				i++
			case '/':
				if i+1 < len(sql) && sql[i+1] == '*' {
					b.WriteByte(sql[i])
					b.WriteByte(sql[i+1])
					i += 2
					state = scanBlockComment
					continue
				}
				b.WriteByte(sql[i])
				i++
			case '$':
				if delim, width, ok := parseDollarQuoteStart(sql[i:]); ok {
					b.WriteString(delim)
					i += width
					state = scanDollarQuote
					dollar = delim
					continue
				}
				b.WriteByte(sql[i])
				i++
			case '?':
				// Preserve common Postgres JSON operators `?|` and `?&`.
				if i+1 < len(sql) && (sql[i+1] == '|' || sql[i+1] == '&') {
					b.WriteByte(sql[i])
					i++
					continue
				}
				if argIndex >= len(args) {
					return "", nil, fmt.Errorf("quarry: raw placeholder count does not match args count")
				}
				argIndex++
				b.WriteString(placeholder(argIndex))
				out = append(out, args[argIndex-1])
				i++
			default:
				b.WriteByte(sql[i])
				i++
			}
		case scanSingleQuote:
			b.WriteByte(sql[i])
			if sql[i] == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					b.WriteByte(sql[i+1])
					i += 2
					continue
				}
				i++
				state = scanNormal
				continue
			}
			i++
		case scanDoubleQuote:
			b.WriteByte(sql[i])
			if sql[i] == '"' {
				if i+1 < len(sql) && sql[i+1] == '"' {
					b.WriteByte(sql[i+1])
					i += 2
					continue
				}
				i++
				state = scanNormal
				continue
			}
			i++
		case scanLineComment:
			b.WriteByte(sql[i])
			if sql[i] == '\n' || sql[i] == '\r' {
				i++
				state = scanNormal
				continue
			}
			i++
		case scanBlockComment:
			b.WriteByte(sql[i])
			if sql[i] == '*' && i+1 < len(sql) && sql[i+1] == '/' {
				b.WriteByte(sql[i+1])
				i += 2
				state = scanNormal
				continue
			}
			i++
		case scanDollarQuote:
			if strings.HasPrefix(sql[i:], dollar) {
				b.WriteString(dollar)
				i += len(dollar)
				state = scanNormal
				dollar = ""
				continue
			}
			b.WriteByte(sql[i])
			i++
		}
	}

	if argIndex != len(args) {
		return "", nil, fmt.Errorf("quarry: raw placeholder count does not match args count")
	}
	return b.String(), out, nil
}

// RewriteNamedPlaceholders rewrites raw `:name` bind tokens while leaving
// quoted strings, identifiers, comments, and dollar-quoted bodies untouched.
// When strict is true, extra values in the map are reported as an error.
func RewriteNamedPlaceholders(sql string, values map[string]any, placeholder func(int) string, strict bool) (string, []any, error) {
	var b strings.Builder
	var out []any
	state := scanNormal
	dollar := ""
	used := make(map[string]struct{})

	for i := 0; i < len(sql); {
		switch state {
		case scanNormal:
			switch sql[i] {
			case '\'':
				b.WriteByte(sql[i])
				i++
				state = scanSingleQuote
			case '"':
				b.WriteByte(sql[i])
				i++
				state = scanDoubleQuote
			case '-':
				if i+1 < len(sql) && sql[i+1] == '-' {
					b.WriteByte(sql[i])
					b.WriteByte(sql[i+1])
					i += 2
					state = scanLineComment
					continue
				}
				b.WriteByte(sql[i])
				i++
			case '/':
				if i+1 < len(sql) && sql[i+1] == '*' {
					b.WriteByte(sql[i])
					b.WriteByte(sql[i+1])
					i += 2
					state = scanBlockComment
					continue
				}
				b.WriteByte(sql[i])
				i++
			case '$':
				if delim, width, ok := parseDollarQuoteStart(sql[i:]); ok {
					b.WriteString(delim)
					i += width
					state = scanDollarQuote
					dollar = delim
					continue
				}
				b.WriteByte(sql[i])
				i++
			case ':':
				if i > 0 && sql[i-1] == ':' {
					b.WriteByte(sql[i])
					i++
					continue
				}
				if i+1 >= len(sql) {
					return "", nil, fmt.Errorf("quarry codex: invalid named parameter syntax")
				}
				if isIdentPart(sql[i+1]) && !isIdentStart(sql[i+1]) {
					return "", nil, fmt.Errorf("quarry codex: invalid named parameter syntax")
				}
				if !isIdentStart(sql[i+1]) {
					b.WriteByte(sql[i])
					i++
					continue
				}
				j := i + 2
				for j < len(sql) && isIdentPart(sql[j]) {
					j++
				}
				name := sql[i+1 : j]
				value, ok := values[name]
				if !ok {
					return "", nil, fmt.Errorf("quarry codex: named parameter %q missing", name)
				}
				used[name] = struct{}{}
				out = append(out, value)
				b.WriteString(placeholder(len(out)))
				i = j
			default:
				b.WriteByte(sql[i])
				i++
			}
		case scanSingleQuote:
			b.WriteByte(sql[i])
			if sql[i] == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					b.WriteByte(sql[i+1])
					i += 2
					continue
				}
				i++
				state = scanNormal
				continue
			}
			i++
		case scanDoubleQuote:
			b.WriteByte(sql[i])
			if sql[i] == '"' {
				if i+1 < len(sql) && sql[i+1] == '"' {
					b.WriteByte(sql[i+1])
					i += 2
					continue
				}
				i++
				state = scanNormal
				continue
			}
			i++
		case scanLineComment:
			b.WriteByte(sql[i])
			if sql[i] == '\n' || sql[i] == '\r' {
				i++
				state = scanNormal
				continue
			}
			i++
		case scanBlockComment:
			b.WriteByte(sql[i])
			if sql[i] == '*' && i+1 < len(sql) && sql[i+1] == '/' {
				b.WriteByte(sql[i+1])
				i += 2
				state = scanNormal
				continue
			}
			i++
		case scanDollarQuote:
			if strings.HasPrefix(sql[i:], dollar) {
				b.WriteString(dollar)
				i += len(dollar)
				state = scanNormal
				dollar = ""
				continue
			}
			b.WriteByte(sql[i])
			i++
		}
	}

	if strict {
		for name := range values {
			if _, ok := used[name]; !ok {
				return "", nil, fmt.Errorf("quarry codex: named parameter %q unused", name)
			}
		}
	}

	return b.String(), out, nil
}

// OffsetDollarPlaceholders rewrites Postgres-style placeholders by a fixed offset.
//
// The scanner intentionally ignores quoted strings, identifiers, comments, and
// dollar-quoted bodies so embedded literal text does not get rewritten.
func OffsetDollarPlaceholders(sql string, offset int) (string, error) {
	if offset == 0 {
		return sql, nil
	}
	var b strings.Builder
	state := scanNormal
	dollar := ""

	for i := 0; i < len(sql); {
		switch state {
		case scanNormal:
			switch sql[i] {
			case '\'':
				b.WriteByte(sql[i])
				i++
				state = scanSingleQuote
			case '"':
				b.WriteByte(sql[i])
				i++
				state = scanDoubleQuote
			case '-':
				if i+1 < len(sql) && sql[i+1] == '-' {
					b.WriteByte(sql[i])
					b.WriteByte(sql[i+1])
					i += 2
					state = scanLineComment
					continue
				}
				b.WriteByte(sql[i])
				i++
			case '/':
				if i+1 < len(sql) && sql[i+1] == '*' {
					b.WriteByte(sql[i])
					b.WriteByte(sql[i+1])
					i += 2
					state = scanBlockComment
					continue
				}
				b.WriteByte(sql[i])
				i++
			case '$':
				if delim, width, ok := parseDollarQuoteStart(sql[i:]); ok {
					b.WriteString(delim)
					i += width
					state = scanDollarQuote
					dollar = delim
					continue
				}
				j := i + 1
				for j < len(sql) && sql[j] >= '0' && sql[j] <= '9' {
					j++
				}
				if j > i+1 {
					var n int
					for k := i + 1; k < j; k++ {
						n = n*10 + int(sql[k]-'0')
					}
					b.WriteByte('$')
					b.WriteString(intToString(n + offset))
					i = j
					continue
				}
				b.WriteByte(sql[i])
				i++
			default:
				b.WriteByte(sql[i])
				i++
			}
		case scanSingleQuote:
			b.WriteByte(sql[i])
			if sql[i] == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					b.WriteByte(sql[i+1])
					i += 2
					continue
				}
				i++
				state = scanNormal
				continue
			}
			i++
		case scanDoubleQuote:
			b.WriteByte(sql[i])
			if sql[i] == '"' {
				if i+1 < len(sql) && sql[i+1] == '"' {
					b.WriteByte(sql[i+1])
					i += 2
					continue
				}
				i++
				state = scanNormal
				continue
			}
			i++
		case scanLineComment:
			b.WriteByte(sql[i])
			if sql[i] == '\n' || sql[i] == '\r' {
				i++
				state = scanNormal
				continue
			}
			i++
		case scanBlockComment:
			b.WriteByte(sql[i])
			if sql[i] == '*' && i+1 < len(sql) && sql[i+1] == '/' {
				b.WriteByte(sql[i+1])
				i += 2
				state = scanNormal
				continue
			}
			i++
		case scanDollarQuote:
			if strings.HasPrefix(sql[i:], dollar) {
				b.WriteString(dollar)
				i += len(dollar)
				state = scanNormal
				dollar = ""
				continue
			}
			b.WriteByte(sql[i])
			i++
		}
	}

	return b.String(), nil
}

type scanState int

const (
	scanNormal scanState = iota
	scanSingleQuote
	scanDoubleQuote
	scanLineComment
	scanBlockComment
	scanDollarQuote
)

func parseDollarQuoteStart(sql string) (string, int, bool) {
	if len(sql) < 2 || sql[0] != '$' {
		return "", 0, false
	}
	if sql[1] == '$' {
		return "$$", 2, true
	}
	if !isDollarTagStart(sql[1]) {
		return "", 0, false
	}
	i := 2
	for i < len(sql) && isDollarTagPart(sql[i]) {
		i++
	}
	if i < len(sql) && sql[i] == '$' {
		return sql[:i+1], i + 1, true
	}
	return "", 0, false
}

func isDollarTagStart(b byte) bool {
	return isIdentStart(b)
}

func isDollarTagPart(b byte) bool {
	return isIdentPart(b)
}

func isIdentStart(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

func isIdentPart(b byte) bool {
	return isIdentStart(b) || (b >= '0' && b <= '9')
}

func intToString(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + (n % 10))
		n /= 10
	}
	return string(buf[i:])
}
