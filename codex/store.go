package codex

import (
	"fmt"
	"sort"
	"strings"

	"github.com/sphireinc/quarry"
)

// QueryTemplate is a reusable query definition stored in a Store.
//
// The interface stays intentionally small so callers can inspect the name and
// bind the template without needing to know the concrete storage type.
type QueryTemplate interface {
	Name() string
	With(*quarry.Quarry) *BoundRaw
}

// Store keeps named query templates in a deterministic registry.
type Store struct {
	strict  bool
	queries map[string]QueryTemplate
}

// NewStore creates an empty query-template registry.
func NewStore() *Store {
	return &Store{queries: make(map[string]QueryTemplate)}
}

// SetStrict toggles whether unused named parameters should be rejected.
//
// The flag only applies to templates added after the call, which keeps the
// store behavior simple and predictable.
func (s *Store) SetStrict(enabled bool) *Store {
	if s != nil {
		s.strict = enabled
	}
	return s
}

// Add stores a named query template after validating its name and contents.
func (s *Store) Add(name, sql string) error {
	if s == nil {
		return fmt.Errorf("quarry codex: nil store")
	}
	if s.queries == nil {
		s.queries = make(map[string]QueryTemplate)
	}
	if err := validateTemplateName(name); err != nil {
		return err
	}
	if strings.TrimSpace(sql) == "" {
		return fmt.Errorf("quarry codex: query %q is empty", name)
	}
	if existing, ok := s.queries[name]; ok && existing != nil {
		return fmt.Errorf("quarry codex: query %q already exists", name)
	}
	s.queries[name] = RawQuery{name: name, sql: sql, named: true, strict: s.strict}
	return nil
}

// MustAdd stores a query or panics if validation fails.
func (s *Store) MustAdd(name, sql string) {
	if err := s.Add(name, sql); err != nil {
		panic(err)
	}
}

// Get fetches a stored template by name.
func (s *Store) Get(name string) (QueryTemplate, bool) {
	if s == nil {
		return nil, false
	}
	q, ok := s.queries[name]
	return q, ok
}

// Names returns the stored query names in sorted order.
func (s *Store) Names() []string {
	if s == nil || len(s.queries) == 0 {
		return nil
	}
	names := make([]string, 0, len(s.queries))
	for name := range s.queries {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// validateTemplateName keeps query names human-friendly without overfitting to SQL syntax.
func validateTemplateName(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("quarry codex: query name is required")
	}
	for _, r := range name {
		switch {
		case r == '.' || r == '_' || r == '-' || r == '/' || r == ':':
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		default:
			return fmt.Errorf("quarry codex: invalid query name %q", name)
		}
	}
	return nil
}
