package quarry_test

import (
	"os"
	"strings"
	"testing"
)

func TestDocsBoundaryCopy(t *testing.T) {
	checks := []struct {
		file string
		want string
	}{
		{file: "README.md", want: "Treat `SetMap` keys as trusted identifiers"},
		{file: "README.md", want: "Use `Raw(...)` only when you need to drop to SQL directly and the SQL fragment itself is trusted."},
		{file: "README.md", want: "Do not treat `Raw(...)` as a sanitizer."},
		{file: "docs/scan.md", want: "`scan` scans rows; it does not infer schemas, generate SQL, or make raw SQL safe."},
		{file: "docs/reference/core/index.html", want: "SetMap validates trusted keys before rendering them."},
		{file: "docs/reference/packages/index.html", want: "The scan layer keeps SQL visible; it does not"},
		{file: "docs/examples/patch-updates/index.html", want: "stable, trusted keys"},
		{file: "docs/examples/admin-edit-form/index.html", want: "stable, trusted"},
		{file: "docs/examples/raw-recipes/index.html", want: "Raw fragments are trusted SQL text; they are not a sanitizer for user input."},
	}

	for _, tc := range checks {
		t.Run(tc.file, func(t *testing.T) {
			data, err := os.ReadFile(tc.file)
			if err != nil {
				t.Fatalf("read %s: %v", tc.file, err)
			}
			if !strings.Contains(string(data), tc.want) {
				t.Fatalf("expected %s to contain %q", tc.file, tc.want)
			}
		})
	}
}

func TestDocsLinkSanity(t *testing.T) {
	links := []struct {
		file   string
		href   string
		target string
	}{
		{file: "README.md", href: "docs/scan.md", target: "docs/scan.md"},
		{file: "README.md", href: "docs/hydra.md", target: "docs/hydra.md"},
		{file: "README.md", href: "docs/dialects.md", target: "docs/dialects.md"},
		{file: "README.md", href: "docs/compatibility.md", target: "docs/compatibility.md"},
		{file: "docs/reference/packages/index.html", href: "../../hydra.md", target: "docs/hydra.md"},
		{file: "docs/reference/core/index.html", href: "../identifiers/", target: "docs/reference/identifiers/index.html"},
		{file: "docs/reference/scan/index.html", href: "../../scan.md", target: "docs/scan.md"},
	}

	for _, tc := range links {
		t.Run(tc.file+"::"+tc.href, func(t *testing.T) {
			data, err := os.ReadFile(tc.file)
			if err != nil {
				t.Fatalf("read %s: %v", tc.file, err)
			}
			if !strings.Contains(string(data), tc.href) {
				t.Fatalf("expected %s to contain href %q", tc.file, tc.href)
			}
			if _, err := os.Stat(tc.target); err != nil {
				t.Fatalf("expected link target %s to exist: %v", tc.target, err)
			}
		})
	}
}
