package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w

	outC := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		outC <- buf.String()
	}()

	fn()

	_ = w.Close()
	os.Stdout = old
	return <-outC
}

func TestMain(t *testing.T) {
	out := captureStdout(t, main)
	if !strings.Contains(out, "SELECT id, email, created_at FROM users WHERE tenant_id = $1") {
		t.Fatalf("unexpected output: %s", out)
	}
	if !strings.Contains(out, "ORDER BY created_at DESC LIMIT 25 OFFSET 0") {
		t.Fatalf("unexpected output: %s", out)
	}
	if !strings.Contains(out, "%bob%") {
		t.Fatalf("unexpected args output: %s", out)
	}
}

func ExampleMain() {
	main()
	// Output:
	// SELECT id, email, created_at FROM users WHERE tenant_id = $1 AND (email ILIKE $2 OR name ILIKE $3) ORDER BY created_at DESC LIMIT 25 OFFSET 0
	// [42 %bob% %bob%]
}
