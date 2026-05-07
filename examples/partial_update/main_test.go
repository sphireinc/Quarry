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
	if !strings.Contains(out, "UPDATE users SET") {
		t.Fatalf("unexpected output: %s", out)
	}
	if !strings.Contains(out, "WHERE id = $4 RETURNING id") {
		t.Fatalf("unexpected output: %s", out)
	}
	if !strings.Contains(out, "Quarry User") {
		t.Fatalf("unexpected args output: %s", out)
	}
	if !strings.Contains(out, "true") {
		t.Fatalf("unexpected args output: %s", out)
	}
}

func ExampleMain() {
	main()
	// Output:
	// UPDATE users SET name = $1, email = $2, enabled = $3 WHERE id = $4 RETURNING id
	// [Quarry User user@example.com true 7]
}
