package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestCommandRunnerSuccessAndFailureClasses(t *testing.T) {
	dir := t.TempDir()
	r := NewCommandRunner(dir)

	if res := r.Run(StepSpec{Name: "ok", Command: "true"}); res.Err != nil {
		t.Fatalf("true should succeed: %v", res.Err)
	}

	res := r.Run(StepSpec{Name: "retry", Command: "exit 1"})
	if res.Err == nil || !res.Retryable || res.Reason != ReasonRetryable {
		t.Fatalf("exit 1 = %+v, want retryable", res)
	}

	res = r.Run(StepSpec{Name: "fatal", Command: "exit 2"})
	if res.Err == nil || res.Retryable || res.Reason != ReasonNonRetryable {
		t.Fatalf("exit 2 = %+v, want non-retryable", res)
	}
}

func TestCommandRunnerMissingOutputIsNonRetryable(t *testing.T) {
	dir := t.TempDir()
	r := NewCommandRunner(dir)
	spec := StepSpec{Name: "claims", Command: "true", Outputs: []string{"missing.txt"}}
	res := r.Run(spec)
	if res.Err == nil || res.Retryable {
		t.Fatalf("missing declared output = %+v, want non-retryable error", res)
	}

	// Existing non-empty output is accepted.
	path := filepath.Join(dir, "have.txt")
	if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	spec.Outputs = []string{"have.txt"}
	if res := r.Run(spec); res.Err != nil {
		t.Fatalf("present output should pass: %v", res.Err)
	}
}

func TestCommandRunnerCancellation(t *testing.T) {
	dir := t.TempDir()
	r := NewCommandRunner(dir)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res := r.RunContext(ctx, StepSpec{Name: "c", Command: "sleep 5"})
	if res.Err == nil || res.Retryable {
		t.Fatalf("cancelled run = %+v, want non-retryable cancellation", res)
	}
}
