package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestShellExecutorSuccess(t *testing.T) {
	dir := t.TempDir()
	exec := NewShellExecutor(filepath.Join(dir, "logs"))
	def := StepDef{
		Name:    "ok",
		Command: "echo hi > out.txt",
		WorkDir: dir,
		Outputs: []string{"out.txt"},
	}
	res := exec.Execute(context.Background(), def, 1)
	if res.Err != nil {
		t.Fatalf("want success, got %v", res.Err)
	}
	if len(res.Outputs) != 1 || res.LogPath == "" {
		t.Fatalf("unexpected result: %+v", res)
	}
	if _, err := os.Stat(res.LogPath); err != nil {
		t.Fatalf("log not written: %v", err)
	}
}

func TestShellExecutorRetryableFailure(t *testing.T) {
	dir := t.TempDir()
	exec := NewShellExecutor("")
	res := exec.Execute(context.Background(), StepDef{
		Name: "flaky", Command: "exit 1", WorkDir: dir,
	}, 1)
	if res.Err == nil || !res.Err.Retryable() {
		t.Fatalf("want retryable error, got %+v", res.Err)
	}
}

func TestShellExecutorPermanentFailure(t *testing.T) {
	dir := t.TempDir()
	exec := NewShellExecutor("")
	res := exec.Execute(context.Background(), StepDef{
		Name: "hardfail", Command: "exit 42", WorkDir: dir,
	}, 1)
	if res.Err == nil || res.Err.Kind != KindPermanent {
		t.Fatalf("want permanent error, got %+v", res.Err)
	}
}

func TestShellExecutorMissingOutputIsPermanent(t *testing.T) {
	dir := t.TempDir()
	exec := NewShellExecutor("")
	res := exec.Execute(context.Background(), StepDef{
		Name: "noout", Command: "true", WorkDir: dir, Outputs: []string{"ghost.bam"},
	}, 1)
	if res.Err == nil || res.Err.Kind != KindPermanent {
		t.Fatalf("want permanent missing-output error, got %+v", res.Err)
	}
}
