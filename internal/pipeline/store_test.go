package pipeline

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	st, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return st
}

func TestStoreRecordsAndReloads(t *testing.T) {
	dir := t.TempDir()
	st, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Record("align", StatusRunning, 1, nil, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.Record("align", StatusSucceeded, 1, []string{"a.bam"}, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: succeeded state must survive.
	st2, err := NewStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	states := st2.Load()
	got := states["align"]
	if got.Status != StatusSucceeded || got.Attempts != 1 {
		t.Fatalf("reloaded state = %+v, want succeeded/1", got)
	}
	if len(got.Outputs) != 1 || got.Outputs[0] != "a.bam" {
		t.Fatalf("outputs not restored: %+v", got.Outputs)
	}
}

func TestStoreRejectsRollbackOfSucceededStep(t *testing.T) {
	st := newTestStore(t)
	if err := st.Record("done", StatusRunning, 1, nil, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.Record("done", StatusSucceeded, 1, []string{"x.vcf"}, ""); err != nil {
		t.Fatal(err)
	}

	// Simulate "manually change it back to pending": the journal itself
	// refuses such a transition.
	err := st.Record("done", StatusPending, 0, nil, "")
	var rb *RollbackError
	if !errors.As(err, &rb) {
		t.Fatalf("want *RollbackError, got %v", err)
	}
	if rb.Step != "done" || rb.From != StatusSucceeded || rb.To != StatusPending {
		t.Fatalf("rollback detail wrong: %+v", rb)
	}
	if !strings.Contains(rb.Error(), "already succeeded") {
		t.Fatalf("rollback error should explain why: %v", err)
	}
	t.Logf("rollback explicitly refused: %v", err)

	// Re-running -> running is rejected too.
	if err := st.Record("done", StatusRunning, 2, nil, ""); err == nil {
		t.Fatal("succeeded -> running must be rejected")
	}
	// Decreasing attempt counters is rejected.
	if err := st.Record("done", StatusSucceeded, 0, nil, ""); err == nil {
		t.Fatal("decreasing attempts must be rejected")
	}
}

func TestStoreRejectsTerminalFailureRollback(t *testing.T) {
	st := newTestStore(t)
	_ = st.Record("boom", StatusRunning, 3, nil, "")
	_ = st.Record("boom", StatusFailed, 3, nil, "retryable: exit 1")

	err := st.Record("boom", StatusPending, 3, nil, "")
	var rb *RollbackError
	if !errors.As(err, &rb) || rb.From != StatusFailed {
		t.Fatalf("want RollbackError from failed, got %v", err)
	}
	t.Logf("failed rollback refused: %v", err)
}

func TestStoreRunningAfterCrashBecomesPending(t *testing.T) {
	dir := t.TempDir()
	st, _ := NewStore(dir)
	_ = st.Record("killed", StatusRunning, 1, nil, "")
	_ = st.Close()

	st2, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	got := st2.Load()["killed"]
	if got.Status != StatusPending {
		t.Fatalf("orphaned running step should normalize to pending, got %s", got.Status)
	}
	if got.Attempts != 1 {
		t.Fatalf("attempt count should be retained across crash, got %d", got.Attempts)
	}
}

func TestStoreDetectsJournalTampering(t *testing.T) {
	dir := t.TempDir()
	st, _ := NewStore(dir)
	_ = st.Record("a", StatusRunning, 1, nil, "")
	_ = st.Record("a", StatusSucceeded, 1, nil, "")
	_ = st.Close()

	path := filepath.Join(dir, journalFile)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Flip "succeeded" to "pending" inside the journal text.
	tampered := strings.Replace(string(data), `"status":"succeeded"`, `"status":"pending"`, 1)
	if tampered == string(data) {
		t.Fatal("test setup failed to alter journal")
	}
	if err := os.WriteFile(path, []byte(tampered), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStore(dir); err == nil {
		t.Fatal("tampered journal must be rejected")
	} else {
		t.Logf("journal tampering detected: %v", err)
	}

	// Truncating the journal (dropping the last entry) must also fail.
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) < 2 {
		t.Fatal("need >= 2 entries")
	}
	shorter := strings.Join(lines[:1], "\n") + "\n"
	if err := os.WriteFile(path, []byte(shorter), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStore(dir); err == nil {
		t.Fatal("truncated journal must be rejected")
	}
}

func TestStoreDetectsSnapshotTampering(t *testing.T) {
	dir := t.TempDir()
	st, _ := NewStore(dir)
	_ = st.Record("a", StatusRunning, 1, nil, "")
	_ = st.Record("a", StatusSucceeded, 1, []string{"o"}, "")
	if err := st.Snapshot(); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	// Edit state.json to pretend the step is pending again.
	snapPath := filepath.Join(dir, snapshotFile)
	data, _ := os.ReadFile(snapPath)
	fake := strings.Replace(string(data), `"status": "succeeded"`, `"status": "pending"`, 1)
	if err := os.WriteFile(snapPath, []byte(fake), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := NewStore(dir)
	var rb *RollbackError
	if !errors.As(err, &rb) {
		t.Fatalf("snapshot rollback must surface RollbackError, got %v", err)
	}
	t.Logf("snapshot tampering detected: %v", err)
}
