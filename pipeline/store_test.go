package pipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func testDef() *PipelineDef {
	return &PipelineDef{
		Name:       "t",
		MaxRunning: 2,
		Steps: []StepDef{
			{Name: "a", Command: "echo a"},
			{Name: "b", Command: "echo b", DependsOn: []string{"a"}},
		},
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestBootstrapAndRecoverFresh(t *testing.T) {
	def := testDef()
	s := newTestStore(t)
	snap, err := s.Bootstrap(def)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != RunRunning || snap.Steps["a"].Status != StatusPending {
		t.Fatalf("bad initial snap: %+v", snap)
	}
	got, err := s.Recover(def)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if got.Steps["a"].Status != StatusPending {
		t.Fatalf("want pending, got %s", got.Steps["a"].Status)
	}
}

func TestRecoverAfterSucceededEvent(t *testing.T) {
	def := testDef()
	s := newTestStore(t)
	snap, _ := s.Bootstrap(def)

	ev1, err := s.AppendTransition(context.Background(), snap.LastSeq+1, "a",
		Transition{StatusPending, StatusRunning}, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	snap.LastSeq = ev1.Seq
	ev2, err := s.AppendTransition(context.Background(), snap.LastSeq+1, "a",
		Transition{StatusRunning, StatusSucceeded}, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	snap.LastSeq = ev2.Seq
	snap.Steps["a"].Status = StatusSucceeded
	snap.Steps["a"].Attempts = 1
	if err := s.SaveSnapshot(snap); err != nil {
		t.Fatal(err)
	}

	got, err := s.Recover(def)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if got.Steps["a"].Status != StatusSucceeded || got.Steps["a"].Attempts != 1 {
		t.Fatalf("unexpected state: %+v", got.Steps["a"])
	}
}

func TestRecoverRejectsManualRollback(t *testing.T) {
	def := testDef()
	s := newTestStore(t)
	snap, _ := s.Bootstrap(def)

	ev1, _ := s.AppendTransition(context.Background(), snap.LastSeq+1, "a",
		Transition{StatusPending, StatusRunning}, 1, "")
	snap.LastSeq = ev1.Seq
	ev2, _ := s.AppendTransition(context.Background(), snap.LastSeq+1, "a",
		Transition{StatusRunning, StatusSucceeded}, 1, "")
	snap.LastSeq = ev2.Seq
	snap.Steps["a"].Status = StatusSucceeded
	s.SaveSnapshot(snap)

	// 人为把快照里的 succeeded 改回 pending。
	p := filepath.Join(s.dir, snapshotFile)
	data, _ := os.ReadFile(p)
	fixed := bytesReplace(data, `"status": "succeeded"`, `"status": "pending"`)
	if string(fixed) == string(data) {
		t.Fatal("test setup: replacement did not happen")
	}
	os.WriteFile(p, fixed, 0o644)

	_, err := s.Recover(def)
	var rb *RollbackError
	if !errors.As(err, &rb) {
		t.Fatalf("expected RollbackError, got %v", err)
	}
	if rb.Step != "a" || rb.Expect != StatusSucceeded || rb.Found != StatusPending {
		t.Fatalf("unexpected rollback detail: %+v", rb)
	}
}

func TestRecoverRejectsTamperedEvent(t *testing.T) {
	def := testDef()
	s := newTestStore(t)
	snap, _ := s.Bootstrap(def)
	s.AppendTransition(context.Background(), snap.LastSeq+1, "a",
		Transition{StatusPending, StatusRunning}, 1, "")

	// 篡改事件日志内容（detail），哈希链必须断裂。
	p := filepath.Join(s.dir, eventsFile)
	data, _ := os.ReadFile(p)
	fixed := bytesReplace(data, `"detail": ""`, `"detail": "hacked"`)
	os.WriteFile(p, fixed, 0o644)

	_, err := s.Recover(def)
	var corrupt *StateCorruptionError
	if !errors.As(err, &corrupt) {
		t.Fatalf("expected StateCorruptionError, got %v", err)
	}
}

func TestRecoverRejectsDefMismatch(t *testing.T) {
	def := testDef()
	s := newTestStore(t)
	s.Bootstrap(def)

	other := testDef()
	other.Steps = append(other.Steps, StepDef{Name: "c", Command: "echo c"})
	_, err := s.Recover(other)
	var mismatch *DefMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("expected DefMismatchError, got %v", err)
	}
}
