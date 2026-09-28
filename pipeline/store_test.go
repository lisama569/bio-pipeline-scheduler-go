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
		Transition{StatusPending, StatusRunning}, 1, "original-detail")

	// 篡改事件日志内容（detail），哈希链必须断裂。
	p := filepath.Join(s.dir, eventsFile)
	data, _ := os.ReadFile(p)
	fixed := bytesReplace(data, `"detail":"original-detail"`, `"detail":"hacked"`)
	if string(fixed) == string(data) {
		t.Fatal("test setup: detail replacement did not match")
	}
	os.WriteFile(p, fixed, 0o644)

	_, err := s.Recover(def)
	var corrupt *StateCorruptionError
	if !errors.As(err, &corrupt) {
		t.Fatalf("expected StateCorruptionError, got %v", err)
	}
}

// 快照领先于事件日志（模拟事件日志被截断/删除）必须判为损坏。
func TestRecoverRejectsSnapshotAheadOfJournal(t *testing.T) {
	def := testDef()
	s := newTestStore(t)
	snap, _ := s.Bootstrap(def)
	// 追加 2 条事件并让快照前进到 seq=2（a succeeded）。
	ev, _ := s.AppendTransition(context.Background(), snap.LastSeq+1, "a",
		Transition{StatusPending, StatusRunning}, 1, "")
	snap.LastSeq = ev.Seq
	snap.Steps["a"].Status = StatusRunning
	snap.Steps["a"].Attempts = 1
	ev, _ = s.AppendTransition(context.Background(), snap.LastSeq+1, "a",
		Transition{StatusRunning, StatusSucceeded}, 1, "")
	snap.LastSeq = ev.Seq
	snap.Steps["a"].Status = StatusSucceeded
	s.SaveSnapshot(snap)

	// 删除事件日志中最后一条 -> 日志停在 seq=1，但快照声称 seq=2。
	p := filepath.Join(s.dir, eventsFile)
	data, _ := os.ReadFile(p)
	lines := splitLines(data)
	if len(lines) != 2 {
		t.Fatalf("setup: want 2 event lines, got %d", len(lines))
	}
	os.WriteFile(p, append(lines[0], '\n'), 0o644)

	_, err := s.Recover(def)
	var corrupt *StateCorruptionError
	if !errors.As(err, &corrupt) {
		t.Fatalf("expected StateCorruptionError for snapshot ahead of journal, got %v", err)
	}
}

// 合法崩溃窗口：事件已 fsync 但快照还停在上一条；恢复应自动补上而非报错。
func TestRecoverToleratesSnapshotLagging(t *testing.T) {
	def := testDef()
	s := newTestStore(t)
	snap, _ := s.Bootstrap(def)
	// 事件进入 running，但崩溃前快照仍是初始 last_seq=0 / pending。
	s.AppendTransition(context.Background(), snap.LastSeq+1, "a",
		Transition{StatusPending, StatusRunning}, 1, "")
	// 不更新 snap、不保存快照 —— 模拟崩溃在事件 fsync 之后、快照 rename 之前。

	got, err := s.Recover(def)
	if err != nil {
		t.Fatalf("合法的快照落后应被容忍并续上，实际报错: %v", err)
	}
	if got.Steps["a"].Status != StatusRunning || got.LastSeq != 1 {
		t.Fatalf("恢复后应为 running/seq1，实际 status=%s seq=%d",
			got.Steps["a"].Status, got.LastSeq)
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
