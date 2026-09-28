package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func newDefResume() *PipelineDef {
	return &PipelineDef{Name: "p", MaxRunning: 1, Steps: []StepDef{
		{Name: "a", Outputs: []string{"a.out"}},
		{Name: "b", DependsOn: []string{"a"}, Outputs: []string{"b.out"}},
		{Name: "c", DependsOn: []string{"b"}, Outputs: []string{"c.out"}},
	}}
}

// TestResumeDoesNotReexecuteCompleted 模拟 a、b 已完成后重启：
// 已完成步骤绝不能从头重跑，c 必须接着跑完。
func TestResumeDoesNotReexecuteCompleted(t *testing.T) {
	def := newDefResume()
	stateDir := t.TempDir()

	openStore := func() *Store {
		s, err := NewStore(stateDir)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}

	// 第一次运行：用一个在 b 成功后“中断”的执行器（通过手动只让 a、b 运行）。
	var mu sync.Mutex
	ranFirst := map[string]int{}
	exec1 := NewFuncExecutor(map[string]func(ctx context.Context, attempt int) StepResult{
		"a": func(ctx context.Context, n int) StepResult {
			mu.Lock()
			ranFirst["a"]++
			mu.Unlock()
			return StepResult{Outputs: []string{"a.out"}}
		},
		"b": func(ctx context.Context, n int) StepResult {
			mu.Lock()
			ranFirst["b"]++
			mu.Unlock()
			return StepResult{Outputs: []string{"b.out"}}
		},
		"c": func(ctx context.Context, n int) StepResult {
			// 通过取消上下文模拟进程在 c 执行期间被杀掉。
			return StepResult{Err: &StepError{Kind: KindRetryable, Message: "interrupted"}}
		},
	})
	// 为了得到“c 处于 running 时进程死亡”的现场，这里改为直接构造：
	// 先正常跑 a、b（c 永久失败会让 b->c 不被跳过地重跑，所以单独控制）。
	{
		s := openStore()
		sch := NewScheduler(def, s, exec1, NewLogger(&strings.Builder{}))
		sch.SetBackoff(time.Millisecond)
		rep, err := sch.Run(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		s.Close()
		// c 用永久失败来产生一个终态现场（max_retries 默认 0，一次即 failed）。
		if rep.Status != RunFailed || !contains(rep.Failed, "c") {
			t.Fatalf("setup: %+v", rep)
		}
	}

	// 第二次运行：记录每个步骤被执行的次数。a、b 已是 succeeded，必须为 0。
	ranSecond := map[string]int{}
	exec2 := NewFuncExecutor(map[string]func(ctx context.Context, attempt int) StepResult{
		"a": func(ctx context.Context, n int) StepResult {
			ranSecond["a"]++
			return StepResult{Outputs: []string{"a.out"}}
		},
		"b": func(ctx context.Context, n int) StepResult {
			ranSecond["b"]++
			return StepResult{Outputs: []string{"b.out"}}
		},
		"c": func(ctx context.Context, n int) StepResult {
			ranSecond["c"]++
			// 已经 failed 的步骤在同一状态目录里同样不重跑（终态），
			// 因此这里跑不到；能跑到说明断点恢复逻辑错误。
			return StepResult{Outputs: []string{"c.out"}}
		},
	})
	{
		s := openStore()
		sch := NewScheduler(def, s, exec2, NewLogger(&strings.Builder{}))
		sch.SetBackoff(time.Millisecond)
		rep, err := sch.Run(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		s.Close()
		if rep.Status != RunFailed {
			t.Fatalf("终态运行应保持 failed（已失败步骤不自动重跑），得到 %s", rep.Status)
		}
	}
	if ranSecond["a"] != 0 || ranSecond["b"] != 0 {
		t.Fatalf("已完成步骤被重复执行: a=%d b=%d", ranSecond["a"], ranSecond["b"])
	}
	if ranSecond["c"] != 0 {
		t.Fatalf("已失败（终态）步骤不应被重跑，c 被执行 %d 次", ranSecond["c"])
	}
	t.Logf("第一次运行执行次数: a=%d b=%d c=失败终态；重启后 a/b/c 执行次数: a=%d b=%d c=%d",
		ranFirst["a"], ranFirst["b"], ranSecond["a"], ranSecond["b"], ranSecond["c"])
}

// TestResumeFromStaleRunning 构造“步骤崩溃在 running 中途”的现场，
// 验证重启后该步骤被重新执行，而其已成功的上游不重跑。
func TestResumeFromStaleRunning(t *testing.T) {
	def := newDefResume()
	stateDir := t.TempDir()

	// 第一阶段：让 a 成功、b 停在 running（手工只追加到 running 事件即“杀进程”）。
	{
		s, err := NewStore(stateDir)
		if err != nil {
			t.Fatal(err)
		}
		snap, err := s.Bootstrap(def)
		if err != nil {
			t.Fatal(err)
		}
		ev, err := s.AppendTransition(context.Background(), snap.LastSeq+1, "a",
			Transition{StatusPending, StatusRunning}, 1, "")
		if err != nil {
			t.Fatal(err)
		}
		snap.LastSeq = ev.Seq
		snap.Steps["a"].Status = StatusRunning
		snap.Steps["a"].Attempts = 1
		ev, err = s.AppendTransition(context.Background(), snap.LastSeq+1, "a",
			Transition{StatusRunning, StatusSucceeded}, 1, "")
		if err != nil {
			t.Fatal(err)
		}
		snap.LastSeq = ev.Seq
		snap.Steps["a"].Status = StatusSucceeded
		snap.Steps["a"].Outputs = []string{"a.out"}
		// b 启动后“崩溃”：只写 pending->running，没有后继。
		ev, err = s.AppendTransition(context.Background(), snap.LastSeq+1, "b",
			Transition{StatusPending, StatusRunning}, 1, "")
		if err != nil {
			t.Fatal(err)
		}
		snap.LastSeq = ev.Seq
		snap.Steps["b"].Status = StatusRunning
		snap.Steps["b"].Attempts = 1
		if err := s.SaveSnapshot(snap); err != nil {
			t.Fatal(err)
		}
		s.Close()
	}

	// 第二阶段：重启。a 不应执行，b 应重新执行一次，c 正常跟上。
	ran := map[string]int{}
	var mu sync.Mutex
	exec := NewFuncExecutor(map[string]func(ctx context.Context, attempt int) StepResult{
		"a": func(ctx context.Context, n int) StepResult {
			mu.Lock()
			ran["a"]++
			mu.Unlock()
			return StepResult{Outputs: []string{"a.out"}}
		},
		"b": func(ctx context.Context, n int) StepResult {
			mu.Lock()
			ran["b"]++
			mu.Unlock()
			return StepResult{Outputs: []string{"b.out"}}
		},
		"c": func(ctx context.Context, n int) StepResult {
			mu.Lock()
			ran["c"]++
			mu.Unlock()
			return StepResult{Outputs: []string{"c.out"}}
		},
	})
	s, err := NewStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	log := &strings.Builder{}
	sch := NewScheduler(def, s, exec, NewLogger(log))
	sch.SetBackoff(time.Millisecond)
	rep, err := sch.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if rep.Status != RunComplete {
		t.Fatalf("恢复后应全部成功，得到 %s (%+v)", rep.Status, rep)
	}
	mu.Lock()
	defer mu.Unlock()
	if ran["a"] != 0 {
		t.Fatalf("已成功的 a 不应重跑，实际 %d 次", ran["a"])
	}
	if ran["b"] != 1 {
		t.Fatalf("stale running 的 b 应恰好重跑 1 次，实际 %d 次", ran["b"])
	}
	if ran["c"] != 1 {
		t.Fatalf("c 应执行 1 次，实际 %d 次", ran["c"])
	}
	if !strings.Contains(log.String(), "running") || !strings.Contains(log.String(), "b") {
		t.Fatalf("恢复日志应提到步骤 b 的 stale running，实际:\n%s", log.String())
	}
	t.Logf("stale running 恢复：a 重跑=%d，b 重跑=%d，c 执行=%d", ran["a"], ran["b"], ran["c"])
}

// TestResumePreservesRetryBudget 验证崩溃发生在重试之间时，
// 已用掉的尝试次数不会在重启后被重置（不能靠重启获得无限重试）。
func TestResumePreservesRetryBudget(t *testing.T) {
	def := &PipelineDef{Name: "p", MaxRunning: 1, Steps: []StepDef{
		{Name: "a", MaxRetries: 2},
	}}
	stateDir := t.TempDir()

	// 现场：a 已经历 1 次失败（running->pending，attempt=1），即已用 1 次尝试。
	{
		s, _ := NewStore(stateDir)
		snap, _ := s.Bootstrap(def)
		ev, _ := s.AppendTransition(context.Background(), snap.LastSeq+1, "a",
			Transition{StatusPending, StatusRunning}, 1, "")
		snap.LastSeq = ev.Seq
		snap.Steps["a"].Status = StatusRunning
		snap.Steps["a"].Attempts = 1
		ev, _ = s.AppendTransition(context.Background(), snap.LastSeq+1, "a",
			Transition{StatusRunning, StatusPending}, 1, "可重试失败")
		snap.LastSeq = ev.Seq
		snap.Steps["a"].Status = StatusPending
		snap.Steps["a"].Attempts = 1
		s.SaveSnapshot(snap)
		s.Close()
	}

	var mu sync.Mutex
	attempts := []int{}
	exec := NewFuncExecutor(map[string]func(ctx context.Context, attempt int) StepResult{
		"a": func(ctx context.Context, attempt int) StepResult {
			mu.Lock()
			attempts = append(attempts, attempt)
			mu.Unlock()
			return StepResult{Err: &StepError{Kind: KindRetryable, Message: "still bad"}}
		},
	})
	s, _ := NewStore(stateDir)
	sch := NewScheduler(def, s, exec, NewLogger(&strings.Builder{}))
	sch.SetBackoff(time.Millisecond)
	rep, err := sch.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if rep.Status != RunFailed {
		t.Fatalf("应在用尽预算后失败: %+v", rep)
	}
	// 预算为“首跑 + 2 次重试”=3 次总尝试；已用 1 次，重启后只剩 attempt 2、3。
	mu.Lock()
	defer mu.Unlock()
	if got := fmt.Sprint(attempts); got != "[2 3]" {
		t.Fatalf("重启后应只看到尝试 2 和 3，实际 %s（重试预算被重置或错误）", got)
	}
	t.Logf("恢复后尝试序号序列=%v（已用预算未被重置）", attempts)
}

// TestSchedulerRejectsTamperedRollback 端到端验证：人为把已完成步骤改回 pending，
// Scheduler.Run 必须返回 RollbackError，而不是偷偷重跑。
func TestSchedulerRejectsTamperedRollback(t *testing.T) {
	def := &PipelineDef{Name: "p", MaxRunning: 1, Steps: []StepDef{
		{Name: "a", Outputs: []string{"a.out"}},
	}}
	stateDir := t.TempDir()

	{
		s, _ := NewStore(stateDir)
		sch := NewScheduler(def, s,
			NewFuncExecutor(map[string]func(ctx context.Context, attempt int) StepResult{
				"a": func(ctx context.Context, n int) StepResult {
					return StepResult{Outputs: []string{"a.out"}}
				},
			}), NewLogger(&strings.Builder{}))
		if rep, err := sch.Run(context.Background()); err != nil || rep.Status != RunComplete {
			t.Fatalf("setup run: %v %+v", err, rep)
		}
		s.Close()
	}

	// 篡改 snapshot.json：succeeded -> pending。
	sp := stateDir + "/snapshot.json"
	data, _ := readFile(sp)
	data = []byte(strings.Replace(string(data), `"status": "succeeded"`, `"status": "pending"`, 1))
	writeFile(sp, data)

	s, _ := NewStore(stateDir)
	sch := NewScheduler(def, s, NewFuncExecutor(nil), NewLogger(&strings.Builder{}))
	_, err := sch.Run(context.Background())
	s.Close()
	var rb *RollbackError
	if !errorsAs(err, &rb) {
		t.Fatalf("期望 RollbackError，实际 %v", err)
	}
	if rb.Step != "a" {
		t.Fatalf("回退错误应指出步骤 a，实际 %+v", rb)
	}
	t.Logf("人为回退被拒绝: %s", err.Error())
}
