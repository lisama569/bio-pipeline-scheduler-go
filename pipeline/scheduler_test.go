package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"
)

func newTestScheduler(t *testing.T, def *PipelineDef, exec Executor) (*Scheduler, *Store) {
	t.Helper()
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	sch := NewScheduler(def, s, exec, testLogger(t))
	sch.SetBackoff(time.Millisecond)
	return sch, s
}

func TestRunDependencyOrder(t *testing.T) {
	def := &PipelineDef{Name: "p", MaxRunning: 4, Steps: []StepDef{
		{Name: "a"},
		{Name: "b", DependsOn: []string{"a"}},
		{Name: "c", DependsOn: []string{"a"}},
		{Name: "d", DependsOn: []string{"b", "c"}},
	}}
	var mu sync.Mutex
	var order []string
	exec := NewFuncExecutor(map[string]func(ctx context.Context, attempt int) StepResult{
		"a": func(ctx context.Context, a int) StepResult {
			mu.Lock()
			order = append(order, "a")
			mu.Unlock()
			return StepResult{}
		},
		"b": func(ctx context.Context, a int) StepResult {
			mu.Lock()
			order = append(order, "b")
			mu.Unlock()
			return StepResult{}
		},
		"c": func(ctx context.Context, a int) StepResult {
			mu.Lock()
			order = append(order, "c")
			mu.Unlock()
			return StepResult{}
		},
		"d": func(ctx context.Context, a int) StepResult {
			mu.Lock()
			order = append(order, "d")
			mu.Unlock()
			return StepResult{}
		},
	})
	sch, _ := newTestScheduler(t, def, exec)
	rep, err := sch.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Status != RunComplete || len(rep.Succeeded) != 4 {
		t.Fatalf("report: %+v", rep)
	}
	mu.Lock()
	defer mu.Unlock()
	idx := map[string]int{}
	for i, n := range order {
		idx[n] = i
	}
	if !(idx["a"] < idx["b"] && idx["a"] < idx["c"] && idx["b"] < idx["d"] && idx["c"] < idx["d"]) {
		t.Fatalf("dependency order violated: %v", order)
	}
}

func TestFailurePropagation(t *testing.T) {
	def := &PipelineDef{Name: "p", MaxRunning: 4, Steps: []StepDef{
		{Name: "a"},
		{Name: "b", DependsOn: []string{"a"}},
		{Name: "c", DependsOn: []string{"b"}},
		{Name: "d"}, // 与失败链无关，应照常成功
	}}
	exec := NewFuncExecutor(map[string]func(ctx context.Context, attempt int) StepResult{
		"a": func(ctx context.Context, attempt int) StepResult {
			return StepResult{Err: &StepError{Kind: KindPermanent, Message: "boom"}}
		},
	})
	sch, _ := newTestScheduler(t, def, exec)
	rep, err := sch.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Status != RunFailed {
		t.Fatalf("want failed run, got %s", rep.Status)
	}
	if !contains(rep.Failed, "a") {
		t.Fatalf("a should fail: %+v", rep)
	}
	if !contains(rep.Skipped, "b") || !contains(rep.Skipped, "c") {
		t.Fatalf("b,c should be skipped: %+v", rep)
	}
	if contains(rep.Skipped, "d") || !contains(rep.Succeeded, "d") {
		t.Fatalf("d should succeed independently: %+v", rep)
	}
}

func TestRetryableSuccessAfterFailures(t *testing.T) {
	def := &PipelineDef{Name: "p", MaxRunning: 1, Steps: []StepDef{
		{Name: "a", MaxRetries: 2},
	}}
	var mu sync.Mutex
	tries := 0
	exec := NewFuncExecutor(map[string]func(ctx context.Context, attempt int) StepResult{
		"a": func(ctx context.Context, attempt int) StepResult {
			mu.Lock()
			defer mu.Unlock()
			tries++
			if attempt < 3 {
				return StepResult{Err: &StepError{Kind: KindRetryable, Message: "transient"}}
			}
			return StepResult{}
		},
	})
	sch, _ := newTestScheduler(t, def, exec)
	rep, err := sch.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Status != RunComplete {
		t.Fatalf("want complete, got %s (%+v)", rep.Status, rep)
	}
	mu.Lock()
	got := tries
	mu.Unlock()
	if got != 3 {
		t.Fatalf("want 3 attempts, got %d", got)
	}
}

func TestPermanentFailureNotRetried(t *testing.T) {
	def := &PipelineDef{Name: "p", MaxRunning: 1, Steps: []StepDef{
		{Name: "a", MaxRetries: 5},
	}}
	tries := 0
	exec := NewFuncExecutor(map[string]func(ctx context.Context, attempt int) StepResult{
		"a": func(ctx context.Context, attempt int) StepResult {
			tries++
			return StepResult{Err: &StepError{Kind: KindPermanent, Message: "hard"}}
		},
	})
	sch, _ := newTestScheduler(t, def, exec)
	rep, err := sch.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Status != RunFailed || tries != 1 {
		t.Fatalf("want single attempt failure, attempts=%d report=%+v", tries, rep)
	}
}

func TestRetryBudgetExhausted(t *testing.T) {
	def := &PipelineDef{Name: "p", MaxRunning: 1, Steps: []StepDef{
		{Name: "a", MaxRetries: 1},
	}}
	tries := 0
	exec := NewFuncExecutor(map[string]func(ctx context.Context, attempt int) StepResult{
		"a": func(ctx context.Context, attempt int) StepResult {
			tries++
			return StepResult{Err: &StepError{Kind: KindRetryable, Message: "always"}}
		},
	})
	sch, _ := newTestScheduler(t, def, exec)
	rep, err := sch.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Status != RunFailed || tries != 2 {
		t.Fatalf("want 2 attempts then fail, got %d rep=%+v", tries, rep)
	}
}

func contains(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

// liveCounter 在步骤真正执行期间被增减，用来验证“整个执行过程中”的并发数。
type liveCounter struct {
	mu      sync.Mutex
	current int
	peak    int
}

func (c *liveCounter) inc() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current++
	if c.current > c.peak {
		c.peak = c.current
	}
	return c.current
}
func (c *liveCounter) dec() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current--
}

func TestGlobalConcurrencyLimitNeverExceeded(t *testing.T) {
	const limit = 2
	// 8 个互不依赖的步骤，全部慢执行，必然有排队时刻。
	var steps []StepDef
	funcs := map[string]func(ctx context.Context, attempt int) StepResult{}
	live := &liveCounter{}
	for i := 0; i < 8; i++ {
		name := "s" + itoa(i)
		steps = append(steps, StepDef{Name: name})
		funcs[name] = func(ctx context.Context, attempt int) StepResult {
			n := live.inc()
			if n > limit {
				t.Errorf("实时并发数 %d 超过全局上限 %d", n, limit)
			}
			time.Sleep(20 * time.Millisecond)
			live.dec()
			return StepResult{}
		}
	}
	def := &PipelineDef{Name: "p", MaxRunning: limit, Steps: steps}
	sch, _ := newTestScheduler(t, def, NewFuncExecutor(funcs))
	rep, err := sch.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Status != RunComplete || len(rep.Succeeded) != 8 {
		t.Fatalf("report: %+v", rep)
	}
	if sch.ConcurrencyViolations() != 0 {
		t.Fatalf("调度器自观测到 %d 次超限", sch.ConcurrencyViolations())
	}
	if live.peak != limit {
		t.Fatalf("期望峰值达到上限 %d，实际峰值 %d（测试未真正压到上限）", limit, live.peak)
	}
	t.Logf("全局上限=%d 执行过程峰值并发=%d 全程未超限", limit, live.peak)
}

func TestResourceQuotaNeverExceeded(t *testing.T) {
	// 全局允许 6 个并行，但 cpu 标签配额只有 2、gpu 只有 1。
	var steps []StepDef
	funcs := map[string]func(ctx context.Context, attempt int) StepResult{}
	cpuLive, gpuLive := &liveCounter{}, &liveCounter{}
	mk := func(name, label string, live *liveCounter) {
		steps = append(steps, StepDef{Name: name, Resources: map[string]string{label: "1"}})
		funcs[name] = func(ctx context.Context, attempt int) StepResult {
			n := live.inc()
			limit := 2
			if label == "gpu" {
				limit = 1
			}
			if n > limit {
				t.Errorf("资源 %s 实时占用 %d 超过配额 %d", label, n, limit)
			}
			time.Sleep(20 * time.Millisecond)
			live.dec()
			return StepResult{}
		}
	}
	for i := 0; i < 5; i++ {
		mk("cpu"+itoa(i), "cpu", cpuLive)
	}
	for i := 0; i < 3; i++ {
		mk("gpu"+itoa(i), "gpu", gpuLive)
	}
	def := &PipelineDef{
		Name: "p", MaxRunning: 6,
		Quotas: map[string]int{"cpu": 2, "gpu": 1},
		Steps:  steps,
	}
	sch, _ := newTestScheduler(t, def, NewFuncExecutor(funcs))
	rep, err := sch.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Status != RunComplete {
		t.Fatalf("report: %+v", rep)
	}
	if sch.ConcurrencyViolations() != 0 {
		t.Fatalf("配额被突破: %d 次", sch.ConcurrencyViolations())
	}
	if cpuLive.peak != 2 || gpuLive.peak != 1 {
		t.Fatalf("资源峰值异常 cpu=%d gpu=%d", cpuLive.peak, gpuLive.peak)
	}
	t.Logf("配额 cpu=2 gpu=1；执行过程峰值 cpu=%d gpu=%d，全程未突破", cpuLive.peak, gpuLive.peak)
}

func TestQueueingActuallyHappens(t *testing.T) {
	// 上限 1 + 慢步骤：若排队失效，单测会在实时计数处失败；这里再断言耗时形态。
	var steps []StepDef
	funcs := map[string]func(ctx context.Context, attempt int) StepResult{}
	live := &liveCounter{}
	for i := 0; i < 4; i++ {
		name := "s" + itoa(i)
		steps = append(steps, StepDef{Name: name})
		funcs[name] = func(ctx context.Context, attempt int) StepResult {
			if live.inc() > 1 {
				t.Errorf("并发超过 1")
			}
			time.Sleep(15 * time.Millisecond)
			live.dec()
			return StepResult{}
		}
	}
	def := &PipelineDef{Name: "p", MaxRunning: 1, Steps: steps}
	sch, _ := newTestScheduler(t, def, NewFuncExecutor(funcs))
	if rep, err := sch.Run(context.Background()); err != nil || rep.Status != RunComplete {
		t.Fatalf("run: %v %+v", err, rep)
	}
	if live.peak != 1 {
		t.Fatalf("峰值应为 1，得到 %d", live.peak)
	}
}
