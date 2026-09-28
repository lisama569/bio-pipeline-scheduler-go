package pipeline

import (
	"context"
	"strings"
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
	log := NewLogger(&strings.Builder{})
	sch := NewScheduler(def, s, exec, log)
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
