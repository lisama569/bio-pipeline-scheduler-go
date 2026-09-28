package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func crashDef() *PipelineDef {
	return &PipelineDef{Name: "p", MaxRunning: 2, Steps: []StepDef{
		{Name: "a"},
		{Name: "b", DependsOn: []string{"a"}},
		{Name: "c", DependsOn: []string{"a"}},
		{Name: "d", DependsOn: []string{"b", "c"}},
		{Name: "e", DependsOn: []string{"d"}},
	}}
}

// procExecutor 模拟随时可能在“某个步骤提交成功后”被杀的进程：
// 每完成 budget 个步骤就取消进程 ctx，其他正在执行的步骤随即中断（停在 running）。
type procExecutor struct {
	hold          time.Duration
	remaining     int32
	kill          context.CancelFunc
	successCounts *sync.Map
}

func (e *procExecutor) Execute(ctx context.Context, d StepDef, attempt int) StepResult {
	select {
	case <-time.After(e.hold):
	case <-ctx.Done():
		// 模拟步骤在执行中途因进程被杀死而中断：返回可重试的 ctx 错误。
		// 进程随后结束（Run 在有界优雅期内收尾并释放文件锁），
		// running 现场由下次启动按 stale running 恢复。
		return StepResult{Err: &StepError{Kind: KindRetryable, Message: "interrupted by kill"}}
	}
	if v, ok := e.successCounts.Load(d.Name); ok {
		atomic.AddInt64(v.(*int64), 1)
	} else {
		var n int64 = 1
		e.successCounts.LoadOrStore(d.Name, &n)
	}
	// 该步骤已完整执行；若本进程预算用尽，在调度器提交完这次成功后再杀进程，
	// 模拟“成功已落盘、随后进程死亡”这一最常见崩溃点。
	if atomic.AddInt32(&e.remaining, -1) <= 0 {
		go func() {
			time.Sleep(25 * time.Millisecond)
			e.kill()
		}()
	}
	return StepResult{}
}

func TestRepeatedCrashThenConverge(t *testing.T) {
	def := crashDef()
	stateDir := t.TempDir()
	var successCounts sync.Map

	runLife := func(budget int32) (*RunReport, error) {
		s, err := NewStore(stateDir)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		exec := &procExecutor{
			hold:          10 * time.Millisecond,
			remaining:     budget,
			kill:          cancel,
			successCounts: &successCounts,
		}
		sch := NewScheduler(def, s, exec, testLogger(t))
		sch.SetBackoff(time.Millisecond)
		return sch.Run(ctx)
	}

	// 连续 6 个短命进程，每个只允许 1 个步骤完整提交即被杀。
	for i := 0; i < 6; i++ {
		_, _ = runLife(1)
	}
	// 最后一个进程预算充足，必须收敛为全部成功。
	rep, err := runLife(100)
	if err != nil {
		t.Fatalf("final run: %v", err)
	}
	if rep.Status != RunComplete || len(rep.Succeeded) != 5 {
		t.Fatalf("未收敛: %+v", rep)
	}
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		v, ok := successCounts.Load(name)
		var n int64
		if ok {
			n = atomic.LoadInt64(v.(*int64))
		}
		if n != 1 {
			t.Fatalf("步骤 %s 成功提交 %d 次（必须恰好 1 次，不能重复产出）", name, n)
		}
	}
	t.Log("多次崩溃后重启收敛，a/b/c/d/e 各自成功提交恰好 1 次")
}
