package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeStep models one step's behaviour in tests.
type fakeStep struct {
	delay      time.Duration
	fail       int  // number of initial failures
	fatal      bool // failures are non-retryable
	errMsg     string
	failsCount int32  // atomic: attempts observed so far
	markStart  func() // called as the attempt begins (under any locks)
}

// fakeRunner is an in-memory StepRunner that tracks live concurrency with
// a high-watermark counter. Every start/update is sampled so a test can
// assert the limit held for the entire run, not just at the end.
type fakeRunner struct {
	mu         sync.Mutex
	steps      map[string]*fakeStep
	running    atomic.Int32
	peak       atomic.Int32
	tagRunning map[string]*atomic.Int32
	tagPeak    map[string]*atomic.Int32
	order      []string
	orderMu    sync.Mutex
	sampler    func()
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{
		steps:      map[string]*fakeStep{},
		tagRunning: map[string]*atomic.Int32{},
		tagPeak:    map[string]*atomic.Int32{},
	}
}

func (r *fakeRunner) set(name string, fs fakeStep) *fakeStep {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.steps[name] = &fs
	return &fs
}

func (r *fakeRunner) Run(spec StepSpec) Result {
	r.mu.Lock()
	fs := r.steps[spec.Name]
	r.mu.Unlock()
	if fs == nil {
		fs = &fakeStep{}
	}

	cur := r.running.Add(1)
	for {
		peak := r.peak.Load()
		if cur <= peak || r.peak.CompareAndSwap(peak, cur) {
			break
		}
	}
	if spec.Resources.Tag != "" {
		r.mu.Lock()
		c, ok := r.tagRunning[spec.Resources.Tag]
		if !ok {
			c = &atomic.Int32{}
			r.tagRunning[spec.Resources.Tag] = c
			r.tagPeak[spec.Resources.Tag] = &atomic.Int32{}
		}
		p := r.tagPeak[spec.Resources.Tag]
		r.mu.Unlock()
		n := c.Add(int32(spec.Resources.Slots))
		for {
			old := p.Load()
			if n <= old || p.CompareAndSwap(old, n) {
				break
			}
		}
	}
	if fs.markStart != nil {
		fs.markStart()
	}
	if r.sampler != nil {
		r.sampler()
	}

	done := make(chan struct{})
	go func() {
		select {
		case <-time.After(fs.delay):
		case <-done:
		}
	}()
	time.Sleep(fs.delay)
	close(done)

	r.running.Add(-1)
	if spec.Resources.Tag != "" {
		r.mu.Lock()
		c := r.tagRunning[spec.Resources.Tag]
		r.mu.Unlock()
		c.Add(-int32(spec.Resources.Slots))
	}

	r.orderMu.Lock()
	r.order = append(r.order, spec.Name)
	r.orderMu.Unlock()

	fails := atomic.AddInt32(&fs.failsCount, 1) <= int32(fs.fail)
	if fails {
		if fs.fatal {
			return Result{Err: fmt.Errorf("%s: fatal", spec.Name), Retryable: false, Reason: ReasonNonRetryable}
		}
		return Result{Err: fmt.Errorf("%s: boom", spec.Name), Retryable: true, Reason: ReasonRetryable}
	}
	return Result{}
}

func specFromSteps(steps ...StepSpec) *Spec { return &Spec{Name: "test", Steps: steps} }

func logToTest(t *testing.T) func(string, ...any) {
	t.Helper()
	var mu sync.Mutex
	return func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		t.Logf(format, args...)
	}
}

func runWithStore(t *testing.T, spec *Spec, runner StepRunner, cfg Config) (error, *Store) {
	t.Helper()
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ex := NewExecutor(spec, runner, store, cfg)
	ex.SetLogger(logToTest(t))
	return ex.Run(context.Background()), store
}

// ---- tests ---------------------------------------------------------------

func TestExecutorRunsInDependencyOrder(t *testing.T) {
	r := newFakeRunner()
	r.set("a", fakeStep{delay: 30 * time.Millisecond})
	r.set("b", fakeStep{delay: 10 * time.Millisecond})
	r.set("c", fakeStep{})
	spec := specFromSteps(
		StepSpec{Name: "a", Command: "x"},
		StepSpec{Name: "b", Command: "x", Depends: []string{"a"}},
		StepSpec{Name: "c", Command: "x", Depends: []string{"a", "b"}},
	)
	err, store := runWithStore(t, spec, r, Config{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	pos := map[string]int{}
	for i, n := range r.order {
		pos[n] = i
	}
	if !(pos["a"] < pos["b"] && pos["b"] < pos["c"]) {
		t.Fatalf("order violated: %v", r.order)
	}
	for _, n := range []string{"a", "b", "c"} {
		if store.Load()[n].Status != StatusSucceeded {
			t.Fatalf("step %s not succeeded", n)
		}
	}
}

func TestExecutorConcurrencyNeverExceedsGlobalLimit(t *testing.T) {
	const limit = 2
	r := newFakeRunner()
	steps := []StepSpec{}
	for i := 0; i < 8; i++ {
		name := fmt.Sprintf("s%d", i)
		r.set(name, fakeStep{delay: 25 * time.Millisecond})
		steps = append(steps, StepSpec{Name: name, Command: "x"})
	}
	spec := specFromSteps(steps...)

	// Independently sample the live counter from another goroutine for the
	// *entire duration* of the run.
	var samplePeak atomic.Int32
	var sampleWG sync.WaitGroup
	sampleWG.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer sampleWG.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				n := r.running.Load()
				for {
					old := samplePeak.Load()
					if n <= old || samplePeak.CompareAndSwap(old, n) {
						break
					}
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()

	err, _ := runWithStore(t, spec, r, Config{GlobalConcurrency: limit, InitialBackoff: time.Millisecond})
	cancel()
	sampleWG.Wait()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if peak := r.peak.Load(); peak > limit {
		t.Fatalf("in-run counter peaked at %d, global limit is %d", peak, limit)
	}
	if peak := samplePeak.Load(); peak > limit {
		t.Fatalf("external sampler observed %d concurrent steps, limit is %d", peak, limit)
	}
	if r.peak.Load() < limit {
		t.Fatalf("pipeline never reached the limit (peak=%d); test would not prove anything", r.peak.Load())
	}
	t.Logf("global limit=%d respected throughout; observed peak=%d", limit, samplePeak.Load())
}

func TestExecutorResourceTagQuota(t *testing.T) {
	r := newFakeRunner()
	steps := []StepSpec{}
	// 3 gpu slots total across 4 steps that each want 2 slots: with quota 4,
	// at most 2 may run at once; cpu steps also run but are a separate pool.
	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("gpu%d", i)
		r.set(name, fakeStep{delay: 20 * time.Millisecond})
		steps = append(steps, StepSpec{Name: name, Command: "x",
			Resources: Resources{Tag: "gpu", Slots: 2}})
	}
	for i := 0; i < 2; i++ {
		name := fmt.Sprintf("cpu%d", i)
		r.set(name, fakeStep{delay: 20 * time.Millisecond})
		steps = append(steps, StepSpec{Name: name, Command: "x",
			Resources: Resources{Tag: "cpu", Slots: 1}})
	}
	spec := specFromSteps(steps...)
	err, _ := runWithStore(t, spec, r, Config{
		GlobalConcurrency: 10,
		ResourceQuotas:    map[string]int{"gpu": 4, "cpu": 1},
		InitialBackoff:    time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.tagPeak["gpu"].Load() > 4 {
		t.Fatalf("gpu slots peaked at %d > quota 4", r.tagPeak["gpu"].Load())
	}
	if r.tagPeak["cpu"].Load() > 1 {
		t.Fatalf("cpu slots peaked at %d > quota 1", r.tagPeak["cpu"].Load())
	}
	t.Logf("tag quotas respected: gpu peak slots=%d, cpu peak slots=%d",
		r.tagPeak["gpu"].Load(), r.tagPeak["cpu"].Load())
}

func TestExecutorFailureMarksDownstreamUnreachable(t *testing.T) {
	r := newFakeRunner()
	r.set("good", fakeStep{})
	r.set("bad", fakeStep{fail: 1, errMsg: "x"})
	r.set("child", fakeStep{})
	r.set("grandchild", fakeStep{})
	r.set("independent", fakeStep{})
	spec := specFromSteps(
		StepSpec{Name: "good", Command: "x"},
		StepSpec{Name: "bad", Command: "x", Depends: []string{"good"}, Retries: 0},
		StepSpec{Name: "child", Command: "x", Depends: []string{"bad"}},
		StepSpec{Name: "grandchild", Command: "x", Depends: []string{"child"}},
		StepSpec{Name: "independent", Command: "x", Depends: []string{"good"}},
	)
	err, store := runWithStore(t, spec, r, Config{InitialBackoff: time.Millisecond})
	if err == nil {
		t.Fatal("expected RunError")
	}
	var re *RunError
	if !errors.As(err, &re) {
		t.Fatalf("want *RunError, got %T", err)
	}
	got := map[string]Status{}
	for _, f := range re.Failed {
		got[f.Step] = f.Status
	}
	for _, f := range re.Unreachable {
		got[f.Step] = f.Status
	}
	if got["bad"] != StatusFailed {
		t.Fatalf("bad = %v, want failed", got["bad"])
	}
	if got["child"] != StatusUnreachable || got["grandchild"] != StatusUnreachable {
		t.Fatalf("downstream not unreachable: %+v", got)
	}
	if _, ran := got["independent"]; ran {
		t.Fatalf("independent step must still succeed, got %v", got["independent"])
	}
	states := store.Load()
	if states["independent"].Status != StatusSucceeded {
		t.Fatalf("independent = %s, want succeeded", states["independent"].Status)
	}
	// Downstream runners must never have been invoked.
	for _, n := range []string{"child", "grandchild"} {
		for _, ran := range r.order {
			if ran == n {
				t.Fatalf("unreachable step %q was executed", n)
			}
		}
	}
	t.Logf("failure propagation: %v", err)
}

func TestExecutorRetryableFailuresAreRetried(t *testing.T) {
	r := newFakeRunner()
	r.set("flaky", fakeStep{fail: 2, delay: 5 * time.Millisecond})
	spec := specFromSteps(StepSpec{Name: "flaky", Command: "x", Retries: 2})
	err, store := runWithStore(t, spec, r, Config{InitialBackoff: time.Millisecond})
	if err != nil {
		t.Fatalf("retryable failures should succeed within budget, got %v", err)
	}
	if n := store.Load()["flaky"].Attempts; n != 3 {
		t.Fatalf("attempts = %d, want 3", n)
	}
}

func TestExecutorNonRetryableFailsImmediately(t *testing.T) {
	r := newFakeRunner()
	r.set("dead", fakeStep{fail: 5, fatal: true})
	spec := specFromSteps(StepSpec{Name: "dead", Command: "x", Retries: 5})
	err, store := runWithStore(t, spec, r, Config{InitialBackoff: time.Millisecond})
	var re *RunError
	if !errors.As(err, &re) || len(re.Failed) != 1 {
		t.Fatalf("want one terminal failure, got %v", err)
	}
	if n := store.Load()["dead"].Attempts; n != 1 {
		t.Fatalf("non-retryable step attempted %d times, want exactly 1", n)
	}
	if !strings.Contains(re.Failed[0].Reason, ReasonNonRetryable) {
		t.Fatalf("reason should classify non_retryable: %s", re.Failed[0].Reason)
	}
	t.Logf("non-retryable failure not retried: %v", err)
}

func TestExecutorRetryBudgetExhaustedFails(t *testing.T) {
	r := newFakeRunner()
	r.set("alwaysbad", fakeStep{fail: 100})
	spec := specFromSteps(StepSpec{Name: "alwaysbad", Command: "x", Retries: 2})
	err, store := runWithStore(t, spec, r, Config{InitialBackoff: time.Millisecond})
	if err == nil {
		t.Fatal("expected failure after exhausting retries")
	}
	if n := store.Load()["alwaysbad"].Attempts; n != 3 {
		t.Fatalf("attempts = %d, want 3 (1 + 2 retries)", n)
	}
}

// TestGlobalLimitOneQueuesWithoutOverlap proves that with a single global
// slot, two independent steps never overlap: while one is blocked the
// other cannot acquire the slot. Whichever goroutine wins the slot first,
// the second must stay at zero attempts until the first releases.
func TestGlobalLimitOneQueuesWithoutOverlap(t *testing.T) {
	r := newCountingRunner()
	r.register("alpha", "beta")
	r.gate("alpha")
	r.gate("beta")
	spec := specFromSteps(
		StepSpec{Name: "alpha", Command: "x"},
		StepSpec{Name: "beta", Command: "x"},
	)
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ex := NewExecutor(spec, r, store, Config{GlobalConcurrency: 1, InitialBackoff: time.Millisecond})
	ex.SetLogger(logToTest(t))

	done := make(chan error, 1)
	go func() { done <- ex.Run(context.Background()) }()

	// Wait until exactly one of them is in flight.
	deadline := time.Now().Add(5 * time.Second)
	for r.count("alpha")+r.count("beta") == 0 {
		if time.Now().After(deadline) {
			t.Fatal("neither step started")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	total := r.count("alpha") + r.count("beta")
	if total != 1 {
		t.Fatalf("with cap=1, %d steps started concurrently (alpha=%d beta=%d)",
			total, r.count("alpha"), r.count("beta"))
	}
	first := "alpha"
	if r.count("beta") == 1 {
		first = "beta"
	}
	second := "beta"
	if first == "beta" {
		second = "alpha"
	}
	r.releaseStep(first)

	deadline = time.Now().Add(5 * time.Second)
	for r.count(second) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("queued step never started after slot release")
		}
		time.Sleep(time.Millisecond)
	}
	// Second now holds the slot and is gated: it must not finish yet, but
	// there must never be more than one start total beyond alpha+beta each once.
	r.releaseStep(second)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if r.count("alpha") != 1 || r.count("beta") != 1 {
		t.Fatalf("each step should run exactly once: alpha=%d beta=%d",
			r.count("alpha"), r.count("beta"))
	}
	t.Logf("cap=1 enforced: %s ran first, %s queued and ran after release", first, second)
}
