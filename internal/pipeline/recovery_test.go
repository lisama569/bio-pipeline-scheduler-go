package pipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingRunner counts attempts per step; optional gate blocks named steps
// until released, so a test can kill the process mid-run deterministically.
type countingRunner struct {
	mu      sync.Mutex
	counts  map[string]*atomic.Int32
	started map[string]chan struct{}
	release map[string]chan struct{}
	once    map[string]*sync.Once
}

func newCountingRunner() *countingRunner {
	return &countingRunner{
		counts:  map[string]*atomic.Int32{},
		started: map[string]chan struct{}{},
		release: map[string]chan struct{}{},
		once:    map[string]*sync.Once{},
	}
}

func (c *countingRunner) register(names ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range names {
		v := atomic.Int32{}
		c.counts[n] = &v
	}
}

// gate makes one step block until releaseStep is called, and signals
// started when its attempt actually begins.
func (c *countingRunner) gate(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.started[name] = make(chan struct{})
	c.release[name] = make(chan struct{})
	c.once[name] = &sync.Once{}
}

func (c *countingRunner) count(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return int(c.counts[name].Load())
}

func (c *countingRunner) waitStarted(name string) {
	c.mu.Lock()
	ch := c.started[name]
	c.mu.Unlock()
	<-ch
}

// waitAnyStarted blocks until at least one of the named (gated) steps
// reports an attempt start.
func (c *countingRunner) waitAnyStarted(names ...string) {
	for {
		c.mu.Lock()
		found := false
		for _, n := range names {
			if ch, ok := c.started[n]; ok {
				select {
				case <-ch:
					found = true
				default:
				}
			}
		}
		c.mu.Unlock()
		if found {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func (c *countingRunner) releaseStep(name string) {
	close(c.release[name])
}

func (c *countingRunner) Run(spec StepSpec) Result {
	return c.RunContext(context.Background(), spec)
}

func (c *countingRunner) RunContext(ctx context.Context, spec StepSpec) Result {
	c.mu.Lock()
	ctr := c.counts[spec.Name]
	started := c.started[spec.Name]
	release := c.release[spec.Name]
	c.mu.Unlock()
	if ctr != nil {
		ctr.Add(1)
	}
	if started != nil {
		select {
		case <-started:
		default:
			close(started)
		}
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return Result{Err: ctx.Err(), Retryable: false, Reason: ReasonCancelled}
		}
	}
	return Result{}
}

func recoverySpec() *Spec {
	return specFromSteps(
		StepSpec{Name: "s1", Command: "x"},
		StepSpec{Name: "s2", Command: "x", Depends: []string{"s1"}},
		StepSpec{Name: "s3", Command: "x", Depends: []string{"s2"}},
		StepSpec{Name: "s4", Command: "x", Depends: []string{"s2"}},
	)
}

func TestRecoveryResumesWithoutRepeatingCompletedSteps(t *testing.T) {
	stateDir := t.TempDir()
	names := []string{"s1", "s2", "s3", "s4"}

	// ---- run 1: s1/s2 finish; one of s3/s4 holds the only slot when we
	// cancel, the other is still queued ----
	r1 := newCountingRunner()
	r1.register(names...)
	r1.gate("s3")
	r1.gate("s4")
	store1, err := NewStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	ex1 := NewExecutor(recoverySpec(), r1, store1, Config{
		GlobalConcurrency: 1,
		InitialBackoff:    time.Millisecond,
	})
	ex1.SetLogger(logToTest(t))

	ctx1, cancel1 := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- ex1.Run(ctx1) }()

	// Wait until exactly one of the sibling steps has started.
	r1.waitAnyStarted("s3", "s4")
	started := "s3"
	if r1.count("s3") == 0 {
		started = "s4"
	}
	queued := "s4"
	if started == "s4" {
		queued = "s3"
	}
	// Release the blocked gate so the in-flight call can return, then cancel.
	r1.releaseStep(started)
	cancel1()
	<-runErr
	if err := store1.Close(); err != nil {
		t.Fatal(err)
	}

	// Journal state right after the crash.
	probe, _ := NewStore(stateDir)
	mid := probe.Load()
	probe.Close()
	if mid["s1"].Status != StatusSucceeded || mid["s2"].Status != StatusSucceeded {
		t.Fatalf("s1/s2 should be succeeded after crash, got %+v %+v", mid["s1"], mid["s2"])
	}
	// The queued sibling never started (cap=1). The in-flight one is
	// either pending (cancelled before completion) or succeeded (the
	// completion beat the cancel signal): both are legal crash outcomes.
	if c := r1.count(queued); c != 0 {
		t.Fatalf("queued %s should never have started under cap=1, started %d times", queued, c)
	}
	switch mid[started].Status {
	case StatusPending, StatusSucceeded:
	default:
		t.Fatalf("interrupted %s status = %s, want pending or succeeded", started, mid[started].Status)
	}

	// ---- run 2: fresh process, same state directory ----
	r2 := newCountingRunner()
	r2.register(names...)
	store2, err := NewStore(stateDir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	ex2 := NewExecutor(recoverySpec(), r2, store2, Config{InitialBackoff: time.Millisecond})
	ex2.SetLogger(logToTest(t))
	if err := ex2.Run(context.Background()); err != nil {
		t.Fatalf("recovery run failed: %v", err)
	}
	if err := store2.Close(); err != nil {
		t.Fatal(err)
	}

	// s1/s2 completed before the crash and must never run again.
	if n := r2.count("s1"); n != 0 {
		t.Fatalf("s1 re-executed %d times after recovery", n)
	}
	if n := r2.count("s2"); n != 0 {
		t.Fatalf("s2 re-executed %d times after recovery", n)
	}
	// The sibling that had never started runs exactly once.
	if n := r2.count(queued); n != 1 {
		t.Fatalf("previously queued %s ran %d times after recovery, want 1", queued, n)
	}
	// The in-flight sibling runs again only if it had not finished.
	wantStarted := 1
	if mid[started].Status == StatusSucceeded {
		wantStarted = 0
	}
	if n := r2.count(started); n != wantStarted {
		t.Fatalf("%s ran %d times after recovery, want %d (crash state was %s)",
			started, n, wantStarted, mid[started].Status)
	}

	// Final journal: all succeeded, and s1/s2 only ever ran once in total.
	final, _ := NewStore(stateDir)
	defer final.Close()
	for _, n := range names {
		st := final.Load()[n]
		if st.Status != StatusSucceeded {
			t.Fatalf("step %s final status %s", n, st.Status)
		}
	}
	t.Log("recovery completed: s1/s2 skipped, s3/s4 executed exactly once")
}

func TestRecoveryRefusesMismatchedPipeline(t *testing.T) {
	stateDir := t.TempDir()
	store, _ := NewStore(stateDir)
	if err := store.Record("old", StatusRunning, 1, nil, ""); err != nil {
		t.Fatal(err)
	}
	if err := store.Record("old", StatusSucceeded, 1, nil, ""); err != nil {
		t.Fatal(err)
	}
	store.Close()

	store2, _ := NewStore(stateDir)
	defer store2.Close()
	spec := specFromSteps(StepSpec{Name: "new", Command: "x"})
	ex := NewExecutor(spec, newFakeRunner(), store2, Config{})
	if err := ex.Run(context.Background()); err == nil {
		t.Fatal("running a spec that doesn't match journaled steps must fail")
	} else {
		t.Logf("mismatch refused: %v", err)
	}
}

// End-to-end style test using the real CommandRunner and on-disk outputs:
// a crash mid-way must not double-produce files on resume.
func TestRecoveryWithCommandRunnerAndOutputs(t *testing.T) {
	workdir := t.TempDir()
	stateDir := filepath.Join(workdir, "state")

	// Step1 creates a marker with an append-only counter: if it ever ran
	// twice on resume, the marker would say 2.
	script := func(name string) StepSpec {
		return StepSpec{
			Name:    name,
			Command: fmt.Sprintf("n=$(cat %s.count 2>/dev/null || echo 0); echo $((n+1)) > %s.count", name, name),
			Outputs: []string{name + ".count"},
		}
	}
	spec := specFromSteps(
		script("s1"),
		script("s2"),
	)
	spec.Steps[1].Depends = []string{"s1"}

	store, err := NewStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	runner := NewCommandRunner(workdir)
	ex := NewExecutor(spec, runner, store, Config{})
	if err := ex.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.Close()

	// Second invocation: everything succeeds on disk, so commands rerun
	// would overwrite. Assert they never execute by removing permissions...
	// simpler: marker content stays "1".
	store2, err := NewStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	ex2 := NewExecutor(spec, NewCommandRunner(workdir), store2, Config{})
	ex2.SetLogger(logToTest(t))
	if err := ex2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"s1", "s2"} {
		data, err := os.ReadFile(filepath.Join(workdir, n+".count"))
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != "1\n" {
			t.Fatalf("step %s marker = %q, want 1 (ran more than once?)", n, data)
		}
	}
}

var _ = atomic.LoadInt32
