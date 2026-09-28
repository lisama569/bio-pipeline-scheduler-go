package pipeline

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Config controls an execution run.
type Config struct {
	// GlobalConcurrency caps simultaneously running steps (0 = unlimited).
	GlobalConcurrency int
	// ResourceQuotas caps concurrent slots per resource tag
	// (missing tag / 0 = unlimited).
	ResourceQuotas map[string]int
	// InitialBackoff is the delay before the second attempt; subsequent
	// delays double. Defaults to 50ms when zero.
	InitialBackoff time.Duration
}

// StepFailure records the terminal outcome of one failed step.
type StepFailure struct {
	Step   string
	Status Status
	Reason string
}

// RunError is returned when one or more steps do not succeed.
type RunError struct {
	Failed      []StepFailure
	Unreachable []StepFailure
}

func (e *RunError) Error() string {
	var b strings.Builder
	if len(e.Failed) > 0 {
		names := make([]string, 0, len(e.Failed))
		for _, f := range e.Failed {
			names = append(names, fmt.Sprintf("%s(%s)", f.Step, f.Reason))
		}
		fmt.Fprintf(&b, "%d step(s) failed: %s", len(names), strings.Join(names, ", "))
	}
	if len(e.Unreachable) > 0 {
		if b.Len() > 0 {
			b.WriteString("; ")
		}
		names := make([]string, 0, len(e.Unreachable))
		for _, f := range e.Unreachable {
			names = append(names, f.Step)
		}
		fmt.Fprintf(&b, "%d downstream step(s) not run (upstream failed): %s", len(names), strings.Join(names, ", "))
	}
	return b.String()
}

// runContextRunner is implemented by runners that accept cancellation.
type runContextRunner interface {
	RunContext(ctx context.Context, spec StepSpec) Result
}

// Executor runs a validated spec against a runner with persistence.
type Executor struct {
	spec   *Spec
	runner StepRunner
	store  *Store
	cfg    Config
	logf   func(format string, args ...any)
}

// NewExecutor builds an executor.
func NewExecutor(spec *Spec, runner StepRunner, store *Store, cfg Config) *Executor {
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = 50 * time.Millisecond
	}
	return &Executor{spec: spec, runner: runner, store: store, cfg: cfg, logf: func(string, ...any) {}}
}

// SetLogger attaches a sink receiving "step=... attempt=... status=..." lines.
func (e *Executor) SetLogger(f func(format string, args ...any)) {
	if f != nil {
		e.logf = f
	}
}

// stepRuntime tracks live scheduling state.
type stepRuntime struct {
	spec     StepSpec
	status   Status
	attempts int
	reason   string
	done     bool // terminal
}

// resourcePool is a counting semaphore supporting multi-slot acquisition
// without ever handing out more than capacity units. It is built on a
// buffered channel of unit tokens, so the "capacity never exceeded"
// property is enforced by the channel itself; cancellation races with
// token acquisition via a select and always returns any token it wins.
type resourcePool struct {
	name   string
	tokens chan struct{}
}

func newResourcePool(name string, capacity int) *resourcePool {
	p := &resourcePool{
		name:   name,
		tokens: make(chan struct{}, capacity),
	}
	for i := 0; i < capacity; i++ {
		p.tokens <- struct{}{}
	}
	return p
}

func (p *resourcePool) acquire(ctx context.Context, slots int) bool {
	for i := 0; i < slots; i++ {
		select {
		case <-p.tokens:
		case <-ctx.Done():
			// Return every token acquired by this call so far: never leak.
			for j := 0; j < i; j++ {
				p.tokens <- struct{}{}
			}
			return false
		}
	}
	return true
}

func (p *resourcePool) release(slots int) {
	for i := 0; i < slots; i++ {
		p.tokens <- struct{}{}
	}
}

// Run executes the pipeline. Already-succeeded steps are skipped; pending
// steps run respecting global and per-tag quotas; failures mark downstream
// steps unreachable.
func (e *Executor) Run(ctx context.Context) error {
	steps := make(map[string]*stepRuntime, len(e.spec.Steps))
	known := make(map[string]struct{}, len(e.spec.Steps))
	persisted := e.store.Load()
	for i := range e.spec.Steps {
		sp := e.spec.Steps[i]
		known[sp.Name] = struct{}{}
		rt := &stepRuntime{spec: sp, status: StatusPending}
		if ps, ok := persisted[sp.Name]; ok {
			rt.status = ps.Status
			rt.attempts = ps.Attempts
			rt.reason = ps.Reason
			rt.done = ps.Status == StatusSucceeded || ps.Status == StatusFailed || ps.Status == StatusUnreachable
		}
		steps[sp.Name] = rt
	}
	// Journal must not refer to steps the current spec does not know.
	for name := range persisted {
		if _, ok := known[name]; !ok {
			return fmt.Errorf("persisted state references step %q which is not in the pipeline spec; refusing to run a mismatched pipeline", name)
		}
	}

	n := len(steps)
	globalCap := e.cfg.GlobalConcurrency
	if globalCap <= 0 {
		globalCap = n
	}
	global := newResourcePool("global", globalCap)
	tagPools := make(map[string]*resourcePool)
	for tag, cap := range e.cfg.ResourceQuotas {
		if cap > 0 {
			tagPools[tag] = newResourcePool("tag:"+tag, cap)
		}
	}

	var (
		mu      sync.Mutex
		cond    = sync.Cond{L: &mu}
		wg      sync.WaitGroup
		failed  []StepFailure
		unreach []StepFailure
	)

	// record a transition in memory and on disk (caller must not hold mu;
	// the in-memory publication that unblocks downstream happens later).
	record := func(rt *stepRuntime, status Status, attempts int, reason string) error {
		mu.Lock()
		rt.status = status
		rt.attempts = attempts
		rt.reason = reason
		mu.Unlock()
		if err := e.store.Record(rt.spec.Name, status, attempts, rt.spec.Outputs, reason); err != nil {
			return err
		}
		e.logf("step=%s attempt=%d status=%s reason=%q", rt.spec.Name, attempts, status, reason)
		return nil
	}

	runner, hasContext := e.runner.(runContextRunner)

	// A cancelled context wakes every goroutine blocked on cond.
	stopWatch := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-ctx.Done():
			mu.Lock()
			cond.Broadcast()
			mu.Unlock()
			<-stopWatch
		case <-stopWatch:
		}
	}()

	for i := range e.spec.Steps {
		rt := steps[e.spec.Steps[i].Name]
		if rt.done {
			if rt.status == StatusSucceeded {
				e.logf("step=%s attempt=%d status=%s reason=\"restored from disk, skipped\"", rt.spec.Name, rt.attempts, rt.status)
			} else {
				mu.Lock()
				sf := StepFailure{Step: rt.spec.Name, Status: rt.status, Reason: rt.reason}
				if rt.status == StatusUnreachable {
					unreach = append(unreach, sf)
				} else {
					failed = append(failed, sf)
				}
				mu.Unlock()
				e.logf("step=%s attempt=%d status=%s reason=\"terminal state restored from disk\"", rt.spec.Name, rt.attempts, rt.status)
			}
			continue
		}

		wg.Add(1)
		go func(rt *stepRuntime) {
			defer wg.Done()

			// 1) Wait for every dependency to terminate.
			mu.Lock()
			for {
				if ctx.Err() != nil {
					mu.Unlock()
					return
				}
				badDep := ""
				allDone := true
				for _, dep := range rt.spec.Depends {
					d := steps[dep]
					if !d.done {
						allDone = false
						break
					}
					if d.status != StatusSucceeded {
						badDep = dep
					}
				}
				if allDone {
					if badDep != "" {
						rt.reason = "upstream step " + badDep + " did not succeed"
						mu.Unlock()
						// Persist first (fsync), only then publish the
						// terminal state to waiting downstream steps.
						if err := e.store.Record(rt.spec.Name, StatusUnreachable, rt.attempts, nil, rt.reason); err != nil {
							e.logf("step=%s persist error: %v", rt.spec.Name, err)
							return
						}
						e.logf("step=%s attempt=%d status=%s reason=%q", rt.spec.Name, rt.attempts, StatusUnreachable, rt.reason)
						mu.Lock()
						rt.status = StatusUnreachable
						rt.done = true
						unreach = append(unreach, StepFailure{Step: rt.spec.Name, Status: StatusUnreachable, Reason: rt.reason})
						cond.Broadcast()
						mu.Unlock()
						return
					}
					break
				}
				cond.Wait()
			}
			mu.Unlock()

			// 2) Acquire global + tag budgets (bounded queue, never over cap).
			slots := rt.spec.Resources.Slots
			tag := rt.spec.Resources.Tag
			pool := tagPools[tag]
			if !global.acquire(ctx, 1) {
				return // cancelled while queued; step stays pending
			}
			if pool != nil && !pool.acquire(ctx, slots) {
				global.release(1) // never leak the global unit
				return
			}
			defer global.release(1)
			if pool != nil {
				defer pool.release(slots)
			}

			// 3) Run with retries. Non-retryable failures stop immediately.
			for {
				if ctx.Err() != nil {
					return
				}
				rt.attempts++
				if err := record(rt, StatusRunning, rt.attempts, ""); err != nil {
					e.logf("step=%s persist error: %v", rt.spec.Name, err)
					return
				}
				var res Result
				if hasContext {
					res = runner.RunContext(ctx, rt.spec)
				} else {
					res = e.runner.Run(rt.spec)
				}

				if res.Err == nil {
					if err := e.store.Record(rt.spec.Name, StatusSucceeded, rt.attempts, rt.spec.Outputs, ""); err != nil {
						e.logf("step=%s persist error: %v", rt.spec.Name, err)
						return
					}
					e.logf("step=%s attempt=%d status=%s", rt.spec.Name, rt.attempts, StatusSucceeded)
					mu.Lock()
					rt.status = StatusSucceeded
					rt.done = true
					cond.Broadcast()
					mu.Unlock()
					return
				}

				if !res.Retryable || rt.attempts > rt.spec.Retries {
					rt.reason = res.Reason + ": " + res.Err.Error()
					if err := e.store.Record(rt.spec.Name, StatusFailed, rt.attempts, nil, rt.reason); err != nil {
						e.logf("step=%s persist error: %v", rt.spec.Name, err)
						return
					}
					e.logf("step=%s attempt=%d status=%s reason=%q", rt.spec.Name, rt.attempts, StatusFailed, rt.reason)
					mu.Lock()
					rt.status = StatusFailed
					rt.done = true
					failed = append(failed, StepFailure{Step: rt.spec.Name, Status: StatusFailed, Reason: rt.reason})
					cond.Broadcast()
					mu.Unlock()
					return
				}

				// Retryable: exponential back off and try again.
				e.logf("step=%s attempt=%d status=retrying reason=%q", rt.spec.Name, rt.attempts, res.Reason)
				backoff := e.cfg.InitialBackoff << (rt.attempts - 1)
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return
				}
			}
		}(rt)
	}

	wg.Wait()
	close(stopWatch)
	<-watchDone
	_ = e.store.Snapshot()

	sortFailures := func(in []StepFailure) []StepFailure {
		out := append([]StepFailure(nil), in...)
		sort.Slice(out, func(i, j int) bool { return out[i].Step < out[j].Step })
		return out
	}
	if len(failed) > 0 || len(unreach) > 0 {
		return &RunError{Failed: sortFailures(failed), Unreachable: sortFailures(unreach)}
	}
	return nil
}
