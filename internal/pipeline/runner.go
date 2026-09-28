package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Failure reason categories reported in Result.Reason.
const (
	// ReasonNonRetryable is used for failures the command itself declared
	// permanent (exit code 2 by convention) or missing declared outputs.
	ReasonNonRetryable = "non_retryable"
	// ReasonRetryable is used for ordinary non-zero exits / start failures.
	ReasonRetryable = "retryable"
	// ReasonCancelled is used when the context was cancelled mid-run.
	ReasonCancelled = "cancelled"
)

// ExitFatal is the shell exit code a step may use to declare a permanent,
// non-retryable failure (reserving 1 for ordinary retryable failure).
const ExitFatal = 2

// CommandRunner executes steps as local shell commands. It is safe for
// concurrent use; each Run starts its own process.
type CommandRunner struct {
	dir string
	// logf, when set, receives per-attempt status lines.
	logf func(format string, args ...any)
}

// NewCommandRunner creates a runner that executes commands inside dir.
func NewCommandRunner(dir string) *CommandRunner {
	return &CommandRunner{dir: dir}
}

// SetLogger attaches a sink for human-readable attempt logs.
func (r *CommandRunner) SetLogger(f func(format string, args ...any)) {
	r.logf = f
}

func (r *CommandRunner) log(format string, args ...any) {
	if r.logf != nil {
		r.logf(format, args...)
	}
}

// Run executes one step via "sh -c" inside the working directory.
//
// Retry classification:
//   - exit code 2 -> non-retryable (the step declares a permanent error),
//   - any other non-zero exit / inability to start -> retryable,
//   - missing declared outputs after a zero exit -> non-retryable,
//   - context cancellation -> non-retryable (no point retrying a shutdown).
func (r *CommandRunner) Run(spec StepSpec) Result {
	ctx := context.Background()
	return r.RunContext(ctx, spec)
}

// RunContext is Run with cancellation support.
func (r *CommandRunner) RunContext(ctx context.Context, spec StepSpec) Result {
	cmd := exec.CommandContext(ctx, "sh", "-c", spec.Command)
	cmd.Dir = r.dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	startErr := cmd.Start()
	if startErr != nil {
		if ctx.Err() != nil {
			return Result{Err: startErr, Retryable: false, Reason: ReasonCancelled}
		}
		return Result{Err: startErr, Retryable: true, Reason: ReasonRetryable}
	}

	waitErr := cmd.Wait()
	if ctx.Err() != nil {
		return Result{Err: ctx.Err(), Retryable: false, Reason: ReasonCancelled}
	}
	if waitErr != nil {
		code := 1
		var exitErr *exec.ExitError
		if asErr(waitErr, &exitErr) {
			code = exitErr.ExitCode()
		}
		if code == ExitFatal {
			return Result{Err: fmt.Errorf("step %q exited %d (declared fatal)", spec.Name, code),
				Retryable: false, Reason: ReasonNonRetryable}
		}
		return Result{Err: fmt.Errorf("step %q exited %d", spec.Name, code),
			Retryable: true, Reason: ReasonRetryable}
	}

	// Zero exit: verify every declared output exists and is non-empty.
	missing, err := verifyOutputs(r.dir, spec.Outputs)
	if err != nil {
		return Result{Err: err, Retryable: false, Reason: ReasonNonRetryable}
	}
	if len(missing) > 0 {
		return Result{
			Err:       fmt.Errorf("step %q reported success but outputs are missing or empty: %s", spec.Name, strings.Join(missing, ", ")),
			Retryable: false,
			Reason:    ReasonNonRetryable,
		}
	}
	return Result{}
}

// verifyOutputs returns paths that do not exist or have zero length.
// Relative paths resolve against the working directory.
func verifyOutputs(dir string, outputs []string) ([]string, error) {
	var missing []string
	for _, out := range outputs {
		p := out
		if !filepath.IsAbs(p) {
			p = filepath.Join(dir, p)
		}
		info, err := os.Stat(p)
		if err != nil {
			missing = append(missing, out)
			continue
		}
		if info.IsDir() || info.Size() == 0 {
			missing = append(missing, out)
		}
	}
	return missing, nil
}

// asErr extracts the concrete *exec.ExitError from a wrapped error.
func asErr(err error, target **exec.ExitError) bool {
	return errors.As(err, target)
}
