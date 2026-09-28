// Package pipeline implements a DAG-based bioinformatics pipeline engine
// with bounded concurrency, failure propagation, persistent state and
// crash recovery.
package pipeline

// Status represents the lifecycle state of a step.
type Status string

const (
	// StatusPending means the step has never been executed.
	StatusPending Status = "pending"
	// StatusReady means dependencies are done but it is waiting for a slot.
	StatusReady Status = "ready"
	// StatusRunning means an attempt is currently in progress.
	StatusRunning Status = "running"
	// StatusSucceeded means the step finished successfully and produced outputs.
	StatusSucceeded Status = "succeeded"
	// StatusFailed means the step exhausted its retries (terminal failure).
	StatusFailed Status = "failed"
	// StatusUnreachable means an upstream dependency failed (never started).
	StatusUnreachable Status = "unreachable"
)

// Resources describes how a step consumes concurrency budget.
type Resources struct {
	// Tag is an optional resource pool label, e.g. "cpu" or "gpu".
	Tag string `json:"tag,omitempty"`
	// Slots is how many units of the pool the step occupies (defaults to 1).
	Slots int `json:"slots,omitempty"`
}

// StepSpec describes one analysis step loaded from a pipeline file.
type StepSpec struct {
	Name      string    `json:"name"`
	Command   string    `json:"command"`
	Depends   []string  `json:"depends_on,omitempty"`
	Retries   int       `json:"retries,omitempty"`
	Resources Resources `json:"resources,omitempty"`
	Outputs   []string  `json:"outputs,omitempty"`
}

// Spec is the complete pipeline description.
type Spec struct {
	Name  string     `json:"name"`
	Steps []StepSpec `json:"steps"`
}

// Result describes the outcome of a single step execution attempt.
type Result struct {
	// Err is nil on success.
	Err error
	// Retryable indicates whether a failure may be retried.
	Retryable bool
	// Reason is a human-readable failure category.
	Reason string
}

// StepRunner executes one step. Implementations must be safe for concurrent use.
type StepRunner interface {
	Run(spec StepSpec) Result
}
