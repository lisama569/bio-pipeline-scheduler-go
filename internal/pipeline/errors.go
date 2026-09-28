package pipeline

import "fmt"

// Validation error kinds. Callers can type-assert *ValidationError and
// switch on Kind to tell failure classes apart.
const (
	// KindInvalidSpec: malformed JSON or structurally unusable spec.
	KindInvalidSpec = "invalid_spec"
	// KindDuplicateStep: two steps share the same name.
	KindDuplicateStep = "duplicate_step"
	// KindMissingDependency: a depends_on entry names an unknown step.
	KindMissingDependency = "missing_dependency"
	// KindCycle: the dependency graph contains a directed cycle.
	KindCycle = "cycle"
)

// ValidationError is returned when a pipeline spec is invalid.
type ValidationError struct {
	Kind    string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("pipeline validation failed [%s]: %s", e.Kind, e.Message)
}

// IsCycle reports whether err is a dependency-cycle validation error.
func IsCycle(err error) bool {
	if ve, ok := err.(*ValidationError); ok {
		return ve.Kind == KindCycle
	}
	return false
}

// IsMissingDependency reports whether err references an unknown step.
func IsMissingDependency(err error) bool {
	if ve, ok := err.(*ValidationError); ok {
		return ve.Kind == KindMissingDependency
	}
	return false
}

// RollbackError is returned when persisted state claims a completed step
// has been moved backwards to a non-terminal state.
type RollbackError struct {
	Step   string
	From   Status
	To     Status
	Reason string
}

func (e *RollbackError) Error() string {
	return fmt.Sprintf("illegal state rollback for step %q: %s -> %s (%s)",
		e.Step, e.From, e.To, e.Reason)
}
