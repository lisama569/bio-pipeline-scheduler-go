package pipeline

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Load parses and validates a pipeline spec from JSON.
//
// Format:
//
//	{
//	  "name": "pipeline-name",
//	  "steps": [
//	    {
//	      "name": "align",
//	      "command": "minimap2 ...",
//	      "depends_on": ["download"],
//	      "retries": 2,
//	      "resources": {"tag": "cpu", "slots": 2},
//	      "outputs": ["result.bam"]
//	    }
//	  ]
//	}
func Load(r io.Reader) (*Spec, error) {
	var spec Spec
	dec := json.NewDecoder(r)
	if err := dec.Decode(&spec); err != nil {
		return nil, &ValidationError{Kind: KindInvalidSpec, Message: "parse JSON: " + err.Error()}
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	return &spec, nil
}

// Validate checks structural rules of the spec: it must have steps, step
// names must be unique and non-empty, every dependency must exist, and the
// dependency graph must be acyclic. Each failure class gets its own Kind.
func (s *Spec) Validate() error {
	if len(s.Steps) == 0 {
		return &ValidationError{Kind: KindInvalidSpec, Message: "pipeline has no steps"}
	}
	seen := make(map[string]struct{}, len(s.Steps))
	for i := range s.Steps {
		st := &s.Steps[i]
		if strings.TrimSpace(st.Name) == "" {
			return &ValidationError{Kind: KindInvalidSpec, Message: fmt.Sprintf("step at index %d has empty name", i)}
		}
		if strings.TrimSpace(st.Command) == "" {
			return &ValidationError{Kind: KindInvalidSpec, Message: fmt.Sprintf("step %q has empty command", st.Name)}
		}
		if _, dup := seen[st.Name]; dup {
			return &ValidationError{Kind: KindDuplicateStep, Message: "duplicate step name: " + st.Name}
		}
		seen[st.Name] = struct{}{}
	}

	// Missing dependencies are detected before cycle detection so that a
	// dangling edge is never misreported as a cycle.
	for _, st := range s.Steps {
		for _, dep := range st.Depends {
			if _, ok := seen[dep]; !ok {
				return &ValidationError{
					Kind:    KindMissingDependency,
					Message: fmt.Sprintf("step %q depends on unknown step %q", st.Name, dep),
				}
			}
			if dep == st.Name {
				return &ValidationError{Kind: KindCycle, Message: fmt.Sprintf("step %q depends on itself", st.Name)}
			}
		}
	}

	if cycle := findCycle(s.Steps); cycle != nil {
		return &ValidationError{
			Kind:    KindCycle,
			Message: "dependency cycle: " + strings.Join(cycle, " -> ") + " -> " + cycle[0],
		}
	}

	// Apply defaults after validation succeeds.
	for i := range s.Steps {
		if s.Steps[i].Retries < 0 {
			s.Steps[i].Retries = 0
		}
		if s.Steps[i].Resources.Slots <= 0 {
			s.Steps[i].Resources.Slots = 1
		}
	}
	return nil
}

// findCycle returns the node names on a directed cycle, or nil if the
// graph is a DAG. Classic DFS with white/gray/black colouring.
func findCycle(steps []StepSpec) []string {
	const white, gray, black = 0, 1, 2
	color := make(map[string]int, len(steps))
	adj := make(map[string][]string, len(steps))
	for _, st := range steps {
		color[st.Name] = white
		adj[st.Name] = st.Depends
	}

	var stack []string
	var dfs func(string) []string
	dfs = func(u string) []string {
		color[u] = gray
		stack = append(stack, u)
		for _, v := range adj[u] {
			switch color[v] {
			case gray:
				// Found a back edge u -> v; extract v..u out of the stack.
				for i, n := range stack {
					if n == v {
						cyc := append([]string(nil), stack[i:]...)
						return cyc
					}
				}
			case white:
				if cyc := dfs(v); cyc != nil {
					return cyc
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[u] = black
		return nil
	}

	for _, st := range steps {
		if color[st.Name] == white {
			if cyc := dfs(st.Name); cyc != nil {
				return cyc
			}
		}
	}
	return nil
}
