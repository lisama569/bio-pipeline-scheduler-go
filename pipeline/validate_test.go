package pipeline

import (
	"errors"
	"testing"
)

func kinds(err error) map[DefErrorKind]bool {
	var ve *ValidationError
	if !errors.As(err, &ve) {
		return nil
	}
	m := map[DefErrorKind]bool{}
	for _, p := range ve.Problems {
		m[p.Kind] = true
	}
	return m
}

func TestValidateOK(t *testing.T) {
	def := &PipelineDef{Steps: []StepDef{
		{Name: "a"},
		{Name: "b", DependsOn: []string{"a"}},
		{Name: "c", DependsOn: []string{"a"}},
		{Name: "d", DependsOn: []string{"b", "c"}},
	}}
	if err := def.Validate(); err != nil {
		t.Fatalf("expected valid, got %v", err)
	}
}

func TestValidateMissingDep(t *testing.T) {
	def := &PipelineDef{Steps: []StepDef{
		{Name: "a", DependsOn: []string{"ghost"}},
	}}
	m := kinds(def.Validate())
	if !m[ErrMissingDep] {
		t.Fatalf("expected missing_dependency, got %v", m)
	}
	if m[ErrCycle] {
		t.Fatalf("did not expect cycle")
	}
}

func TestValidateCycle(t *testing.T) {
	def := &PipelineDef{Steps: []StepDef{
		{Name: "a", DependsOn: []string{"c"}},
		{Name: "b", DependsOn: []string{"a"}},
		{Name: "c", DependsOn: []string{"b"}},
	}}
	m := kinds(def.Validate())
	if !m[ErrCycle] {
		t.Fatalf("expected cycle, got %v", m)
	}
}

func TestValidateSelfDependency(t *testing.T) {
	def := &PipelineDef{Steps: []StepDef{
		{Name: "a", DependsOn: []string{"a"}},
	}}
	m := kinds(def.Validate())
	if !m[ErrSelfDependency] {
		t.Fatalf("expected self_dependency, got %v", m)
	}
}

func TestValidateDuplicateAndEmpty(t *testing.T) {
	def := &PipelineDef{Steps: []StepDef{
		{Name: "a"},
		{Name: "a"},
		{Name: ""},
	}}
	m := kinds(def.Validate())
	if !m[ErrDuplicateStep] || !m[ErrEmptyName] {
		t.Fatalf("expected duplicate+empty, got %v", m)
	}
}

func TestValidateCycleAndMissingTogether(t *testing.T) {
	def := &PipelineDef{Steps: []StepDef{
		{Name: "a", DependsOn: []string{"b"}},
		{Name: "b", DependsOn: []string{"a"}},
		{Name: "c", DependsOn: []string{"nope"}},
	}}
	m := kinds(def.Validate())
	if !m[ErrCycle] || !m[ErrMissingDep] {
		t.Fatalf("expected both cycle and missing dep, got %v", m)
	}
}
