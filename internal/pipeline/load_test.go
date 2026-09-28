package pipeline

import (
	"errors"
	"strings"
	"testing"
)

func mustLoad(t *testing.T, jsonDoc string) *Spec {
	t.Helper()
	spec, err := Load(strings.NewReader(jsonDoc))
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}
	return spec
}

func TestValidateAcceptsDAG(t *testing.T) {
	doc := `{
		"name": "exome",
		"steps": [
			{"name": "download", "command": "echo download", "outputs": []},
			{"name": "align", "command": "echo align", "depends_on": ["download"]},
			{"name": "call", "command": "echo call", "depends_on": ["align"]}
		]
	}`
	spec := mustLoad(t, doc)
	if len(spec.Steps) != 3 {
		t.Fatalf("want 3 steps, got %d", len(spec.Steps))
	}
	// Defaults applied.
	if spec.Steps[0].Resources.Slots != 1 {
		t.Fatalf("default slots = %d, want 1", spec.Steps[0].Resources.Slots)
	}
}

func TestValidateCycleDetectedAndClassified(t *testing.T) {
	doc := `{
		"name": "cyclic",
		"steps": [
			{"name": "a", "command": "echo a", "depends_on": ["c"]},
			{"name": "b", "command": "echo b", "depends_on": ["a"]},
			{"name": "c", "command": "echo c", "depends_on": ["b"]}
		]
	}`
	_, err := Load(strings.NewReader(doc))
	if err == nil {
		t.Fatal("expected cycle error, got nil")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) || ve.Kind != KindCycle {
		t.Fatalf("want KindCycle, got %v (type %T)", err, err)
	}
	if !IsCycle(err) {
		t.Fatal("IsCycle should be true")
	}
	if !strings.Contains(err.Error(), "a") || !strings.Contains(err.Error(), "->") {
		t.Fatalf("cycle error should name the cycle, got: %v", err)
	}
	t.Logf("cycle correctly rejected: %v", err)
}

func TestValidateSelfDependencyIsCycle(t *testing.T) {
	doc := `{"steps":[{"name":"a","command":"echo a","depends_on":["a"]}]}`
	_, err := Load(strings.NewReader(doc))
	var ve *ValidationError
	if !errors.As(err, &ve) || ve.Kind != KindCycle {
		t.Fatalf("want KindCycle, got %v", err)
	}
}

func TestValidateMissingDependencyDetectedAndClassified(t *testing.T) {
	doc := `{
		"name": "broken",
		"steps": [
			{"name": "a", "command": "echo a"},
			{"name": "b", "command": "echo b", "depends_on": ["ghost"]}
		]
	}`
	_, err := Load(strings.NewReader(doc))
	if err == nil {
		t.Fatal("expected missing dependency error, got nil")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) || ve.Kind != KindMissingDependency {
		t.Fatalf("want KindMissingDependency, got %v (type %T)", err, err)
	}
	if !IsMissingDependency(err) || IsCycle(err) {
		t.Fatal("error classification helpers disagree")
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("error should name missing step 'ghost': %v", err)
	}
	t.Logf("missing dependency correctly rejected: %v", err)
}

func TestValidateDuplicateStep(t *testing.T) {
	doc := `{"steps":[
		{"name":"a","command":"echo a"},
		{"name":"a","command":"echo a2"}
	]}`
	_, err := Load(strings.NewReader(doc))
	var ve *ValidationError
	if !errors.As(err, &ve) || ve.Kind != KindDuplicateStep {
		t.Fatalf("want KindDuplicateStep, got %v", err)
	}
}

func TestValidateEmptyAndMalformed(t *testing.T) {
	if _, err := Load(strings.NewReader(`{"steps":[]}`)); err == nil {
		t.Fatal("empty pipeline should be rejected")
	}
	if _, err := Load(strings.NewReader(`{not json`)); err == nil {
		t.Fatal("malformed JSON should be rejected")
	}
	if _, err := Load(strings.NewReader(`{"steps":[{"name":"","command":"x"}]}`)); err == nil {
		t.Fatal("empty step name should be rejected")
	}
}
