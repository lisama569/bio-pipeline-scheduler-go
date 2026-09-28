package pipeline

import (
	"strings"
	"testing"
)

func TestLoadOK(t *testing.T) {
	src := `{
      "name": "demo",
      "max_running": 2,
      "steps": [
        {"name": "a", "command": "echo a"},
        {"name": "b", "command": "echo b", "depends_on": ["a"]}
      ]
    }`
	def, err := Load(strings.NewReader(src))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if def.MaxRunning != 2 || len(def.Steps) != 2 {
		t.Fatalf("unexpected def: %+v", def)
	}
}

func TestLoadDefaultsMaxRunning(t *testing.T) {
	def, err := Load(strings.NewReader(`{"steps":[{"name":"a"},{"name":"b"},{"name":"c"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if def.MaxRunning != 3 {
		t.Fatalf("default max_running want 3, got %d", def.MaxRunning)
	}
}

func TestLoadRejectsUnknownField(t *testing.T) {
	_, err := Load(strings.NewReader(`{"steps":[{"name":"a"}],"bogus":1}`))
	if err == nil {
		t.Fatal("expected unknown field error")
	}
}

func TestLoadRejectsCycle(t *testing.T) {
	src := `{"steps":[
      {"name":"a","depends_on":["b"]},
      {"name":"b","depends_on":["a"]}
    ]}`
	_, err := Load(strings.NewReader(src))
	if err == nil || !strings.Contains(err.Error(), "成环") {
		t.Fatalf("expected cycle rejection, got %v", err)
	}
}

func TestFingerprintStable(t *testing.T) {
	src := `{"name":"x","steps":[{"name":"a","command":"echo"}]}`
	d1, _ := Load(strings.NewReader(src))
	d2, _ := Load(strings.NewReader(src))
	if DefFingerprint(d1) != DefFingerprint(d2) {
		t.Fatal("fingerprint should be stable for same definition")
	}
	d3, _ := Load(strings.NewReader(`{"name":"x","steps":[{"name":"a","command":"other"}]}`))
	if DefFingerprint(d1) == DefFingerprint(d3) {
		t.Fatal("different definitions should differ")
	}
}
