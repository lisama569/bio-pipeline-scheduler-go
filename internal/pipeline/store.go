package pipeline

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// StepState is the persisted state of one step.
type StepState struct {
	Name     string   `json:"name"`
	Status   Status   `json:"status"`
	Attempts int      `json:"attempts"`
	Outputs  []string `json:"outputs,omitempty"`
	Reason   string   `json:"reason,omitempty"`
	Updated  string   `json:"updated,omitempty"`
}

// event is one append-only journal entry. Entries are hash-chained so
// that deletion or modification of history is detected on replay.
type event struct {
	Seq      int      `json:"seq"`
	Step     string   `json:"step"`
	Status   Status   `json:"status"`
	Attempts int      `json:"attempts"`
	Outputs  []string `json:"outputs,omitempty"`
	Reason   string   `json:"reason,omitempty"`
	Time     string   `json:"time"`
	PrevHash string   `json:"prev_hash"`
	Hash     string   `json:"hash"`
}

// payloadHash covers every content field plus the previous hash.
func (ev *event) computeHash() string {
	h := sha256.New()
	fmt.Fprintf(h, "%d|%s|%s|%d|%s|%s|%s",
		ev.Seq, ev.Step, ev.Status, ev.Attempts,
		sortedJoin(ev.Outputs), ev.Reason, ev.PrevHash)
	return hex.EncodeToString(h.Sum(nil))
}

func sortedJoin(xs []string) string {
	cp := append([]string(nil), xs...)
	sort.Strings(cp)
	return fmt.Sprint(cp)
}

// Store persists pipeline state to disk. The append-only journal
// (events.jsonl) is the source of truth; state.json is a compact snapshot
// used for quick inspection and cross-checked against the journal.
type Store struct {
	dir      string
	mu       sync.Mutex
	journal  *os.File
	writer   *bufio.Writer
	seq      int
	prevHash string
	tail     map[string]Status // last status recorded per step
	states   map[string]StepState
}

const (
	journalFile  = "events.jsonl"
	snapshotFile = "state.json"
	manifestFile = "manifest.json"
)

// manifest anchors the tail of the hash chain. Without it, dropping the
// last journal entry would leave a still-valid prefix. The manifest is
// fsynced together with every record.
type manifest struct {
	Entries  int    `json:"entries"`
	TailHash string `json:"tail_hash"`
}

// NewStore opens (or creates) a state store in the given directory and
// replays the journal into memory.
func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create state dir %q: %w", dir, err)
	}
	st := &Store{
		dir:    dir,
		tail:   map[string]Status{},
		states: map[string]StepState{},
	}
	if err := st.replay(); err != nil {
		return nil, err
	}
	if err := st.verifyManifest(); err != nil {
		return nil, err
	}
	if err := st.verifySnapshot(); err != nil {
		return nil, err
	}
	if err := st.openJournal(); err != nil {
		return nil, err
	}
	return st, nil
}

func (st *Store) journalPath() string  { return filepath.Join(st.dir, journalFile) }
func (st *Store) snapshotPath() string { return filepath.Join(st.dir, snapshotFile) }

// replay reads every journal entry, verifies the hash chain, enforces the
// transition rules, and rebuilds the in-memory state table.
func (st *Store) replay() error {
	f, err := os.Open(st.journalPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	prev := ""
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var ev event
		if err := json.Unmarshal(line, &ev); err != nil {
			return fmt.Errorf("state journal line %d: corrupt entry: %w", lineNo, err)
		}
		want := ev.computeHash()
		if ev.Hash != want {
			return fmt.Errorf("state journal line %d: hash mismatch for step %q (journal tampered or truncated)", lineNo, ev.Step)
		}
		if ev.PrevHash != prev {
			return fmt.Errorf("state journal line %d: broken hash chain for step %q (history was modified)", lineNo, ev.Step)
		}
		if err := st.checkTransition(&ev); err != nil {
			return err
		}
		st.apply(&ev, ev.Hash)
		prev = ev.Hash
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("read state journal: %w", err)
	}
	return nil
}

// terminal statuses can never transition to a different status.
func isTerminal(s Status) bool {
	return s == StatusSucceeded || s == StatusFailed || s == StatusUnreachable
}

// checkTransition enforces legal lifecycle movement. It rejects:
//   - moving a succeeded/failed/unreachable step backwards (tampering),
//   - decreasing the attempt counter,
//   - gaps/duplicates in the sequence numbers.
func (st *Store) checkTransition(ev *event) error {
	if ev.Seq != st.seq+1 {
		return fmt.Errorf("state journal: expected seq %d, got %d (journal truncated or replayed twice?)", st.seq+1, ev.Seq)
	}
	if cur, ok := st.tail[ev.Step]; ok {
		if isTerminal(cur) && ev.Status != cur {
			return &RollbackError{
				Step:   ev.Step,
				From:   cur,
				To:     ev.Status,
				Reason: rollbackReason(cur),
			}
		}
		if ev.Attempts < st.states[ev.Step].Attempts {
			return &RollbackError{
				Step:   ev.Step,
				From:   cur,
				To:     ev.Status,
				Reason: fmt.Sprintf("attempt counter went backwards: %d -> %d", st.states[ev.Step].Attempts, ev.Attempts),
			}
		}
	}
	return nil
}

func rollbackReason(s Status) string {
	switch s {
	case StatusSucceeded:
		return "step already succeeded and its outputs are recorded; refusing to re-run it (would duplicate outputs). Fix by restoring the journal or removing the state directory for a fresh run"
	case StatusFailed:
		return "step is in terminal failed state; recorded outcome cannot be reverted to pending"
	case StatusUnreachable:
		return "step was marked unreachable after an upstream failure; terminal state cannot be reverted"
	default:
		return "terminal state cannot be reverted"
	}
}

func (st *Store) apply(ev *event, head string) {
	st.seq = ev.Seq
	st.prevHash = head
	st.tail[ev.Step] = ev.Status
	cur := st.states[ev.Step]
	cur.Name = ev.Step
	cur.Status = ev.Status
	cur.Attempts = ev.Attempts
	cur.Reason = ev.Reason
	cur.Updated = ev.Time
	if len(ev.Outputs) > 0 {
		cur.Outputs = append([]string(nil), ev.Outputs...)
	}
	st.states[ev.Step] = cur
}

func (st *Store) openJournal() error {
	f, err := os.OpenFile(st.journalPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	st.journal = f
	st.writer = bufio.NewWriter(f)
	return nil
}

// Record appends a state transition, flushes and fsyncs it before
// returning. Outputs are taken verbatim (usually verified-existing paths).
func (st *Store) Record(name string, status Status, attempts int, outputs []string, reason string) error {
	st.mu.Lock()
	defer st.mu.Unlock()

	ev := &event{
		Seq:      st.seq + 1,
		Step:     name,
		Status:   status,
		Attempts: attempts,
		Outputs:  append([]string(nil), outputs...),
		Reason:   reason,
		Time:     time.Now().UTC().Format(time.RFC3339Nano),
	}
	ev.PrevHash = st.lastHash()
	ev.Hash = ev.computeHash()

	// Validate before touching disk.
	if err := st.checkTransition(ev); err != nil {
		return err
	}

	line, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	if _, err := st.writer.Write(line); err != nil {
		return err
	}
	if err := st.writer.WriteByte('\n'); err != nil {
		return err
	}
	if err := st.writer.Flush(); err != nil {
		return err
	}
	if err := st.journal.Sync(); err != nil {
		return err
	}
	if err := st.writeManifest(ev.Seq, ev.Hash); err != nil {
		return err
	}
	st.apply(ev, ev.Hash)
	return nil
}

func (st *Store) writeManifest(entries int, tailHash string) error {
	data, err := json.Marshal(manifest{Entries: entries, TailHash: tailHash})
	if err != nil {
		return err
	}
	path := filepath.Join(st.dir, manifestFile)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return st.syncDir()
}

// verifyManifest rejects journal truncation: the manifest anchors the
// number of entries and the hash of the final one.
func (st *Store) verifyManifest() error {
	data, err := os.ReadFile(filepath.Join(st.dir, manifestFile))
	if err != nil {
		if os.IsNotExist(err) {
			// No manifest and no journal => fresh store. A journal without
			// a manifest means it was hand-created: refuse it.
			if st.seq == 0 {
				return nil
			}
			return fmt.Errorf("state journal has %d entries but %s is missing; state was tampered with", st.seq, manifestFile)
		}
		return err
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("state manifest is corrupt: %w", err)
	}
	if m.Entries != st.seq {
		return fmt.Errorf("state journal truncated: manifest expects %d entries, found %d", m.Entries, st.seq)
	}
	if m.Entries > 0 && m.TailHash != st.prevHash {
		return fmt.Errorf("state journal tail mismatch: manifest disagrees with last entry (state was tampered with)")
	}
	return nil
}

// lastHash returns the hash of the latest journal entry, or "".
func (st *Store) lastHash() string {
	return st.prevHash
}

// Snapshot writes a compact snapshot atomically and fsyncs it.
func (st *Store) Snapshot() error {
	st.mu.Lock()
	defer st.mu.Unlock()

	states := st.snapshotLocked()
	data, err := json.MarshalIndent(states, "", "  ")
	if err != nil {
		return err
	}
	tmp := st.snapshotPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, st.snapshotPath()); err != nil {
		return err
	}
	return st.syncDir()
}

func (st *Store) snapshotLocked() []StepState {
	out := make([]StepState, 0, len(st.states))
	for _, s := range st.states {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (st *Store) syncDir() error {
	d, err := os.Open(st.dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	_ = d.Close()
	return err
}

// verifySnapshot cross-checks state.json against the journal-derived
// truth. The snapshot is only a cache; disagreeing with the journal means
// one of the two was edited by hand.
func (st *Store) verifySnapshot() error {
	data, err := os.ReadFile(st.snapshotPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var snap []StepState
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("state snapshot %q is corrupt: %w", snapshotFile, err)
	}
	if len(snap) != len(st.states) {
		return fmt.Errorf("state snapshot lists %d steps but journal has %d; state files were tampered with", len(snap), len(st.states))
	}
	for _, ss := range snap {
		cur, ok := st.states[ss.Name]
		if !ok {
			return &RollbackError{Step: ss.Name, From: "unknown", To: ss.Status, Reason: "snapshot contains step absent from journal"}
		}
		if ss.Status != cur.Status || ss.Attempts != cur.Attempts || !sameSet(ss.Outputs, cur.Outputs) {
			return &RollbackError{
				Step: ss.Name,
				From: cur.Status,
				To:   ss.Status,
				Reason: fmt.Sprintf("snapshot disagrees with journal (snap: status=%s attempts=%d; journal: status=%s attempts=%d); a completed step cannot be reset by editing state.json",
					ss.Status, ss.Attempts, cur.Status, cur.Attempts),
			}
		}
	}
	return nil
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	x := append([]string(nil), a...)
	y := append([]string(nil), b...)
	sort.Strings(x)
	sort.Strings(y)
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

// Load returns a copy of the current states keyed by step name.
// A step left in "running" by a crashed process is reported as pending:
// no live process can be running it when the store is opened fresh.
func (st *Store) Load() map[string]StepState {
	st.mu.Lock()
	defer st.mu.Unlock()
	out := make(map[string]StepState, len(st.states))
	for k, v := range st.states {
		if v.Status == StatusRunning || v.Status == StatusReady {
			v.Status = StatusPending
		}
		out[k] = v
	}
	return out
}

// Close flushes and closes the journal and writes a final snapshot.
func (st *Store) Close() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if err := st.writer.Flush(); err != nil {
		return err
	}
	return st.journal.Close()
}
