package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	snapshotFile = "snapshot.json"
	eventsFile   = "events.log"
	lockFile     = "scheduler.lock"
)

// Store 负责运行状态的持久化与断点恢复。
// 状态目录布局：
//
//	snapshot.json  最新完整快照（原子替换写入）
//	events.log     只追加的状态迁移事件日志（哈希链防篡改）
//	scheduler.lock 文件锁，保证同一状态目录同一时刻只有一个调度进程
type Store struct {
	dir       string
	lock      *os.File
	writeMu   sync.Mutex // 串行化事件链追加与快照写入
	lastHashS string     // 已落盘的最后一条事件哈希（内存缓存）
}

// NewStore 打开（必要时创建）状态目录并取得排他文件锁。
func NewStore(stateDir string) (*Store, error) {
	if stateDir == "" {
		return nil, fmt.Errorf("状态目录不能为空")
	}
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return nil, fmt.Errorf("创建状态目录 %q 失败: %w", stateDir, err)
	}
	lock, err := acquireLock(filepath.Join(stateDir, lockFile))
	if err != nil {
		return nil, err
	}
	return &Store{dir: stateDir, lock: lock}, nil
}

// Close 释放文件锁。
func (s *Store) Close() error {
	if s == nil || s.lock == nil {
		return nil
	}
	_ = releaseLock(s.lock)
	err := s.lock.Close()
	s.lock = nil
	return err
}

func nowUTC() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// Bootstrap 在空状态目录中写入初始快照；已有快照则直接复用（幂等）。
func (s *Store) Bootstrap(def *PipelineDef) (*Snapshot, error) {
	if existing, err := s.readSnapshot(); err == nil {
		return existing, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	steps := make(map[string]*StepState, len(def.Steps))
	for i := range def.Steps {
		name := def.Steps[i].Name
		steps[name] = &StepState{
			Name:      name,
			Status:    StatusPending,
			Attempts:  0,
			UpdatedAt: nowUTC(),
		}
	}
	snap := &Snapshot{
		PipelineName: def.Name,
		DefHash:      DefFingerprint(def),
		Status:       RunRunning,
		Steps:        steps,
		LastSeq:      0,
		UpdatedAt:    nowUTC(),
	}
	if err := s.SaveSnapshot(snap); err != nil {
		return nil, err
	}
	return snap, nil
}

// Recover 读取快照并回放事件日志，校验定义一致性、哈希链完整性，
// 并检测任何把状态往回改的人为回退。返回的快照可直接作为继续运行的起点。
func (s *Store) Recover(def *PipelineDef) (*Snapshot, error) {
	snap, err := s.readSnapshot()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return s.Bootstrap(def)
		}
		return nil, err
	}

	if snap.DefHash != DefFingerprint(def) {
		return nil, &DefMismatchError{Reason: fmt.Sprintf(
			"状态目录保存的定义指纹 %s 与当前定义 %s 不一致；"+
				"请使用与之前相同的流程定义，或为新的运行指定新的状态目录",
			snap.DefHash, DefFingerprint(def))}
	}

	// 第一步：校验快照自身，状态只能是定义中声明的步骤且状态值合法。
	if err := validateSnapshotShape(snap, def); err != nil {
		return nil, err
	}

	// 第二步：从初始状态回放事件日志，重建权威状态。
	events, err := s.readEvents()
	if err != nil {
		return nil, err
	}
	authoritative, err := replay(def, events)
	if err != nil {
		return nil, err
	}

	// 第三步：把磁盘快照与事件日志回放结果比对。
	// 由于事件先 fsync、快照后原子替换，崩溃后只可能出现两种局面：
	//   a) 快照领先日志（snap.LastSeq > 日志末尾）—— 不可能由正常崩溃产生，
	//      说明事件日志被截断/删除，判为损坏；
	//   b) 快照落后日志 —— 合法的崩溃窗口，以事件日志回放结果为准续跑。
	// 此外逐步骤比较“进度”：把已完成状态人为改回 pending 这类回退必须拒绝。
	if snap.LastSeq > authoritative.LastSeq {
		return nil, &StateCorruptionError{Reason: fmt.Sprintf(
			"snapshot 的 last_seq=%d 领先于事件日志末尾 seq=%d（事件日志可能被截断或删除）",
			snap.LastSeq, authoritative.LastSeq)}
	}
	lagged := snap.LastSeq < authoritative.LastSeq
	for name, st := range snap.Steps {
		got := authoritative.Steps[name]
		if got == nil {
			return nil, &StateCorruptionError{Reason: "snapshot 含未知步骤 " + name}
		}
		if statusRank(st.Status) > statusRank(got.Status) {
			// 快照声称比事件日志走得更远，但日志里没有对应事件 —— 快照被伪造。
			return nil, &StateCorruptionError{Reason: fmt.Sprintf(
				"步骤 %q 的 snapshot 状态 %s 领先于事件日志可证实的状态 %s",
				name, st.Status, got.Status)}
		}
		if st.Attempts > got.Attempts {
			return nil, &StateCorruptionError{Reason: fmt.Sprintf(
				"步骤 %q 的 snapshot attempts=%d 领先于事件日志可证实的 %d",
				name, st.Attempts, got.Attempts)}
		}
		// 日志已到终态（succeeded/failed/skipped），快照却是更早的非终态，
		// 典型的“人为把已完成改回待执行”。崩溃窗口不会落在这种点上
		//（成功/失败事件与其快照在同一 Commit 里，且终态不可能再被合法改写）。
		if isTerminalStatus(got.Status) && st.Status != got.Status && statusRank(st.Status) < statusRank(got.Status) {
			return nil, &RollbackError{Step: name, Expect: got.Status, Found: st.Status}
		}
		if lagged {
			// 快照落后时，产出物等字段以日志回放为准；snapshot 中的 outputs 可信地
			// 来自上一个已 Commit 的一致点，这里不额外比较，避免误报。
			continue
		}
	}
	if lagged {
		// 产出物只记录在快照里（事件不含 outputs）：对已在上一个一致快照中
		// 成功的步骤，沿用其产出物，避免落后续跑时丢失产出登记。
		for name, st := range snap.Steps {
			if st.Status == StatusSucceeded && authoritative.Steps[name].Status == StatusSucceeded {
				authoritative.Steps[name].Outputs = append([]string(nil), st.Outputs...)
			}
		}
		// 以事件日志为准，把快照补到一致点（原子写回），完成恢复。
		authoritative.Status = runStatusFromSteps(authoritative.Steps)
		if err := s.SaveSnapshot(authoritative); err != nil {
			return nil, err
		}
	}
	authoritative.UpdatedAt = snap.UpdatedAt

	// 预热哈希链缓存，使运行期追加事件不必每次重读日志。
	s.writeMu.Lock()
	if len(events) > 0 {
		s.lastHashS = events[len(events)-1].Hash
	}
	s.writeMu.Unlock()
	return authoritative, nil
}

// statusRank 给状态一个粗略“进度”序，用于识别回退/伪造。
func statusRank(x StepStatus) int {
	switch x {
	case StatusPending:
		return 1
	case StatusRunning:
		return 2
	case StatusSucceeded, StatusFailed, StatusSkipped:
		return 3
	default:
		return 0
	}
}

func isTerminalStatus(x StepStatus) bool {
	return x == StatusSucceeded || x == StatusFailed || x == StatusSkipped
}

func runStatusFromSteps(steps map[string]*StepState) RunStatus {
	if !isTerminal(steps) {
		return RunRunning
	}
	return terminalStatus(steps)
}

// validateSnapshotShape 检查快照覆盖且仅覆盖定义中的步骤，状态值合法。
func validateSnapshotShape(snap *Snapshot, def *PipelineDef) error {
	if snap.Steps == nil {
		return &StateCorruptionError{Reason: "snapshot 缺少 steps 字段"}
	}
	known := map[string]bool{}
	for i := range def.Steps {
		known[def.Steps[i].Name] = true
	}
	for name := range snap.Steps {
		if !known[name] {
			return &DefMismatchError{Reason: "snapshot 含有定义中不存在的步骤 " + name}
		}
	}
	for name := range known {
		if _, ok := snap.Steps[name]; !ok {
			return &DefMismatchError{Reason: "snapshot 缺少定义中声明的步骤 " + name}
		}
	}
	return nil
}

// replay 从空状态按顺序应用全部事件；任何非法迁移（包括回退）都被拒绝。
func replay(def *PipelineDef, events []*Event) (*Snapshot, error) {
	steps := make(map[string]*StepState, len(def.Steps))
	for i := range def.Steps {
		name := def.Steps[i].Name
		steps[name] = &StepState{Name: name, Status: StatusPending}
	}

	var prevHash string
	lastSeq := 0
	for _, ev := range events {
		if ev.Seq != lastSeq+1 {
			return nil, &StateCorruptionError{Reason: fmt.Sprintf(
				"事件序号不连续：期望 %d，实际 %d", lastSeq+1, ev.Seq)}
		}
		if ev.PrevHash != prevHash {
			return nil, &StateCorruptionError{Reason: fmt.Sprintf(
				"事件 #%d 的 prev_hash 与上一条不匹配（事件日志被篡改或截断）", ev.Seq)}
		}
		if got := hashEvent(ev.PrevHash, eventPayload(ev)); got != ev.Hash {
			return nil, &StateCorruptionError{Reason: fmt.Sprintf(
				"事件 #%d 哈希校验失败（事件内容被篡改）", ev.Seq)}
		}

		st, ok := steps[ev.Step]
		if !ok {
			return nil, &DefMismatchError{Reason: "事件引用了不存在的步骤 " + ev.Step}
		}
		if err := applyTransition(st, ev); err != nil {
			return nil, err
		}
		prevHash = ev.Hash
		lastSeq = ev.Seq
	}

	status := RunRunning
	if isTerminal(steps) {
		status = terminalStatus(steps)
	}
	return &Snapshot{
		PipelineName: def.Name,
		DefHash:      DefFingerprint(def),
		Status:       status,
		Steps:        steps,
		LastSeq:      lastSeq,
	}, nil
}

// applyTransition 把单个事件应用到步骤状态，并强制合法的状态迁移方向。
func applyTransition(st *StepState, ev *Event) error {
	if st.Status != ev.Transition.From {
		// 日志说从 X 迁出，但该步骤当前并不在 X：典型的人为回退后重跑。
		return &RollbackError{Step: ev.Step, Expect: st.Status, Found: ev.Transition.From}
	}
	if !legalTransition(ev.Transition.From, ev.Transition.To) {
		return &StateCorruptionError{Reason: fmt.Sprintf(
			"步骤 %q 存在非法状态迁移 %s -> %s", ev.Step, ev.Transition.From, ev.Transition.To)}
	}
	st.Status = ev.Transition.To
	if ev.Attempt > st.Attempts {
		st.Attempts = ev.Attempt
	}
	if ev.Transition.To == StatusSucceeded || ev.Transition.To == StatusFailed {
		st.LastError = ev.Detail
		if ev.Transition.To == StatusSucceeded {
			st.LastError = ""
		}
	}
	st.UpdatedAt = ev.At
	return nil
}

// legalTransition 定义允许的迁移；终态（succeeded/failed/skipped）不可迁出。
func legalTransition(from, to StepStatus) bool {
	switch from {
	case StatusPending:
		return to == StatusRunning || to == StatusSkipped
	case StatusRunning:
		return to == StatusSucceeded || to == StatusFailed || to == StatusPending
	default:
		return false // succeeded / failed / skipped 都是终态
	}
}

// AppendTransition 原子地完成「分配序号 -> 计算哈希链 -> 追加并 fsync」。
// 多个 goroutine 并发调用时由 writeMu 串行化，保证事件序号与哈希链严格一致。
// 返回已落盘的事件，调用方据此更新内存快照。
func (s *Store) AppendTransition(ctx context.Context, seq int, step string, tr Transition, attempt int, detail string) (*Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	ev := &Event{
		Seq:        seq,
		Step:       step,
		Transition: tr,
		Attempt:    attempt,
		Detail:     detail,
		At:         nowUTC(),
		PrevHash:   s.lastHashS,
	}
	ev.Hash = hashEvent(ev.PrevHash, eventPayload(ev))

	line, err := json.Marshal(ev)
	if err != nil {
		return nil, fmt.Errorf("序列化事件失败: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(s.dir, eventsFile), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("打开事件日志失败: %w", err)
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		f.Close()
		return nil, fmt.Errorf("追加事件失败: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, fmt.Errorf("刷新事件日志失败: %w", err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("关闭事件日志失败: %w", err)
	}
	// 仅在确保持久化成功后推进内存缓存。
	s.lastHashS = ev.Hash
	return ev, nil
}

// Commit 原子地完成一次状态迁移的全部持久化：
//
//	在同一把写锁、同一个持久化顺序里
//	「追加事件并 fsync」->「更新快照字段」->「原子替换快照并 fsync 目录」。
//
// 这保证崩溃后快照绝不会领先于事件日志：能在快照里看到 seq=N，
// 事件 1..N 就一定已 durable。mut 用来在落盘前/后就地更新快照字段，
// 返回已落盘事件。调用方不应在 Commit 外另行保存快照。
func (s *Store) Commit(ctx context.Context, snap *Snapshot, step string, tr Transition, attempt int, detail string, mut func(st *StepState, ev *Event)) (*Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	ev := &Event{
		Seq:        snap.LastSeq + 1,
		Step:       step,
		Transition: tr,
		Attempt:    attempt,
		Detail:     detail,
		At:         nowUTC(),
		PrevHash:   s.lastHashS,
	}
	ev.Hash = hashEvent(ev.PrevHash, eventPayload(ev))

	line, err := json.Marshal(ev)
	if err != nil {
		return nil, fmt.Errorf("序列化事件失败: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(s.dir, eventsFile), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("打开事件日志失败: %w", err)
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		f.Close()
		return nil, fmt.Errorf("追加事件失败: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, fmt.Errorf("刷新事件日志失败: %w", err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("关闭事件日志失败: %w", err)
	}
	s.lastHashS = ev.Hash

	// 事件已 durable，再在同一临界区更新并落盘快照，杜绝两者交错。
	snap.LastSeq = ev.Seq
	if mut != nil {
		if st := snap.Steps[step]; st != nil {
			mut(st, ev)
		}
	}
	if err := s.saveSnapshotLocked(snap); err != nil {
		return nil, err
	}
	return ev, nil
}

// SaveSnapshot 原子写入快照（写临时文件 + fsync + rename）。
// 与事件追加共用 writeMu，保证快照与事件链在磁盘上的先后顺序。
func (s *Store) SaveSnapshot(snap *Snapshot) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.saveSnapshotLocked(snap)
}

func (s *Store) saveSnapshotLocked(snap *Snapshot) error {
	snap.UpdatedAt = nowUTC()
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化快照失败: %w", err)
	}
	tmp := filepath.Join(s.dir, snapshotFile+".tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("写临时快照失败: %w", err)
	}
	if err := syncDir(s.dir); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, snapshotFile)); err != nil {
		return fmt.Errorf("替换快照失败: %w", err)
	}
	return syncDir(s.dir)
}

func (s *Store) readSnapshot() (*Snapshot, error) {
	data, err := os.ReadFile(filepath.Join(s.dir, snapshotFile))
	if err != nil {
		return nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, &StateCorruptionError{Reason: "snapshot.json 无法解析: " + err.Error()}
	}
	return &snap, nil
}

func (s *Store) readEvents() ([]*Event, error) {
	data, err := os.ReadFile(filepath.Join(s.dir, eventsFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("读取事件日志失败: %w", err)
	}
	var events []*Event
	for i, raw := range splitLines(data) {
		if len(raw) == 0 {
			continue
		}
		var ev Event
		if err := json.Unmarshal(raw, &ev); err != nil {
			return nil, &StateCorruptionError{Reason: fmt.Sprintf(
				"事件日志第 %d 行无法解析: %s", i+1, err)}
		}
		events = append(events, &ev)
	}
	return events, nil
}

func splitLines(data []byte) [][]byte {
	var out [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			out = append(out, data[start:i])
			start = i + 1
		}
	}
	if start < len(data) {
		out = append(out, data[start:])
	}
	return out
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("打开状态目录做 fsync 失败: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsync 状态目录失败: %w", err)
	}
	return nil
}

// isTerminal 判断所有步骤是否都已到达终态。
func isTerminal(steps map[string]*StepState) bool {
	for _, st := range steps {
		switch st.Status {
		case StatusSucceeded, StatusFailed, StatusSkipped:
		default:
			return false
		}
	}
	return true
}

// terminalStatus 在全部终态的前提下汇总整体状态：有失败步骤则 failed。
func terminalStatus(steps map[string]*StepState) RunStatus {
	for _, st := range steps {
		if st.Status == StatusFailed {
			return RunFailed
		}
	}
	return RunComplete
}

// sortedStepNames 返回步骤名的稳定排序（供日志/报告使用）。
func sortedStepNames(steps map[string]*StepState) []string {
	names := make([]string, 0, len(steps))
	for n := range steps {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
