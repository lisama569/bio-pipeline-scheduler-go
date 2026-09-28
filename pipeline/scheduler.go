package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// 默认的可重试失败退避（小步固定，测试里可覆盖）。
var defaultBackoff = 200 * time.Millisecond

// Scheduler 按依赖顺序调度步骤：
//   - 全局并发上限 + 资源标签配额，超限时排队等待而非突破上限；
//   - 每个事件先落盘事件日志再更新快照，支持断点恢复；
//   - 步骤失败耗尽重试后，全部下游标记为 skipped（不可达）；
//   - 不可重试失败不会进入重试。
type Scheduler struct {
	def        *PipelineDef
	store      *Store
	exec       Executor
	log        *Logger
	maxRunning int
	quotas     map[string]int
	backoff    time.Duration

	mu   sync.Mutex
	snap *Snapshot
	// inflight 记录正在执行的步骤，用于观测与测试断言。
	inflight map[string]bool
	// peakRunning / maxObservedRunning 用于并发上限测试。
	running     int
	peakRunning int
	// violations 在观测到突破上限时累加（正常应恒为 0）。
	violations int
	// 每个资源标签当前占用数与峰值。
	resUsed map[string]int
	resPeak map[string]int
}

// NewScheduler 创建调度器。
func NewScheduler(def *PipelineDef, store *Store, exec Executor, log *Logger) *Scheduler {
	if log == nil {
		log = NewLogger(nil)
	}
	return &Scheduler{
		def:        def,
		store:      store,
		exec:       exec,
		log:        log,
		maxRunning: def.MaxRunning,
		quotas:     def.Quotas,
		backoff:    defaultBackoff,
		inflight:   map[string]bool{},
		resUsed:    map[string]int{},
		resPeak:    map[string]int{},
	}
}

// SetBackoff 覆盖重试退避时长（主要用于测试加速）。
func (sch *Scheduler) SetBackoff(d time.Duration) { sch.backoff = d }

// PeakRunning 返回截至当前同时执行步骤数的历史峰值。
func (sch *Scheduler) PeakRunning() int {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	return sch.peakRunning
}

// ConcurrencyViolations 返回观测到的并发超限次数（始终应为 0）。
func (sch *Scheduler) ConcurrencyViolations() int {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	return sch.violations
}

// PeakResource 返回某资源标签占用数的历史峰值。
func (sch *Scheduler) PeakResource(label string) int {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	return sch.resPeak[label]
}

// Run 执行（或从中断点继续）整个流程。
func (sch *Scheduler) Run(ctx context.Context) (*RunReport, error) {
	// 1. 从状态目录恢复：已完成的步骤保持终态，stale running 回到 pending 重跑。
	snap, err := sch.store.Recover(sch.def)
	if err != nil {
		return nil, err
	}
	sch.snap, err = sch.reconcile(ctx, snap)
	if err != nil {
		return nil, err
	}

	// 2. 如果恢复出来已经全部终态，直接出报告（幂等重跑，不会重复产出）。
	if isTerminal(sch.snap.Steps) {
		sch.finishSnapshot()
		return sch.report(), nil
	}

	// 3. 基于当前状态做一次失败传播（例如上次进程在标记 skipped 前被杀死）。
	sch.propagateFailures(ctx)

	// 4. 协调器主循环。
	ready := make(chan string, len(sch.def.Steps))
	// results 必须有缓冲：worker 释放资源后要能立即发完通知，
	// 否则协调器正在别处阻塞时会和 worker 互相等待形成死锁。
	results := make(chan string, len(sch.def.Steps))
	finished := make(chan struct{})
	finishedClosed := false
	closeFinished := func() {
		if !finishedClosed {
			finishedClosed = true
			close(finished)
		}
	}

	// 全局信号量（缓冲通道），容量即全局并发上限。
	globalSem := make(chan struct{}, sch.maxRunning)

	// 每个资源标签一个配额信号量。
	resSem := map[string]chan struct{}{}
	for label, q := range sch.quotas {
		resSem[label] = make(chan struct{}, q)
	}

	defs := map[string]StepDef{}
	for i := range sch.def.Steps {
		defs[sch.def.Steps[i].Name] = sch.def.Steps[i]
	}

	var wg sync.WaitGroup

	// queued 记录已经塞进 ready 通道但尚未被取走的步骤，避免重复入队。
	queued := map[string]bool{}
	// enqueue 只负责“若不在队列则入队”，非阻塞（通道容量 == 步骤数，必能放下）。
	enqueue := func(name string) {
		if queued[name] {
			return
		}
		queued[name] = true
		ready <- name
	}
	enqueueReady := func() {
		for _, d := range sch.def.Steps {
			if queued[d.Name] {
				continue
			}
			if sch.isReady(d.Name) {
				enqueue(d.Name)
			}
		}
	}

	// drainResults 非阻塞处理所有已完成 worker 的通知。
	drainResults := func() {
		for {
			select {
			case <-results:
				sch.propagateFailures(ctx)
				enqueueReady()
			default:
				return
			}
		}
	}

	// 主循环在 goroutine 中运行，Run 等待它结束。
	loopErr := make(chan error, 1)
	go func() {
		defer closeFinished()
		enqueueReady()
		for {
			// 没有任何待处理/执行中的步骤时退出。
			sch.mu.Lock()
			pendingCount := 0
			for _, st := range sch.snap.Steps {
				if st.Status == StatusPending {
					pendingCount++
				}
			}
			running := sch.running
			sch.mu.Unlock()
			if pendingCount == 0 && running == 0 {
				return
			}

			select {
			case <-ctx.Done():
				loopErr <- ctx.Err()
				return
			case name := <-ready:
				queued[name] = false
				// ready 是“可能可运行”的通知，需要重新确认状态与容量。
				if !sch.isReady(name) {
					drainResults()
					enqueueReady()
					continue
				}
				d := defs[name]

				// 获取全部资源：全局信号量 + 该步骤声明的每个标签 1 个配额。
				if !tryAcquire(globalSem) {
					// 全局容量满：把名字放回 ready 通道，等 worker 释放后再试。
					enqueue(name)
					select {
					case <-ctx.Done():
						loopErr <- ctx.Err()
						return
					case <-results:
						sch.propagateFailures(ctx)
						enqueueReady()
					}
					continue
				}
				acquired := []chan struct{}{globalSem}
				blockedLabel := ""
				for label := range d.Resources {
					sem, ok := resSem[label]
					if !ok {
						// def 里声明了标签但 quotas 未配置：默认配额 1，避免静默无限并发。
						sem = make(chan struct{}, 1)
						resSem[label] = sem
					}
					if !tryAcquire(sem) {
						blockedLabel = label
						break
					}
					acquired = append(acquired, sem)
				}
				if blockedLabel != "" {
					for _, sem := range acquired {
						<-sem
					}
					enqueue(name)
					sch.log.Logf("[排队] 步骤=%s 等待资源标签=%s 释放配额", name, blockedLabel)
					select {
					case <-ctx.Done():
						loopErr <- ctx.Err()
						return
					case <-results:
						sch.propagateFailures(ctx)
						enqueueReady()
					}
					continue
				}

				// 同步落盘 running；若失败则释放已获取的信号量后中止。
				attempt := sch.nextAttempt(name)
				if err := sch.transition(ctx, name, StatusPending, StatusRunning, attempt, ""); err != nil {
					for _, sem := range acquired {
						<-sem
					}
					loopErr <- err
					return
				}
				sch.markStarted(name, d.Resources)
				wg.Add(1)
				go func(n string, attempt int, sems []chan struct{}, res map[string]string) {
					defer wg.Done()
					sch.runStep(ctx, n, attempt)
					for _, sem := range sems {
						<-sem
					}
					results <- n
				}(name, attempt, acquired, d.Resources)
				drainResults()

			case <-results:
				// 有步骤释放了容量：级联失败传播 + 重新入队等待者。
				sch.propagateFailures(ctx)
				enqueueReady()
			}
		}
	}()

	select {
	case <-finished:
	case err := <-loopErr:
		if err != nil {
			// 等待已启动的 worker 退出，避免泄漏（worker 会响应 ctx 取消）。
			wg.Wait()
			return nil, err
		}
	}
	wg.Wait()
	close(results)

	if err := sch.finishSnapshot(); err != nil {
		return nil, err
	}
	return sch.report(), nil
}

// reconcile 处理恢复出来的状态：running 是上次进程被杀留下的 stale 状态，
// 转回 pending 重跑（attempts 保留，不重复获得重试预算）。
func (sch *Scheduler) reconcile(ctx context.Context, snap *Snapshot) (*Snapshot, error) {
	for _, st := range snap.Steps {
		if st.Status != StatusRunning {
			continue
		}
		sch.log.Logf("[恢复] 步骤 %s 上次运行中断在 running（已用尝试 %d 次），回到待执行重新执行",
			st.Name, st.Attempts)
		_, err := sch.store.Commit(ctx, snap, st.Name,
			Transition{StatusRunning, StatusPending}, st.Attempts, "进程中断，恢复为待执行",
			func(x *StepState, ev *Event) {
				x.Status = StatusPending
				x.UpdatedAt = ev.At
			})
		if err != nil {
			return nil, fmt.Errorf("恢复 stale running 步骤 %s 失败: %w", st.Name, err)
		}
	}
	return snap, nil
}

// propagateFailures 把 failed 步骤尚未处理的全部下游（含传递依赖）标记为 skipped。
func (sch *Scheduler) propagateFailures(ctx context.Context) {
	// 建立 步骤 -> 直接下游 的反向图。
	dependents := map[string][]string{}
	for _, d := range sch.def.Steps {
		for _, dep := range d.DependsOn {
			dependents[dep] = append(dependents[dep], d.Name)
		}
	}

	sch.mu.Lock()
	failed := []string{}
	for _, st := range sch.snap.Steps {
		if st.Status == StatusFailed {
			failed = append(failed, st.Name)
		}
	}
	sch.mu.Unlock()

	skip := map[string]bool{}
	var visit func(node string)
	visit = func(node string) {
		for _, dn := range dependents[node] {
			if skip[dn] {
				continue
			}
			skip[dn] = true
			visit(dn)
		}
	}
	for _, f := range failed {
		visit(f)
	}

	for name := range skip {
		sch.mu.Lock()
		st := sch.snap.Steps[name]
		cur := st.Status
		sch.mu.Unlock()
		if cur != StatusPending {
			continue // running / 已终态的不动
		}
		if err := sch.transition(ctx, name, StatusPending, StatusSkipped, 0,
			"上游步骤失败，按不可达处理"); err != nil {
			sch.log.Logf("[传播] 标记 %s 为 skipped 失败: %v", name, err)
		}
	}
}

// runStep 在 worker goroutine 中执行单个步骤，含同进程内重试。
func (sch *Scheduler) runStep(ctx context.Context, name string, attempt int) {
	d := sch.stepDef(name)
	sch.log.Logf("[执行] 步骤=%s 尝试次数=%d 状态变化=pending -> running", name, attempt)

	res := sch.exec.Execute(ctx, d, attempt)
	if res.Err == nil {
		if err := sch.succeed(ctx, name, attempt, res.Outputs); err != nil {
			sch.log.Logf("[错误] 步骤 %s 落盘成功事件失败: %v", name, err)
		}
		sch.log.Logf("[完成] 步骤=%s 尝试次数=%d 状态变化=running -> succeeded 产出=%v",
			name, attempt, res.Outputs)
		sch.markFinished(name, d.Resources)
		return
	}

	sch.log.Logf("[失败] 步骤=%s 尝试次数=%d 可重试=%v 原因=%s",
		name, attempt, res.Err.Retryable(), firstLine(res.Err.Message))

	retryable := res.Err.Retryable()
	if explicit := d.Retryable; explicit != nil {
		retryable = *explicit
	}
	if retryable && attempt <= d.MaxRetries {
		// running -> pending，等待协调器重新派发，退避后生效。
		if err := sch.transition(ctx, name, StatusRunning, StatusPending, attempt,
			"可重试失败: "+firstLine(res.Err.Message)); err != nil {
			sch.log.Logf("[错误] 步骤 %s 落盘重试事件失败: %v", name, err)
		}
		sch.setLastError(name, res.Err.Message)
		sch.log.Logf("[重试] 步骤=%s 状态变化=running -> pending，%s 后重试（还剩 %d 次）",
			name, sch.backoff, d.MaxRetries-attempt+1)
		sch.markFinished(name, d.Resources)
		select {
		case <-time.After(sch.backoff):
		case <-ctx.Done():
		}
		return
	}

	// 不可重试，或重试预算耗尽：终态 failed。
	reason := res.Err.Message
	if res.Err.Retryable() && attempt > d.MaxRetries {
		reason = "重试次数已耗尽（" + itoa(attempt-1) + " 次重试），最后错误: " + firstLine(res.Err.Message)
	}
	if err := sch.transition(ctx, name, StatusRunning, StatusFailed, attempt, reason); err != nil {
		sch.log.Logf("[错误] 步骤 %s 落盘失败事件失败: %v", name, err)
	}
	sch.setLastError(name, reason)
	sch.log.Logf("[终态] 步骤=%s 尝试次数=%d 状态变化=running -> failed 原因=%s",
		name, attempt, firstLine(reason))
	sch.markFinished(name, d.Resources)
}

// nextAttempt 返回步骤下一次执行的尝试序号（从 1 开始，恢复后沿用历史计数）。
func (sch *Scheduler) nextAttempt(name string) int {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	st := sch.snap.Steps[name]
	if st.Attempts == 0 {
		return 1
	}
	return st.Attempts + 1
}

// isReady 判断步骤当前是否可被派发：pending 且所有直接依赖已 succeeded。
func (sch *Scheduler) isReady(name string) bool {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	st := sch.snap.Steps[name]
	if st == nil || st.Status != StatusPending {
		return false
	}
	d := sch.stepDefLocked(name)
	for _, dep := range d.DependsOn {
		if sch.snap.Steps[dep].Status != StatusSucceeded {
			return false
		}
	}
	return true
}

// transition 是普通状态变更的唯一入口：在调度器锁内调用 Store.Commit，
// 事件追加与快照写入在同一持久化临界区完成，杜绝两者交错导致的状态损坏。
func (sch *Scheduler) transition(ctx context.Context, name string, from, to StepStatus, attempt int, detail string) error {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	st := sch.snap.Steps[name]
	if st.Status != from {
		return &RollbackError{Step: name, Expect: from, Found: st.Status}
	}
	_, err := sch.store.Commit(ctx, sch.snap, name, Transition{from, to}, attempt, detail,
		func(x *StepState, ev *Event) {
			x.Status = to
			if attempt > x.Attempts {
				x.Attempts = attempt
			}
			x.UpdatedAt = ev.At
			switch to {
			case StatusFailed:
				x.LastError = detail
			case StatusSucceeded:
				x.LastError = ""
			}
		})
	return err
}

// succeed 在一次原子落盘里记录成功事件、状态与产出物。
func (sch *Scheduler) succeed(ctx context.Context, name string, attempt int, outputs []string) error {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	st := sch.snap.Steps[name]
	if st.Status != StatusRunning {
		return &RollbackError{Step: name, Expect: StatusRunning, Found: st.Status}
	}
	_, err := sch.store.Commit(ctx, sch.snap, name,
		Transition{StatusRunning, StatusSucceeded}, attempt, "",
		func(x *StepState, ev *Event) {
			x.Status = StatusSucceeded
			if attempt > x.Attempts {
				x.Attempts = attempt
			}
			x.Outputs = append([]string(nil), outputs...)
			x.LastError = ""
			x.UpdatedAt = ev.At
		})
	return err
}

func (sch *Scheduler) setLastError(name, msg string) {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	sch.snap.Steps[name].LastError = msg
}

func (sch *Scheduler) markStarted(name string, res map[string]string) {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	sch.inflight[name] = true
	sch.running++
	if sch.running > sch.maxRunning {
		sch.violations++
	}
	if sch.running > sch.peakRunning {
		sch.peakRunning = sch.running
	}
	for label := range res {
		sch.resUsed[label]++
		limit := sch.quotas[label]
		if limit == 0 {
			limit = 1 // 与执行侧一致：未配置配额的标签默认容量 1
		}
		if sch.resUsed[label] > limit {
			sch.violations++
		}
		if sch.resUsed[label] > sch.resPeak[label] {
			sch.resPeak[label] = sch.resUsed[label]
		}
	}
}

func (sch *Scheduler) markFinished(name string, res map[string]string) {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	if !sch.inflight[name] {
		return
	}
	delete(sch.inflight, name)
	sch.running--
	for label := range res {
		sch.resUsed[label]--
	}
}

func (sch *Scheduler) stepDef(name string) StepDef {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	return sch.stepDefLocked(name)
}

func (sch *Scheduler) stepDefLocked(name string) StepDef {
	for i := range sch.def.Steps {
		if sch.def.Steps[i].Name == name {
			return sch.def.Steps[i]
		}
	}
	return StepDef{Name: name}
}

func (sch *Scheduler) finishSnapshot() error {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	if isTerminal(sch.snap.Steps) {
		sch.snap.Status = terminalStatus(sch.snap.Steps)
	}
	return sch.store.SaveSnapshot(sch.snap)
}

func (sch *Scheduler) report() *RunReport {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	r := &RunReport{Status: sch.snap.Status}
	for _, name := range sortedStepNames(sch.snap.Steps) {
		switch sch.snap.Steps[name].Status {
		case StatusSucceeded:
			r.Succeeded = append(r.Succeeded, name)
		case StatusFailed:
			r.Failed = append(r.Failed, name)
		case StatusSkipped:
			r.Skipped = append(r.Skipped, name)
		}
	}
	return r
}

// tryAcquire 非阻塞获取一个信号量占位。
func tryAcquire(sem chan struct{}) bool {
	select {
	case sem <- struct{}{}:
		return true
	default:
		return false
	}
}

func firstLine(s string) string {
	for i, c := range s {
		if c == '\n' {
			return s[:i]
		}
	}
	return s
}
