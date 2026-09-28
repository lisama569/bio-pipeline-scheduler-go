package pipeline

import "context"

// Scheduler 按依赖顺序调度步骤，执行全局/资源配额并发控制、重试与失败传播。
type Scheduler struct {
	def        *PipelineDef
	store      *Store
	exec       Executor
	log        *Logger
	maxRunning int
	quotas     map[string]int
}

// NewScheduler 创建调度器。
func NewScheduler(def *PipelineDef, store *Store, exec Executor, log *Logger) *Scheduler {
	return &Scheduler{}
}

// Run 执行（或从中断点继续）整个流程，直到全部结束或上下文取消。
func (sch *Scheduler) Run(ctx context.Context) (*RunReport, error) {
	return nil, nil
}
