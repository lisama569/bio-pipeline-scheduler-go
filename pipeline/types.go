package pipeline

// 流程描述文件中一个分析步骤的声明。
type StepDef struct {
	Name        string            `json:"name"`
	Command     string            `json:"command"`
	DependsOn   []string          `json:"depends_on,omitempty"`
	Resources   map[string]string `json:"resources,omitempty"`
	Outputs     []string          `json:"outputs,omitempty"`
	MaxRetries  int               `json:"max_retries,omitempty"`
	Retryable   *bool             `json:"retryable,omitempty"`
	WorkDir     string            `json:"workdir,omitempty"`
	Description string            `json:"description,omitempty"`
}

// 流程描述文件。
type PipelineDef struct {
	Name       string         `json:"name"`
	Steps      []StepDef      `json:"steps"`
	MaxRunning int            `json:"max_running,omitempty"`
	Quotas     map[string]int `json:"quotas,omitempty"`
}

// StepStatus 是步骤在其生命周期中的状态。
type StepStatus string

const (
	StatusPending   StepStatus = "pending"
	StatusRunning   StepStatus = "running"
	StatusSucceeded StepStatus = "succeeded"
	StatusFailed    StepStatus = "failed"
	StatusSkipped   StepStatus = "skipped" // 上游失败导致不可达
)

// RunStatus 是整次流程运行的汇总状态。
type RunStatus string

const (
	RunRunning  RunStatus = "running"
	RunComplete RunStatus = "complete"
	RunFailed   RunStatus = "failed"
)

// StepState 是一个步骤需要落盘的全部状态。
type StepState struct {
	Name      string     `json:"name"`
	Status    StepStatus `json:"status"`
	Attempts  int        `json:"attempts"`
	Outputs   []string   `json:"outputs,omitempty"`
	LastError string     `json:"last_error,omitempty"`
	UpdatedAt string     `json:"updated_at"`
}

// Transition 记录一次状态迁移。
type Transition struct {
	From StepStatus `json:"from"`
	To   StepStatus `json:"to"`
}

// Event 是只追加事件日志中的一条记录。
type Event struct {
	Seq        int        `json:"seq"`
	Step       string     `json:"step"`
	Transition Transition `json:"transition"`
	Attempt    int        `json:"attempt,omitempty"`
	Detail     string     `json:"detail,omitempty"`
	At         string     `json:"at"`
	PrevHash   string     `json:"prev_hash"`
	Hash       string     `json:"hash"`
}

// Snapshot 是状态目录中的完整快照。
type Snapshot struct {
	PipelineName string                `json:"pipeline_name"`
	DefHash      string                `json:"def_hash"`
	Status       RunStatus             `json:"status"`
	Steps        map[string]*StepState `json:"steps"`
	LastSeq      int                   `json:"last_seq"`
	UpdatedAt    string                `json:"updated_at"`
}

// StepResult 是执行器返回的结果。
type StepResult struct {
	Outputs []string
	Err     *StepError
}

// RunReport 是一次调度运行的汇总。
type RunReport struct {
	Status    RunStatus
	Succeeded []string
	Failed    []string
	Skipped   []string
}
