package pipeline

import "fmt"

// 定义级错误的细分种类，用于区分「成环」与「依赖缺失」等不同问题。
type DefErrorKind string

const (
	ErrDuplicateStep  DefErrorKind = "duplicate_step"
	ErrMissingDep     DefErrorKind = "missing_dependency"
	ErrSelfDependency DefErrorKind = "self_dependency"
	ErrCycle          DefErrorKind = "cycle"
	ErrEmptyName      DefErrorKind = "empty_step_name"
)

// DefProblem 描述定义中的单个问题。
type DefProblem struct {
	Kind   DefErrorKind
	Step   string
	Detail string
}

func (p DefProblem) message() string {
	switch p.Kind {
	case ErrDuplicateStep:
		return fmt.Sprintf("步骤 %q 重复定义", p.Step)
	case ErrMissingDep:
		return fmt.Sprintf("步骤 %q 依赖了不存在的步骤 %q", p.Step, p.Detail)
	case ErrSelfDependency:
		return fmt.Sprintf("步骤 %q 依赖了自身", p.Step)
	case ErrCycle:
		return fmt.Sprintf("依赖关系成环: %s", p.Detail)
	case ErrEmptyName:
		return "存在没有名称的步骤"
	default:
		return p.Detail
	}
}

// ValidationError 聚合所有定义校验问题，拒绝加载而非跑到一半才崩。
type ValidationError struct {
	Problems []DefProblem
}

func (e *ValidationError) Error() string {
	if e == nil || len(e.Problems) == 0 {
		return "流程定义校验失败"
	}
	msg := "流程定义校验失败:"
	for _, p := range e.Problems {
		msg += "\n  - " + p.message()
	}
	return msg
}

// 执行期错误的种类，用于区分可重试与不可重试失败。
type ErrorKind string

const (
	KindRetryable    ErrorKind = "retryable"
	KindPermanent    ErrorKind = "permanent"
	KindUpstreamFail ErrorKind = "upstream_failed"
)

// StepError 由执行器返回，携带失败是否可重试的信息。
type StepError struct {
	Kind    ErrorKind
	Message string
}

func (e *StepError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("[%s] %s", e.Kind, e.Message)
}

func (e *StepError) Retryable() bool {
	return e != nil && e.Kind == KindRetryable
}

// RollbackError 表示落盘状态被人为回退（例如 succeeded 改回 pending）。
type RollbackError struct {
	Step   string
	Expect StepStatus
	Found  StepStatus
}

func (e *RollbackError) Error() string {
	return fmt.Sprintf("检测到状态回退: 步骤 %q 的落盘状态为 %q，但事件日志显示其已到达 %q；"+
		"已完成的步骤不允许被改回待执行状态", e.Step, e.Found, e.Expect)
}

// StateCorruptionError 表示状态文件损坏或被篡改（哈希链断裂等）。
type StateCorruptionError struct {
	Reason string
}

func (e *StateCorruptionError) Error() string {
	return fmt.Sprintf("持久化状态已损坏: %s", e.Reason)
}

// DefMismatchError 表示状态目录与本次加载的流程定义不一致。
type DefMismatchError struct {
	Reason string
}

func (e *DefMismatchError) Error() string {
	return fmt.Sprintf("流程定义与状态目录不匹配: %s", e.Reason)
}
