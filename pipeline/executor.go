package pipeline

import "context"

// Executor 执行单个步骤。
type Executor interface {
	Execute(ctx context.Context, def StepDef, attempt int) StepResult
}

// ShellExecutor 在本地 shell 中执行步骤命令。
type ShellExecutor struct{}

func NewShellExecutor() *ShellExecutor { return &ShellExecutor{} }

func (e *ShellExecutor) Execute(ctx context.Context, def StepDef, attempt int) StepResult {
	return StepResult{}
}

// FuncExecutor 用内存函数模拟步骤执行，主要用于测试。
type FuncExecutor struct {
	Funcs map[string]func(ctx context.Context, attempt int) StepResult
}

func NewFuncExecutor(fn map[string]func(ctx context.Context, attempt int) StepResult) *FuncExecutor {
	return &FuncExecutor{Funcs: fn}
}

func (e *FuncExecutor) Execute(ctx context.Context, def StepDef, attempt int) StepResult {
	return StepResult{}
}
