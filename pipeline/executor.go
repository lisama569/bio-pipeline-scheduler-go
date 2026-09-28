package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
)

// PermanentExitCode：步骤以该退出码退出时，被视为不可重试的永久失败。
// 生信脚本可以用 exit 42 明确表达“配置/数据本身错误，重试无意义”。
const PermanentExitCode = 42

// Executor 执行单个步骤。
type Executor interface {
	Execute(ctx context.Context, def StepDef, attempt int) StepResult
}

// ShellExecutor 在本地 shell 中执行步骤命令，stdout/stderr 落盘并校验声明的产出物。
type ShellExecutor struct {
	logsDir string
}

func NewShellExecutor(logsDir string) *ShellExecutor {
	return &ShellExecutor{logsDir: logsDir}
}

func (e *ShellExecutor) Execute(ctx context.Context, def StepDef, attempt int) StepResult {
	workDir := def.WorkDir
	if workDir == "" {
		workDir = "."
	}

	logPath := ""
	if e.logsDir != "" {
		logPath = filepath.Join(e.logsDir, fmt.Sprintf("%s.attempt%d.log", def.Name, attempt))
		if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
			return StepResult{Err: &StepError{Kind: KindPermanent,
				Message: fmt.Sprintf("创建日志目录失败: %s", err)}}
		}
	}

	cmd := exec.CommandContext(ctx, "sh", "-c", def.Command)
	cmd.Dir = workDir
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env,
		"PIPELINE_STEP="+def.Name,
		"PIPELINE_ATTEMPT="+strconv.Itoa(attempt),
	)

	var combined bytes.Buffer
	cmd.Stdout = &combined
	cmd.Stderr = &combined

	runErr := cmd.Run()
	if logPath != "" {
		if err := os.WriteFile(logPath, append(combined.Bytes(), '\n'), 0o644); err != nil {
			return StepResult{LogPath: logPath, Err: &StepError{Kind: KindPermanent,
				Message: fmt.Sprintf("写步骤日志失败: %s", err)}}
		}
	}
	if runErr != nil {
		return StepResult{LogPath: logPath, Err: classifyExitError(runErr, def, combined.Bytes())}
	}

	// 命令成功后校验声明的产出物，缺失产出视为不可重试的失败（命令本身没报错，重试也不会变出来）。
	var outputs []string
	for _, rel := range def.Outputs {
		full := rel
		if !filepath.IsAbs(full) {
			full = filepath.Join(workDir, rel)
		}
		info, err := os.Stat(full)
		if err != nil {
			return StepResult{LogPath: logPath, Err: &StepError{Kind: KindPermanent,
				Message: fmt.Sprintf("步骤执行成功但缺少声明的产出物 %q: %s", rel, err)}}
		}
		if info.IsDir() {
			return StepResult{LogPath: logPath, Err: &StepError{Kind: KindPermanent,
				Message: fmt.Sprintf("声明的产出物 %q 是目录而非文件", rel)}}
		}
		outputs = append(outputs, rel)
	}
	return StepResult{Outputs: outputs, LogPath: logPath}
}

// classifyExitError 把命令执行错误归类为可重试或不可重试。
func classifyExitError(runErr error, def StepDef, output []byte) *StepError {
	tail := tailLines(output, 20)
	if ee, ok := runErr.(*exec.ExitError); ok {
		switch ee.ExitCode() {
		case PermanentExitCode:
			return &StepError{Kind: KindPermanent,
				Message: fmt.Sprintf("步骤以永久失败退出码 %d 退出（不再重试）。尾部输出:\n%s",
					PermanentExitCode, tail)}
		default:
			return &StepError{Kind: KindRetryable,
				Message: fmt.Sprintf("命令以退出码 %d 失败。尾部输出:\n%s", ee.ExitCode(), tail)}
		}
	}
	// 非退出码错误（例如上下文取消）按可重试处理，由调度层决定是否还会再跑。
	return &StepError{Kind: KindRetryable, Message: runErr.Error()}
}

func tailLines(b []byte, max int) string {
	lines := bytes.Split(bytes.TrimRight(b, "\n"), []byte("\n"))
	if len(lines) > max {
		lines = lines[len(lines)-max:]
	}
	return string(bytes.Join(lines, []byte("\n")))
}

// FuncExecutor 用内存函数模拟步骤执行，主要用于测试。
type FuncExecutor struct {
	Funcs map[string]func(ctx context.Context, attempt int) StepResult
}

func NewFuncExecutor(fn map[string]func(ctx context.Context, attempt int) StepResult) *FuncExecutor {
	return &FuncExecutor{Funcs: fn}
}

func (e *FuncExecutor) Execute(ctx context.Context, def StepDef, attempt int) StepResult {
	fn, ok := e.Funcs[def.Name]
	if !ok {
		// 没注册的测试步骤默认成功，方便只关心部分步骤的用例。
		return StepResult{Outputs: append([]string(nil), def.Outputs...)}
	}
	return fn(ctx, attempt)
}
