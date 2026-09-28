package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/lisama569/bio-pipeline-scheduler-go/pipeline"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "错误:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return fmt.Errorf("缺少子命令")
	}
	switch args[0] {
	case "validate":
		return cmdValidate(args[1:])
	case "run":
		return cmdRun(args[1:])
	case "-h", "--help", "help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("未知子命令 %q", args[0])
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `用法:
  scheduler validate <pipeline.json>
      只校验流程定义（成环 / 依赖缺失 / 重名等），不执行。
  scheduler run <pipeline.json> --state <目录>
      执行流程，或从指定状态目录的断点处继续运行。
      --state 默认 ./state；日志写入 <state>/logs。
`)
}

func cmdValidate(args []string) error {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("validate 需要一个流程描述文件参数")
	}
	def, err := pipeline.LoadFile(fs.Arg(0))
	if err != nil {
		var ve *pipeline.ValidationError
		if errors.As(err, &ve) {
			return err
		}
		return err
	}
	fmt.Printf("流程 %q 校验通过：%d 个步骤，全局并发上限 %d\n",
		def.Name, len(def.Steps), def.MaxRunning)
	return nil
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	stateDir := fs.String("state", "./state", "持久化状态目录")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("run 需要一个流程描述文件参数")
	}

	def, err := pipeline.LoadFile(fs.Arg(0))
	if err != nil {
		return err
	}

	store, err := pipeline.NewStore(*stateDir)
	if err != nil {
		return err
	}
	defer store.Close()

	if _, err := store.Bootstrap(def); err != nil {
		return err
	}

	log := pipeline.NewLogger(os.Stdout)
	exec := pipeline.NewShellExecutor(*stateDir + "/logs")
	sch := pipeline.NewScheduler(def, store, exec, log)

	// Ctrl-C / kill 时取消执行；已落盘的状态可用于下次断点恢复。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rep, err := sch.Run(ctx)
	if err != nil {
		return err
	}

	fmt.Printf("\n运行结束: 状态=%s 成功=%v 失败=%v 跳过=%v\n",
		rep.Status, rep.Succeeded, rep.Failed, rep.Skipped)
	if rep.Status == pipeline.RunFailed {
		return fmt.Errorf("存在失败或被跳过的步骤")
	}
	return nil
}
