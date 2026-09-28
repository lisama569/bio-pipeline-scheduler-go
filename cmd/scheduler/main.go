// Command scheduler runs a local bioinformatics pipeline described by a
// JSON file, with bounded concurrency, retries and persistent state.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/lisama569/bio-pipeline-scheduler-go/internal/pipeline"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("scheduler: ")

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "run":
		if err := runCmd(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "scheduler:", err)
			os.Exit(1)
		}
	case "validate":
		if err := validateCmd(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "scheduler:", err)
			os.Exit(1)
		}
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "scheduler: unknown subcommand %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage:
  scheduler run --file pipeline.json [--state-dir .pipeline-state] [--workdir .]
                [--concurrency N] [--quota tag=N,tag2=M]
  scheduler validate --file pipeline.json

Exit codes: 0 success, 1 pipeline failure / invalid spec, 2 usage error.
`)
}

func commonFlags(fs *flag.FlagSet) (*string, *string, *string) {
	file := fs.String("file", "", "path to the pipeline description JSON (required)")
	state := fs.String("state-dir", ".pipeline-state", "directory for persistent state")
	work := fs.String("workdir", ".", "working directory in which step commands run")
	return file, state, work
}

func validateCmd(args []string) error {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	file, _, _ := commonFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return fmt.Errorf("--file is required")
	}
	spec, err := loadSpec(*file)
	if err != nil {
		return err
	}
	log.Printf("pipeline %q is valid: %d step(s), dependency graph is a DAG", spec.Name, len(spec.Steps))
	return nil
}

func runCmd(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	file, stateDir, workdir := commonFlags(fs)
	concurrency := fs.Int("concurrency", 0, "global maximum number of concurrently running steps (0 = unlimited)")
	quotaFlag := fs.String("quota", "", "per-tag slot quotas, e.g. cpu=4,gpu=1")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return fmt.Errorf("--file is required")
	}

	spec, err := loadSpec(*file)
	if err != nil {
		return err
	}

	quotas, err := parseQuotas(*quotaFlag)
	if err != nil {
		return err
	}

	store, err := pipeline.NewStore(*stateDir)
	if err != nil {
		return fmt.Errorf("open state store: %w", err)
	}
	defer store.Close()

	runner := pipeline.NewCommandRunner(*workdir)
	runner.SetLogger(func(format string, a ...any) { log.Printf(format, a...) })

	exec := pipeline.NewExecutor(spec, runner, store, pipeline.Config{
		GlobalConcurrency: *concurrency,
		ResourceQuotas:    quotas,
	})
	exec.SetLogger(func(format string, a ...any) { log.Printf(format, a...) })

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("starting pipeline %q with %d step(s) (global concurrency=%d)", spec.Name, len(spec.Steps), *concurrency)
	if err := exec.Run(ctx); err != nil {
		return err
	}
	log.Printf("pipeline %q finished successfully", spec.Name)
	return nil
}

func loadSpec(path string) (*pipeline.Spec, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open pipeline file: %w", err)
	}
	defer f.Close()
	spec, err := pipeline.Load(f)
	if err != nil {
		return nil, err
	}
	return spec, nil
}

func parseQuotas(s string) (map[string]int, error) {
	out := map[string]int{}
	s = strings.TrimSpace(s)
	if s == "" {
		return out, nil
	}
	for _, pair := range strings.Split(s, ",") {
		kv := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid --quota entry %q, want tag=N", pair)
		}
		n, err := strconv.Atoi(strings.TrimSpace(kv[1]))
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid quota %q: %s must be a positive integer", pair, kv[1])
		}
		out[strings.TrimSpace(kv[0])] = n
	}
	return out, nil
}
