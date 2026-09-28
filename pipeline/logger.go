package pipeline

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Logger 打印步骤名、重试次数与状态变化；多个 worker 会并发调用，需要加锁。
type Logger struct {
	mu sync.Mutex
	w  io.Writer
}

func NewLogger(w io.Writer) *Logger {
	if w == nil {
		w = os.Stdout
	}
	return &Logger{w: w}
}

func (l *Logger) Logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.w, "%s %s\n", time.Now().Format("15:04:05.000"), fmt.Sprintf(format, args...))
}
