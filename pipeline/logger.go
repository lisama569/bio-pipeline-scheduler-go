package pipeline

import (
	"fmt"
	"io"
	"os"
	"time"
)

// Logger 打印步骤名、重试次数与状态变化。
type Logger struct {
	w io.Writer
}

func NewLogger(w io.Writer) *Logger {
	if w == nil {
		w = os.Stdout
	}
	return &Logger{w: w}
}

func (l *Logger) Logf(format string, args ...any) {
	fmt.Fprintf(l.w, "%s %s\n", time.Now().Format("15:04:05.000"), fmt.Sprintf(format, args...))
}
