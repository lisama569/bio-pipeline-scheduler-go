package pipeline

import (
	"io"
	"testing"
)

// testLogger 返回一个把步骤名、尝试次数、状态变化写到 t.Log 的 Logger，
// 仅在 `go test -v` 时可见，满足“测试日志打印步骤名/重试次数/状态变化”的要求。
func testLogger(t *testing.T) *Logger {
	t.Helper()
	return NewLogger(io.Writer(testWriter{t: t}))
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", trimNewline(p))
	return len(p), nil
}

func trimNewline(p []byte) string {
	s := string(p)
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}
