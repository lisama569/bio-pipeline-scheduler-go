package pipeline

import (
	"fmt"
	"os"
	"syscall"
)

// acquireLock 对状态目录加排他文件锁，阻止两个调度进程同时写同一份状态。
func acquireLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("打开锁文件失败: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("状态目录已被另一个调度进程占用（文件锁获取失败）: %w", err)
	}
	return f, nil
}

func releaseLock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
