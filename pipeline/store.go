package pipeline

import "context"

// Store 负责运行状态的持久化与断点恢复。
type Store struct {
	dir string
}

// NewStore 在 stateDir 上打开/初始化一个状态存储。
func NewStore(stateDir string) (*Store, error) {
	return nil, nil
}

// Bootstrap 为空目录写入初始快照。
func (s *Store) Bootstrap(def *PipelineDef) (*Snapshot, error) {
	return nil, nil
}

// Recover 读取快照并回放事件日志，重建可用于断点恢复的状态。
func (s *Store) Recover(def *PipelineDef) (*Snapshot, error) {
	return nil, nil
}

// AppendEvent 追加一条状态迁移事件（哈希链保护，落盘后才返回）。
func (s *Store) AppendEvent(ctx context.Context, ev *Event) error {
	return nil
}

// SaveSnapshot 原子写入快照。
func (s *Store) SaveSnapshot(snap *Snapshot) error {
	return nil
}
