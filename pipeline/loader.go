package pipeline

import "io"

// Load 从 r 读取 JSON 格式的流程描述并完成校验。
func Load(r io.Reader) (*PipelineDef, error) {
	return nil, nil
}

// LoadFile 从本地文件加载流程描述。
func LoadFile(path string) (*PipelineDef, error) {
	return nil, nil
}
