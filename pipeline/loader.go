package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// Load 从 r 读取 JSON 格式的流程描述，完成结构校验并填充默认值。
func Load(r io.Reader) (*PipelineDef, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("读取流程描述失败: %w", err)
	}

	var def PipelineDef
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&def); err != nil {
		return nil, fmt.Errorf("流程描述不是合法 JSON: %w", err)
	}
	if dec.More() {
		return nil, fmt.Errorf("流程描述中存在多余的 JSON 值")
	}

	if len(def.Steps) == 0 {
		return nil, fmt.Errorf("流程描述中没有任何步骤")
	}
	if def.MaxRunning < 0 {
		return nil, fmt.Errorf("max_running 不能为负数")
	}
	for label, q := range def.Quotas {
		if q <= 0 {
			return nil, fmt.Errorf("资源标签 %q 的配额必须为正整数，得到 %d", label, q)
		}
	}

	if err := def.Validate(); err != nil {
		return nil, err
	}

	if def.MaxRunning == 0 {
		def.MaxRunning = len(def.Steps)
	}
	return &def, nil
}

// LoadFile 从本地文件加载流程描述。
func LoadFile(path string) (*PipelineDef, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("打开流程描述 %q 失败: %w", path, err)
	}
	defer f.Close()
	def, err := Load(f)
	if err != nil {
		return nil, err
	}
	return def, nil
}

// DefFingerprint 计算定义内容的稳定指纹，用于断点恢复时确认定义没有被偷换。
func DefFingerprint(def *PipelineDef) string {
	// 按定义顺序规范化序列化，使同一份定义始终得到相同指纹。
	b, _ := json.Marshal(struct {
		Name       string         `json:"name"`
		Steps      []StepDef      `json:"steps"`
		MaxRunning int            `json:"max_running"`
		Quotas     map[string]int `json:"quotas"`
	}{def.Name, def.Steps, def.MaxRunning, def.Quotas})
	return shortHash(b)
}
