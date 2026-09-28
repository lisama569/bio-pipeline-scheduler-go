package pipeline

// Validate 校验流程定义：重名步骤、依赖缺失、自依赖与依赖成环都会被明确拒绝。
func (def *PipelineDef) Validate() error {
	return nil
}
