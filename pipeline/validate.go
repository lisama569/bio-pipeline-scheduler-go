package pipeline

// Validate 校验流程定义：
//   - 没有名称 / 重名的步骤；
//   - 依赖了不存在的步骤；
//   - 步骤依赖自身；
//   - 依赖关系成环。
//
// 所有问题都会被收集到一个 *ValidationError 中一次性返回，
// 调用方可以通过 DefProblem.Kind 区分具体是哪一种问题。
func (def *PipelineDef) Validate() error {
	var problems []DefProblem

	seen := make(map[string]int) // step name -> 第一次出现的下标
	graph := make(map[string][]string)
	order := make([]string, 0, len(def.Steps))

	for i := range def.Steps {
		name := def.Steps[i].Name
		if name == "" {
			problems = append(problems, DefProblem{Kind: ErrEmptyName, Step: name,
				Detail: "第 " + itoa(i) + " 个步骤缺少 name"})
			continue
		}
		if first, ok := seen[name]; ok {
			problems = append(problems, DefProblem{Kind: ErrDuplicateStep, Step: name,
				Detail: "分别出现在第 " + itoa(first) + " 和第 " + itoa(i) + " 个位置"})
			continue
		}
		seen[name] = i
		order = append(order, name)
	}

	// 依赖引用校验（重复的定义本身已报错，这里只按唯一名称建图）。
	for i := range def.Steps {
		step := def.Steps[i]
		if step.Name == "" {
			continue
		}
		if seen[step.Name] != i {
			// 非首次出现的重名步骤，跳过建图，避免重复边。
			continue
		}
		for _, dep := range step.DependsOn {
			if dep == step.Name {
				problems = append(problems, DefProblem{Kind: ErrSelfDependency, Step: step.Name})
				continue
			}
			if _, ok := seen[dep]; !ok {
				problems = append(problems, DefProblem{Kind: ErrMissingDep, Step: step.Name, Detail: dep})
				continue
			}
			graph[step.Name] = append(graph[step.Name], dep)
		}
	}

	// 成环检测：三色 DFS，命中灰节点即回边，沿 DFS 栈取出环路径。
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(order))
	var stack []string
	cycleReported := make(map[string]bool)

	var dfs func(node string)
	dfs = func(node string) {
		color[node] = gray
		stack = append(stack, node)
		for _, dep := range graph[node] {
			if color[dep] == black {
				continue
			}
			if color[dep] == gray {
				cycle := extractCycle(stack, dep)
				key := cycleKey(cycle)
				if !cycleReported[key] {
					cycleReported[key] = true
					problems = append(problems, DefProblem{Kind: ErrCycle, Step: dep, Detail: formatCycle(cycle)})
				}
				continue
			}
			dfs(dep)
		}
		stack = stack[:len(stack)-1]
		color[node] = black
	}

	for _, name := range order {
		if color[name] == white {
			dfs(name)
		}
	}

	if len(problems) > 0 {
		return &ValidationError{Problems: problems}
	}
	return nil
}

// extractCycle 从 DFS 栈中取出 backTarget 所在的环。
func extractCycle(stack []string, backTarget string) []string {
	start := 0
	for i, n := range stack {
		if n == backTarget {
			start = i
			break
		}
	}
	cycle := append([]string(nil), stack[start:]...)
	cycle = append(cycle, backTarget)
	return cycle
}

func formatCycle(cycle []string) string {
	out := ""
	for i, n := range cycle {
		if i > 0 {
			out += " -> "
		}
		out += n
	}
	return out
}

// cycleKey 归一化环，使同一条环只报告一次（从最小名字处旋转）。
func cycleKey(cycle []string) string {
	if len(cycle) <= 1 {
		return formatCycle(cycle)
	}
	nodes := cycle[:len(cycle)-1] // 末尾与开头重复
	minIdx := 0
	for i := 1; i < len(nodes); i++ {
		if nodes[i] < nodes[minIdx] {
			minIdx = i
		}
	}
	rotated := make([]string, 0, len(nodes)+1)
	for i := 0; i < len(nodes); i++ {
		rotated = append(rotated, nodes[(minIdx+i)%len(nodes)])
	}
	rotated = append(rotated, nodes[minIdx])
	return formatCycle(rotated)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
