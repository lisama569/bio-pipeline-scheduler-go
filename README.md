# 生信流程调度引擎（bio-pipeline-scheduler）

一个把本地生信分析步骤串成**可反复运行**流程的轻量引擎。重点不是“跑通一遍”，而是三件事在各种异常下仍然靠谱：

- **断点重跑**：每个步骤的状态、尝试次数、产出物都落盘；进程中断后续跑，不重复产出、不漏跑；已完成的步骤不会从头重跑，人为把状态改回去会被明确拒绝。
- **并发受限**：全局并发上限与按资源标签的配额真正生效，超额步骤排队等待，全程（而不只是最终结果）都不会突破上限。
- **失败传播**：步骤失败后其全部下游按“不可达”跳过；失败区分可重试与不可重试，不可重试的失败不会被反复重试。

## 安装与构建

```bash
go build -o scheduler ./cmd/scheduler
```

仅依赖 Go 标准库（Go 1.25+），无第三方依赖。

## 快速开始

```bash
# 只校验定义，不执行
./scheduler validate examples/pipeline.json

# 执行（首次运行会在 --state 目录初始化状态）
cd examples/demo-workspace
../../scheduler run --state ./state ../pipeline.json

# 中断后再次执行同一条命令：已完成的步骤不会重跑，从断点继续
../../scheduler run --state ./state ../pipeline.json
```

每个步骤的 stdout/stderr 会写到 `<state>/logs/<步骤名>.attempt<N>.log`。

## 流程描述格式

流程描述是一个 JSON 文件，未知字段会被拒绝（防止拼错配置被静默忽略）：

```json
{
  "name": "demo-bio-pipeline",
  "max_running": 3,
  "quotas": { "cpu": 2, "gpu": 1 },
  "steps": [
    {
      "name": "qc",
      "command": "fastqc raw.fq",
      "depends_on": ["download"],
      "resources": { "cpu": "1" },
      "outputs": ["clean.fq", "qc.report"],
      "max_retries": 2,
      "retryable": true,
      "workdir": ".",
      "description": "质量控制"
    }
  ]
}
```

### 顶层字段

| 字段 | 含义 |
| --- | --- |
| `name` | 流程名，仅用于展示 |
| `max_running` | 全局同时执行步骤数上限；省略时默认为步骤总数 |
| `quotas` | 按资源标签的配额，例如 `{"cpu": 2}`；步骤通过 `resources` 申请标签 |
| `steps` | 步骤数组，按依赖（而非书写顺序）调度 |

### 步骤字段

| 字段 | 含义 |
| --- | --- |
| `name` | 步骤名，**全流程唯一且非空** |
| `command` | 用 `sh -c` 执行的本地命令；可通过环境变量 `PIPELINE_STEP`、`PIPELINE_ATTEMPT` 拿到步骤名与当前尝试序号 |
| `depends_on` | 依赖的其他步骤名；所有依赖成功后本步骤才可运行，无依赖关系的步骤可并行 |
| `resources` | 申请的资源标签到数值的映射；步骤运行时对每个标签占用 1 个配额 |
| `outputs` | 声明的产出物（相对 `workdir` 的文件路径）；命令成功后会逐一校验文件存在，缺失视为**不可重试**失败 |
| `max_retries` | 可重试失败时的**额外重试次数**；`0`（默认）表示只跑一次，`2` 表示最多共尝试 3 次 |
| `retryable` | 显式覆盖失败的可重试性；设为 `false` 时即使命令以普通非零码退出也不重试 |
| `workdir` | 命令工作目录，默认继承当前目录 |

### 定义期会被明确拒绝的问题

校验在加载阶段一次性报出所有问题，问题类型可区分，不会悄悄跳过或跑到一半才崩：

- `empty_step_name`：存在没有名称的步骤；
- `duplicate_step`：步骤名重复；
- `missing_dependency`：`depends_on` 引用了不存在的步骤（错误信息给出引用方与被引用名）；
- `self_dependency`：步骤依赖自身；
- `cycle`：依赖关系成环（错误信息给出环路径，如 `a -> b -> a`）。

## 步骤状态含义

| 状态 | 含义 | 是否终态 |
| --- | --- | --- |
| `pending` | 待执行（含重试等待、以及崩溃后恢复回来的步骤） | 否 |
| `running` | 正在执行 | 否 |
| `succeeded` | 成功，产出物已登记 | 是 |
| `failed` | 失败且重试预算已耗尽，或命中不可重试失败 | 是 |
| `skipped` | 上游失败导致本步骤不可达，不会执行 | 是 |

合法迁移只有：`pending→running`、`running→succeeded`、`running→failed`、`running→pending`（可重试失败，等待下一轮）、`pending→skipped`（失败传播）。**三个终态不可迁出。**

整体运行状态 `snapshot.status`：运行中为 `running`；全部到达终态且无 failed 为 `complete`；存在 failed 步骤为 `failed`（skipped 不改变“有失败”这一结论）。

## 重试规则

- 命令以普通非零退出码结束 → **可重试**错误，受 `max_retries` 控制；
- 命令以退出码 **42** 结束 → **不可重试（永久）失败**，用于脚本明确表达“配置/数据错误，重试无意义”；
- 命令成功但声明的 `outputs` 有缺失 → **不可重试**失败；
- `retryable: false` 显式关闭重试；
- 每次重试有退避（默认 200ms）；尝试序号从 1 开始，记录在状态与日志里。

不可重试失败与“预算耗尽”都会落到 `failed` 终态，错误原因（含命令尾部输出）写入快照，方便事后定位。

## 并发与资源限制规则

- `max_running` 是硬性全局上限，用容量信号量实现，超额步骤在队列中等待；
- `quotas` 给出每个资源标签的容量，步骤声明 `resources` 即占用对应配额，多标签步骤必须**同时**拿到全部配额才启动，避免占住一部分资源死等；
- 步骤引用了未在 `quotas` 配置的标签时，该标签默认容量为 **1**（宁可保守排队，也不静默放开并发）；
- 全局上限与标签配额同时生效，取两者的交集约束。

## 失败传播规则

某个步骤进入 `failed` 后，它的**所有直接与传递下游**都会被标记为 `skipped`（按未执行/不可达处理，不会去执行）。与失败链无关的独立步骤不受影响，照常运行。传播在每次有步骤结束时以及断点恢复时都会重新计算，因此即使进程在“标记 skipped”中途被杀，重启后也会补齐。

## 持久化与断点恢复

状态目录（`--state`）包含：

```
snapshot.json    最新完整快照（临时文件 + fsync + rename 原子替换）
events.log       只追加的状态迁移事件日志，每条含前一条哈希，构成哈希链
scheduler.lock   文件锁，保证同一状态目录同一时刻只有一个调度进程
logs/            每个步骤每次尝试的 stdout/stderr
```

恢复语义：

1. 读快照并**从头回放事件日志**，用哈希链校验日志未被截断或篡改，用事件重建权威状态；
2. 快照与日志回放结果逐字段比对；
3. 崩溃在 `running` 中途（只有进入事件、没有离开事件）的步骤回到 `pending` 重新执行，但**已用尝试次数保留**，不能靠重启获得额外重试预算；
4. `succeeded` / `failed` / `skipped` 终态步骤重启后不执行，因此不会重复产出；
5. 如果把快照里的 `succeeded` 人为改回 `pending`，回放会发现日志里它已经走得更远，直接返回“状态回退”错误并指出步骤名，拒绝运行；
6. 流程定义会计算指纹并与状态目录绑定，换了一份定义去复用同一状态目录会被拒绝（避免步骤对不上）。

## 本地如何验证这些行为

### 跑全部自动化测试

```bash
go test ./... -race -v
```

重点用例（对应需求中的五类场景）：

| 场景 | 测试 | 验证点 |
| --- | --- | --- |
| 依赖成环 | `TestValidateCycle`、`TestValidateCycleAndMissingTogether` | 成环被识别并能与缺失依赖区分 |
| 依赖缺失 | `TestValidateMissingDep` | 引用不存在步骤被拒绝 |
| 并发上限 | `TestGlobalConcurrencyLimitNeverExceeded`、`TestResourceQuotaNeverExceeded`、`TestQueueingActuallyHappens` | 在步骤**执行期间**实时采样并发数，断言全程未超限，且峰值确实顶到上限（证明真的排队） |
| 失败传播 | `TestFailurePropagation`、`TestPermanentFailureNotRetried`、`TestRetryBudgetExhausted` | 下游 skipped、独立步骤照常、不可重试不重试、预算耗尽才失败 |
| 断点恢复 | `TestResumeFromStaleRunning`、`TestResumeDoesNotReexecuteCompleted`、`TestResumePreservesRetryBudget`、`TestSchedulerRejectsTamperedRollback` | stale running 重跑、已完成不重跑、尝试预算保留、人为回退被拒 |

测试日志通过 `-v` 可见，运行期日志统一打印**步骤名、尝试次数、状态变化**，例如：

```
[执行] 步骤=qc 尝试次数=2 状态变化=pending -> running
[失败] 步骤=qc 尝试次数=2 可重试=true 原因=命令以退出码 1 失败...
[重试] 步骤=qc 状态变化=running -> pending，200ms 后重试（还剩 1 次）
[完成] 步骤=qc 尝试次数=3 状态变化=running -> succeeded 产出=[clean.fq qc.report]
[排队] 步骤=align 等待资源标签=cpu 释放配额
```

### 手工验证断点恢复

```bash
cd examples/demo-workspace
../../scheduler run --state ./state ../pipeline.json     # 正常跑完
../../scheduler run --state ./state ../pipeline.json     # 立即返回，events.log 行数不变
# 模拟人为回退：把 snapshot.json 里某个 "status": "succeeded" 改成 "pending"
../../scheduler run --state ./state ../pipeline.json     # 报“检测到状态回退”并退出
```

### 手工验证并发与失败传播

用 `examples/pipeline.json`（`max_running=3`、`quotas.cpu=2`）观察并行与排队；把任一步骤命令改成 `exit 42` 可观察其下游全部 `skipped`、且只尝试一次。

## 目录结构

```
pipeline/          引擎核心
  types.go         定义/状态/事件类型
  errors.go        可区分的错误类型
  validate.go      成环、缺失依赖等定义校验
  loader.go        JSON 加载与定义指纹
  store.go         快照 + 哈希链事件日志、恢复与回退检测
  hash.go lock.go  哈希与文件锁辅助
  executor.go      shell 执行器与测试用内存执行器
  scheduler.go     调度、并发控制、重试、失败传播
  logger.go        线程安全的结构化日志
cmd/scheduler/     命令行入口
examples/          示例流程
```
