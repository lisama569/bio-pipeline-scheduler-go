# bio-pipeline-scheduler-go

一个面向本地生信分析步骤的可重复运行的流程引擎。流程用 JSON 描述步骤与依赖，引擎负责：

- 按 DAG 依赖顺序调度，无依赖关系的步骤在资源允许时**并行**执行；
- **全局并发上限**与**按资源标签（cpu/gpu/network…）的槽位配额**同时生效，超额步骤排队等待，执行全过程中实际并发数绝不突破上限；
- 每个步骤的状态、尝试（重试）次数和产出物 **append-only 落盘并 fsync**，进程中断后重启可断点恢复，已完成步骤绝不重复执行；
- 失败传播：上游失败的下游一律标记为 `unreachable`，不会被执行；
- 区分**可重试**与**不可重试**失败，不可重试失败只尝试一次；
- 对手工篡改状态文件（例如把已完成步骤改回 pending）明确拒绝并说明原因。

仅使用 Go 标准库，无第三方依赖（Go ≥ 1.25）。

## 1. 构建

```bash
go build ./...
go build -o scheduler ./cmd/scheduler
```

## 2. 流程描述格式

一个 UTF-8 JSON 文件，顶层结构：

```json
{
  "name": "exome-mini",
  "steps": [
    {
      "name": "align",
      "command": "minimap2 -t 2 ref.fa reads.fq > aligned.bam",
      "depends_on": ["download_reads"],
      "retries": 3,
      "resources": {"tag": "cpu", "slots": 2},
      "outputs": ["aligned.bam"]
    }
  ]
}
```

| 字段 | 必填 | 含义 |
| --- | --- | --- |
| `name` | 是 | 步骤唯一名，非空，全流程不可重复 |
| `command` | 是 | 通过 `sh -c` 在 `--workdir` 下执行的 shell 命令 |
| `depends_on` | 否 | 依赖的步骤名列表；所有依赖成功后该步骤才可执行 |
| `retries` | 否 | 额外重试次数（默认 0）。总尝试次数 = 1 + `retries` |
| `resources.tag` | 否 | 资源池标签，如 `cpu`、`gpu`、`network`；不同标签互不影响 |
| `resources.slots` | 否 | 该步骤占用标签池的槽位数（默认 1） |
| `outputs` | 否 | 声明的产出物相对路径（相对 workdir）；命令成功后会逐一校验存在且非空 |

完整示例见 [`examples/pipeline.json`](examples/pipeline.json)。

### 加载期被明确拒绝的问题

加载（`scheduler validate` / `scheduler run`）时做静态校验，任何一类问题都会**在执行任何步骤之前**失败，错误带可区分的类别标记：

| 错误类别 `Kind` | 触发条件 |
| --- | --- |
| `invalid_spec` | JSON 无法解析、步骤列表为空、步骤名为空、命令为空 |
| `duplicate_step` | 两个步骤同名 |
| `missing_dependency` | `depends_on` 引用了不存在的步骤（错误信息含步骤名与被引用名） |
| `cycle` | 依赖图存在有向环，包括自依赖（错误信息打印完整环，如 `a -> c -> b -> a`） |

缺失依赖与成环是**分开检测**的：先查悬空边再查环，绝不会把"引用了不存在的步骤"误报成环。

## 3. 步骤状态及含义

| 状态 | 含义 | 是否终态 |
| --- | --- | --- |
| `pending` | 从未执行，或上次进程在该步骤运行中被杀（重新打开状态目录时由 `running` 归一化而来） | 否 |
| `running` | 某次尝试正在执行；该状态只存在于事件流中，重启时不会被当作"在跑" | 否 |
| `succeeded` | 执行成功且声明产出物校验通过；产出物清单已落盘 | **是** |
| `failed` | 可重试失败已用尽重试预算，或发生不可重试失败 | **是** |
| `unreachable` | 至少一个（传递）依赖未成功（失败/不可达），该步骤从未执行 | **是** |

终态之间不可逆。合法迁移路径：

```
pending → running → succeeded
                   ↘ failed
running → (崩溃) → pending（仅在进程重启时由引擎归一化，不接受手工写入）
任意待执行步骤 → unreachable（当上游终态非 succeeded）
```

## 4. 重试规则

- 步骤命令以 **退出码 0** 结束且 `outputs` 全部存在、非空 → 成功。
- **退出码 1**（或无法启动等普通错误）→ `retryable`，在 `retries` 预算内做指数退避后重试（默认初始退避 50ms，逐次翻倍）。
- **退出码 2** → 约定的**不可重试致命错误**（`non_retryable`），无论 `retries` 配多少都只尝试一次。适合"输入文件格式错误"这类重试无意义的失败。
- 退出码 0 但产出物缺失/为空 → `non_retryable`（防止假成功污染下游）。
- 收到中断信号导致的取消 → 不重试，步骤保持 `pending`，重启后补跑。

总尝试次数 = `retries + 1`。每次尝试的 `running` 事件和最终结论都带递增的 `attempts` 计数落盘。

## 5. 并发与资源限制

调度同时受两层约束，必须**同时**满足才能开始执行：

1. **全局并发** `--concurrency N`：同一时刻运行中的步骤数 ≤ N（0/缺省 = 步骤总数，即不限）。
2. **标签配额** `--quota cpu=4,gpu=1`：同一时刻同一标签池被占用的槽位数之和 ≤ 配额；每个步骤占用 `resources.slots` 个槽位（默认 1）。没有出现在 `--quota` 中的标签不限。

不满足条件的步骤在 FIFO 信号量上排队，**不会**突破任何一个上限（实现上用带缓冲 channel 作为令牌池，容量由 channel 本身保证）。取消等待中的排队步骤会立即归还已取到的令牌，不会泄漏配额。

## 6. 状态持久化与断点恢复

状态写在 `--state-dir`（默认 `.pipeline-state`）：

| 文件 | 作用 |
| --- | --- |
| `events.jsonl` | append-only 事件流，事实来源。每行一个状态迁移，含序号、步骤、状态、尝试次数、产出物、时间戳 |
| `manifest.json` | 记录事件条数与最后一条事件的哈希，每次写入都随事件一起 fsync；用于检测事件流被截断 |
| `state.json` | 人类可读的紧凑快照，仅作缓存，打开时与事件流交叉校验 |

完整性机制：

- 每条事件包含前一条事件内容的 SHA-256（哈希链），改动/删除中间任何一条都会在重放时被发现；
- `manifest.json` 锚定链尾，**只删最后一条事件**这种让哈希链前缀仍然自洽的篡改也会被发现；
- 事件按序号严格递增，尝试次数只增不减；
- 重放时强制执行状态机：`succeeded/failed/unreachable` 等终态迁移到任何其他状态都会返回
  `RollbackError`，明确说明"该步骤已完成、产出物已记录，拒绝重跑以免重复产出"以及恢复办法
  （恢复事件流，或删除状态目录从头跑）；
- `state.json` 与事件流不一致（例如手工把 `succeeded` 改成 `pending`）同样返回 `RollbackError`；
- 事件先 `write + flush + fsync`，再更新内存状态并唤醒下游，因此"下游已开始"必然意味着"上游成功已落盘"；
- 重新运行时：`succeeded` 步骤直接跳过（不执行命令、不重复产出），中断在 `running` 的步骤与尚未执行的步骤正常补跑，`failed/unreachable` 的终态按失败结论恢复。
- 状态目录中出现当前流程定义里不存在的步骤名时拒绝运行，防止拿 A 流程的状态目录跑 B 流程。

## 7. 命令行用法

```bash
# 只做静态校验，不执行
scheduler validate --file examples/pipeline.json

# 执行（可反复执行；第二次起已完成步骤全部跳过）
scheduler run \
  --file examples/pipeline.json \
  --workdir . \
  --state-dir .pipeline-state \
  --concurrency 4 \
  --quota cpu=4,gpu=1,network=2

# 退出码：0 全部成功；1 流程失败或描述非法；2 命令行用法错误
```

`run` 会捕获 SIGINT/SIGTERM：当前尝试停止，排队步骤退出，已 fsync 的状态保留，再次执行同样的命令即可续跑。

运行日志逐行打印 `step=<名> attempt=<第几次尝试> status=<状态> reason=<原因>`，便于观察状态变化与重试。

## 8. 本地验证行为

```bash
# 全部单测（26 个），含 -race；并发用例在整个执行过程中以外部采样器
# 持续读取实时并发数，断言峰值从未超过上限，而非只看最终结果
go test ./... -race -count=1 -v

# 覆盖率
go test ./internal/pipeline/ -coverprofile=cov.out && go tool cover -func=cov.out
```

测试覆盖的关键场景：

| 场景 | 测试 |
| --- | --- |
| 依赖成环（含自依赖）被拒且类别为 `cycle` | `TestValidateCycleDetectedAndClassified` / `TestValidateSelfDependencyIsCycle` |
| 引用不存在步骤被拒且类别为 `missing_dependency` | `TestValidateMissingDependencyDetectedAndClassified` |
| 重名、空定义、坏 JSON | `TestValidateDuplicateStep` / `TestValidateEmptyAndMalformed` |
| 全局上限全程不被突破（外部采样 + 峰值断言必须达到上限） | `TestExecutorConcurrencyNeverExceedsGlobalLimit` |
| cap=1 下两步骤绝不重叠、释放后排队者才启动 | `TestGlobalLimitOneQueuesWithoutOverlap` |
| 标签配额（多槽位步骤）不被突破 | `TestExecutorResourceTagQuota` |
| 失败后直接/传递下游为 `unreachable` 且从未被调用，无关节点照常成功 | `TestExecutorFailureMarksDownstreamUnreachable` |
| 可重试失败在预算内重试成功、预算耗尽才失败 | `TestExecutorRetryableFailuresAreRetried` / `TestExecutorRetryBudgetExhaustedFails` |
| 不可重试失败只尝试一次且原因可区分 | `TestExecutorNonRetryableFailsImmediately` |
| 进程中断后恢复：已完成步骤 0 次重跑，未完成步骤恰好补跑一次 | `TestRecoveryResumesWithoutRepeatingCompletedSteps` |
| 真实命令 + 真实产出物：重跑后计数器文件内容仍为 1（未重复产出） | `TestRecoveryWithCommandRunnerAndOutputs` |
| 手工把终态改回 pending（改事件流/改快照）被拒 | `TestStoreRejectsRollback*` / `TestStoreDetects*Tampering` |
| 状态目录与流程定义不匹配被拒 | `TestRecoveryRefusesMismatchedPipeline` |

手工端到端验证：

```bash
go build -o scheduler ./cmd/scheduler

# 正常 + 断点重跑
scheduler run --file examples/pipeline.json --concurrency 2
scheduler run --file examples/pipeline.json --concurrency 2   # 全部 "restored from disk, skipped"

# 校验拒绝
echo '{"steps":[{"name":"a","command":"echo a","depends_on":["x"]}]}' | \
  sed 's/^/{"steps":[]}/' >/dev/null  # 或直接写文件后：
scheduler validate --file bad-cycle.json

# 观察回退被拒：把 state.json 里某个 succeeded 改成 pending 再运行
scheduler run --file examples/pipeline.json   # 报 RollbackError，退出码 1
```

## 9. 包结构

```
cmd/scheduler/main.go          CLI 入口（validate / run、配额解析、信号处理）
internal/pipeline/pipeline.go  核心类型：Spec/StepSpec/Status/Resources/Result
internal/pipeline/errors.go    可区分的校验错误类别 + RollbackError
internal/pipeline/load.go      JSON 解析 + DAG 校验（重名/悬空边/成环）
internal/pipeline/store.go     哈希链事件流、manifest、快照、重放与回退检测
internal/pipeline/runner.go    本地 sh 命令执行、退出码/产出物失败分类
internal/pipeline/executor.go  调度循环、两级配额、重试退避、失败传播
```
