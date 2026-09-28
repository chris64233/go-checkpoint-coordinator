# go-checkpoint-coordinator

分布式任务的一致检查点协调服务（Go 库）。多个执行成员协同处理一个分布式任务时，
本服务负责发起检查点、收集成员上报，并在语义上保证：**只在冻结的成员集合全部上报后，
原子地生成一份不可变的检查点清单与唯一一个完成通知**。

## 一致性语义

### 1. 任务登记与成员冻结

- `RegisterTask` 登记任务 ID 与**固定的执行成员集合**（成员去重、内部排序）。
- `StartCheckpoint` 发起一轮检查点：分配**从 1 开始的递增轮次号**，并把当前任务成员
  **拷贝冻结**为该轮的等待集合 `Waiting`。此后冻结集合与任务成员解耦：
  - 成员临时退出、集合外的新成员上报，都不会改变本轮完成条件；
  - 冻结成员即使逻辑上已退出，仍需其上报才能完成；
  - 非冻结成员上报返回 `ErrUnknownMember`。
- 上一轮处于 `PENDING` 时发起新一轮返回 `ErrRoundInProgress`，**不允许覆盖进行中的轮次**。

### 2. 上报：幂等、冲突、陈旧

`Report` 必须携带 `TaskID / Round / MemberID / Position（处理位置）/ Digest（状态摘要）`。

| 情形 | 行为 |
|---|---|
| 成员首次上报当前 PENDING 轮 | 接受并记录**首次内容** |
| 同成员同轮重复上报，`Position` 与 `Digest` 完全相同 | 返回原结果（`Replayed=true`），无新副作用 |
| 同成员同轮上报内容不同 | 返回 `ErrConflict`；首次内容保持有效 |
| 对**旧轮次**的迟到上报（内容相同/不同） | 回放原结果 / `ErrConflict`；从未记录的成员返回 `ErrStaleRound`，**绝不写入状态、不影响当前轮** |
| 已终态轮次的补报（当前轮） | 成功轮相同内容可回放；未记录成员返回 `ErrRoundTerminal` |

### 3. 原子完成与不可变清单

- 仅当冻结集合全部成功上报，才在**同一临界区**内把轮次从 `PENDING` 迁移为
  `SUCCEEDED`，并一次性生成 `Manifest`：按成员排序的条目、规范化 SHA-256 摘要、
  唯一 `Notification` ID 与完成时间。
- 多个成员并发上报最后几条时，**恰好一个**调用得到 `Completed=true`；
  不会产生多份清单或多个通知。
- 成功之前任何外部查询只能看到 `PENDING`（无清单）；不存在“完成了一半”的可见状态。
  成功之后清单内容永不再变（查询返回深拷贝，调用方无法篡改内部状态）。

### 4. 中止、超时与唯一终态

- `Abort` 可主动中止 PENDING 轮（重复中止幂等）；成功轮再中止返回 `ErrRoundTerminal`。
- `AdvanceTimeouts(now)` 把所有已到 `deadline` 且仍 PENDING 的**最新轮**一次性标记为
  `TIMED_OUT`（外部定时器周期调用即可，例如每秒）。
- 末次成功上报、中止、超时三者并发竞争时，由于状态迁移与落盘都在同一把互斥锁的
  临界区内完成，**先提交者胜**，只保留一个终态。
- `ABORTED` / `TIMED_OUT` 均为失败终态，**不会**出现在恢复依据中；失败后迟到的上报
  或中止也无法复活该轮。

### 5. 可恢复检查点与历史

- `LatestCheckpoint` 返回任务最新一轮 `SUCCEEDED` 的检查点（含不可变清单）；
  没有成功轮时返回 `nil`。失败轮次永远不会被返回为恢复点。
- `GetRound` / `GetLatestRound` 查询任意/最新轮，`History` 按轮次升序返回全部历史。

### 6. 持久化

- 构造时传入快照文件路径即开启持久化；传空路径仅使用内存（适合测试）。
- 每次状态变更都在临界区内把完整状态以 JSON 写盘：**临时文件写入 → `fsync` → `rename`**
  原子替换，崩溃后只会看到旧快照或新快照，不会出现半截文件。
- 进程重启后用同一路径 `NewCoordinator` 即可完整恢复任务、冻结集合、各轮终态、
  不可变清单与通知 ID，下一轮轮次号继续递增。

## 安装与 API

```go
import gocc "github.com/chris64233/go-checkpoint-coordinator"
```

| API | 说明 |
|---|---|
| `NewCoordinator(snapshotPath string, opts ...Option) (*Coordinator, error)` | 创建/恢复协调器；`WithClock` 可注入时钟 |
| `RegisterTask(id, name string, members []string) (*Task, error)` | 登记任务与固定成员 |
| `StartCheckpoint(taskID string, deadline time.Time) (*Checkpoint, error)` | 发起新一轮（零 deadline = 不超时） |
| `Report(Report) (*ReportResult, error)` | 成员上报（幂等/冲突/陈旧语义见上） |
| `Abort(taskID string, roundNo int64, reason string) (*Checkpoint, error)` | 中止；`roundNo=0` 表示最新轮 |
| `AdvanceTimeouts(now time.Time) ([]*Checkpoint, error)` | 推进截止时间，`now` 零值用系统时钟 |
| `LatestCheckpoint(taskID string) (*Checkpoint, error)` | 最新可恢复（成功）检查点，无则 `nil` |
| `GetRound / GetLatestRound / History` | 单轮 / 最新轮 / 全部历史查询 |

错误哨兵：`ErrTaskNotFound`、`ErrTaskExists`、`ErrRoundInProgress`、`ErrRoundNotFound`、
`ErrUnknownMember`、`ErrConflict`、`ErrRoundTerminal`、`ErrStaleRound`、
`ErrInvalidArgument`，均可用 `errors.Is` 判定。

## 使用示例

```go
coord, err := gocc.NewCoordinator("/var/lib/checkpoint/state.json")
if err != nil {
    log.Fatal(err)
}

if _, err := coord.RegisterTask("orders", "订单处理", []string{"w1", "w2", "w3"}); err != nil {
    log.Fatal(err)
}

// 发起一轮，30 秒截止
cp, err := coord.StartCheckpoint("orders", time.Now().Add(30*time.Second))
if err != nil {
    log.Fatal(err) // 上一轮未结束会得到 ErrRoundInProgress
}
round := cp.Round

// 各成员上报（可并发、可重试；相同内容天然幂等）
res, err := coord.Report(gocc.Report{
    TaskID: "orders", Round: round, MemberID: "w1",
    Position: 4096, Digest: "sha256:abcd...",
})
if err != nil {
    // ErrConflict=同轮内容冲突；ErrUnknownMember=非冻结成员 …
    log.Fatal(err)
}
if res.Completed {
    fmt.Println("检查点完成，唯一通知：", res.Checkpoint.Manifest.Notification)
}

// 周期性推进超时（也可由定时器以系统时间调用 AdvanceTimeouts(time.Time{})）
if _, err := coord.AdvanceTimeouts(time.Now()); err != nil {
    log.Print(err)
}

// 故障恢复时只取最新成功检查点；中止/超时轮不会出现
recoverable, err := coord.LatestCheckpoint("orders")
```

## 测试

```bash
go test -race -count=1 ./...
```

测试覆盖（`coordinator_test.go`，共 16 个用例）：

- 成员登记、排序去重、参数校验、重复登记；
- 轮次号递增、进行中不可覆盖、冻结集合不受成员退出/外来成员影响；
- 上报幂等回放、内容冲突、旧轮迟到上报隔离；
- 16 成员并发末次上报：恰好一次完成、唯一清单/通知、查询无中间态；
- 上报 vs 中止、上报 vs 超时各 200 轮竞争：终态唯一且对双方返回自洽；
- 超时推进（提前/到期/仅作用于最新轮）、失败轮不可恢复也不可复活；
- 历史顺序与状态、最新恢复点；
- JSON 快照重启完整恢复（清单摘要与通知 ID 不变、轮次号续增）、原子写不留临时文件。

## 设计说明

- 协调器是单进程内的强一致内核：一把互斥锁串行化全部状态迁移，且“改状态 + 落盘”
  在同一临界区内完成；落盘失败会回滚内存变更，调用方可安全重试。
- 若要多副本/跨进程部署，可在此外层叠加复制或选主（状态全部在快照文件中，
  恢复点即 `LatestCheckpoint`），本库自身不提供分布式复制。
