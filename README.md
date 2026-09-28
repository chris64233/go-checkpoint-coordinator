# go-checkpoint-coordinator

分布式任务的**一致检查点（checkpoint）协调服务**。多个成员共同处理一个分布式任务，
协调器收集每个成员的处理位置与状态摘要，在全部成员到齐时一次性产出不可变的检查点
清单，并对中止、超时、并发最后上报等竞争情况给出唯一终态。全部状态持久化到磁盘，
进程重启后可继续运行。

## 核心语义

### 1. 任务登记与成员冻结

- `RegisterTask(taskID, members)`：登记任务及其**固定**执行成员；成员列表会被排序、去重，
  登记后不可修改。
- `StartCheckpoint(taskID, timeout)`：发起一轮检查点。轮次号在任务内从 1 开始**单调递增**，
  发起瞬间把需要等待的成员集合**冻结**（拷贝一份已登记成员）。
- 上一轮仍是 `pending` 时不能发起新一轮（返回 `ErrRoundOpen`），不能被直接覆盖。
- 成员“临时退出”不会缩小冻结集合：完成条件始终是发起时冻结的全体成员；成员迟到或缺席
  只会让轮次保持等待（或最终超时）。

轮次状态机：

```
pending ──全部成员上报──▶ completed（生成清单 + 完成通知，均唯一）
   │  ├──主动中止────────▶ aborted   （失败轮次，仅失败通知）
   │  └──截止时间到达────▶ timed_out （失败轮次，仅失败通知）
```

### 2. 成员上报：幂等、冲突、陈旧轮次

`Report(taskID, round, member, position, digest)` 必须携带任务、轮次、处理位置和状态摘要：

| 情况 | 行为 |
|---|---|
| 同一成员对同一轮次重复上报**相同** position+digest | 幂等，返回**原结果**（`AlreadyReported=true`）；轮次完成后仍可重放，返回同一份清单与通知 |
| 同一成员重复上报**不同**内容 | 返回 `ErrConflict`，已有上报不被覆盖 |
| 上报目标轮次 ≠ 任务当前轮次（旧轮迟到/未来轮） | 返回 `ErrStaleRound`，迟到上报绝不混入当前检查点 |
| 上报成员不在冻结集合中 | 返回 `ErrUnknownMember` |
| 轮次已 aborted/timed_out | 返回 `ErrAlreadyTerminal`，失败轮次不再接收任何上报 |

### 3. 原子完成：唯一清单与唯一通知

- 只有冻结集合中**所有**成员都成功上报，才会生成清单（`Manifest`）与完成通知
  （`Notification`）；清单创建后不可变（查询返回深拷贝）。
- 整个状态迁移（接收最后一个上报 → 置 completed → 写清单 → 写通知）在同一把互斥锁内
  完成并一次性原子持久化。最后几个成员并发上报时，**恰好一个**调用者观察到完成，
  外部查询永远看不到“只完成一部分”的检查点（pending 轮次及其部分上报不会作为可恢复
  检查点暴露）。
- 清单 ID 由任务、轮次和全部上报内容做 SHA-256 派生，确定性且不可重复。

### 4. 中止 / 超时 / 最后上报的终态竞争

- `AbortCheckpoint(taskID, reason)`：主动中止当前轮次。
- 截止时间：可在 `StartCheckpoint` 时指定；`Report` 时懒判定，也可调用
  `AdvanceTimeouts()` 主动推进所有到点轮次（适合没有新上报到来时由定时器调用）。
- 中止、超时推进、最后一次成功上报三者互相竞争时，由协调器串行化：**一轮只有一个终态、
  一条通知**。后到的竞争操作得到 `ErrAlreadyTerminal`。
- aborted / timed_out 都是**失败轮次**：不生成清单、不产生完成通知，且
  `GetLatestRecoverableCheckpoint` 永远不会返回它们——失败轮次不能成为恢复依据。

### 5. 查询与持久化

- `GetLatestRecoverableCheckpoint(taskID)`：返回最近一个 **completed** 轮次的清单；
  后面的轮次失败了也不会回退成“无”，而是继续指向更早的成功轮次；无成功轮次时返回
  `nil, nil`。
- `GetCheckpointHistory(taskID)`：按轮次升序返回全部历史（每轮的状态、清单、通知）。
- 另提供 `GetTask` / `GetRound` / `GetManifest` / `Notifications` 等查询。
- 每次变更后把全部状态写成一个 JSON 文件：先写同目录临时文件 → `fsync` → `rename`
  原子替换 → 目录 `fsync`，任何时刻文件要么是旧版本要么是新版本，不会读到半截内容。
  用 `New(path)` 重新打开即可恢复；写入失败会回滚内存状态并向调用者返回错误，
  协调器不会对外暴露未持久化成功的状态。

## 快速开始

```go
package main

import (
	"fmt"
	"time"

	gcc "github.com/chris64233/go-checkpoint-coordinator"
)

func main() {
	co, err := gcc.New("data/checkpoint.json") // 不存在会自动创建
	if err != nil {
		panic(err)
	}

	if _, err := co.RegisterTask("orders", []string{"shard-a", "shard-b"}); err != nil {
		panic(err)
	}

	if _, err := co.StartCheckpoint("orders", 30*time.Second); err != nil {
		panic(err)
	}

	res, err := co.Report("orders", 1, "shard-a", 1024, "sha256:aaa")
	if err != nil {
		panic(err)
	}
	res, err = co.Report("orders", 1, "shard-b", 2048, "sha256:bbb")
	if err != nil {
		panic(err)
	}
	if res.Completed {
		fmt.Println("checkpoint manifest:", res.Manifest.ID)
	}

	// 之后任意时刻 / 重启之后：
	mani, _ := co.GetLatestRecoverableCheckpoint("orders")
	fmt.Printf("recover from round %d\n", mani.Round)
}
```

测试中可用 `NewInMemory()` 使用不落盘的存储；也可用 `NewWithStore` 注入自定义存储和时钟
（`Options.Clock`，便于确定性地测试超时）。

## API 一览

| 方法 | 说明 |
|---|---|
| `New(path)` / `NewInMemory()` / `NewWithStore(store, opts)` | 打开/构造协调器 |
| `RegisterTask(taskID, members)` | 登记固定成员的任务 |
| `StartCheckpoint(taskID, timeout)` | 发起新一轮，冻结成员集合；`timeout<=0` 表示不自动超时 |
| `Report(taskID, round, member, position, digest)` | 成员上报；幂等/冲突/陈旧校验，可能原子完成本轮 |
| `AbortCheckpoint(taskID, reason)` | 主动中止当前轮次 |
| `AdvanceTimeouts()` | 把所有已到截止时间的 pending 轮次推进为 timed_out |
| `GetLatestRecoverableCheckpoint(taskID)` | 最新成功检查点（失败/pending 轮次不可见） |
| `GetCheckpointHistory(taskID)` | 全部轮次历史（含清单与通知） |
| `GetTask` / `GetRound` / `GetManifest` / `Notifications` | 单项查询 |

## 错误哨兵

`ErrTaskExists`、`ErrTaskNotFound`、`ErrEmptyMembers`、`ErrRoundOpen`、`ErrNoRound`、
`ErrUnknownMember`、`ErrStaleRound`、`ErrConflict`、`ErrAlreadyTerminal`，均可用
`errors.Is` 判定。

## 并发模型

所有方法在单个 `Coordinator` 上可安全并发调用。每个变更操作在同一把互斥锁内完成
“读状态 → 校验 → 迁移 → 持久化”全过程，因此：

- 最后一批并发上报只会产生一份清单、一条通知；
- 中止/超时/最后上报竞争只保留一个终态；
- 查询不会读到迁移中途的状态；
- 内存状态与磁盘状态不会出现“已返回成功但未落盘”的分歧。

## 运行测试

```bash
go test -race -count=1 ./...
```

测试覆盖：登记校验与去重、冻结集合隔离、轮次递增与覆盖保护、上报幂等/冲突/陈旧轮次、
8 成员 100 轮并发最后上报只完成一次（含持续读者校验无部分结果可见）、中止与最后上报
200 轮竞争、超时与最后上报 200 轮竞争、懒超时与批量超时推进、失败轮次不作恢复点与
历史查询、文件存储重启恢复、写入失败回滚等。
