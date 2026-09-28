package gocheckpointcoordinator

import "time"

// RoundStatus 表示一轮检查点的生命周期状态。
type RoundStatus string

const (
	// RoundPending 轮次已发起，正在等待冻结集合中的成员上报。
	RoundPending RoundStatus = "PENDING"
	// RoundSucceeded 冻结集合全部成功上报，已一次性生成不可变清单。
	RoundSucceeded RoundStatus = "SUCCEEDED"
	// RoundAborted 检查点被主动中止。
	RoundAborted RoundStatus = "ABORTED"
	// RoundTimedOut 截止时间到达仍未集齐上报。
	RoundTimedOut RoundStatus = "TIMED_OUT"
)

// IsTerminal 报告该状态是否为终态。
func (s RoundStatus) IsTerminal() bool {
	return s == RoundSucceeded || s == RoundAborted || s == RoundTimedOut
}

// Failure 判断该终态是否为失败终态。失败轮次不能作为后续恢复依据。
func (s RoundStatus) Failure() bool {
	return s == RoundAborted || s == RoundTimedOut
}

// Task 是登记后的任务视图。Members 为发起检查点时冻结成员集合的固定依据。
type Task struct {
	ID        string
	Name      string
	Members   []string
	CreatedAt time.Time
}

// Report 是成员一次上报的全部内容。
// 同一成员对同一轮次的 Position 与 Digest 必须完全一致，否则视为冲突。
type Report struct {
	TaskID     string
	Round      int64
	MemberID   string
	Position   int64
	Digest     string
	ReportedAt time.Time
}

// ManifestEntry 是不可变检查点清单中单个成员的条目。
type ManifestEntry struct {
	MemberID string `json:"member_id"`
	Position int64  `json:"position"`
	Digest   string `json:"digest"`
}

// Manifest 是一轮检查点成功时一次性生成的不可变清单。
// 成功之后其任何字段都不会再变化；Digest 为全部条目排序后的规范化摘要。
type Manifest struct {
	Round        int64           `json:"round"`
	Entries      []ManifestEntry `json:"entries"`
	Digest       string          `json:"digest"`
	CompletedAt  time.Time       `json:"completed_at"`
	Notification string          `json:"notification"`
}

// Checkpoint 是一轮检查点的完整对外视图（成功/失败/进行中统一结构）。
type Checkpoint struct {
	TaskID    string
	Round     int64
	Status    RoundStatus
	Waiting   []string // 发起时冻结、需要等待的成员集合（成功后表示全部集齐）
	Deadline  time.Time
	CreatedAt time.Time
	EndedAt   time.Time // 进入终态的时间；PENDING 时为零值
	Manifest  *Manifest // 仅 SUCCEEDED 时非空
	Reason    string    // 失败终态原因（aborted / timed out）
}
