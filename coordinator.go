// Package gocheckpointcoordinator 实现分布式任务的一致检查点协调服务。
//
// 核心一致性语义：
//   - 任务登记固定执行成员；发起检查点时生成递增轮次号，并冻结本轮等待集合，
//     此后成员临时退出或任务成员变更都不会改变已冻结的完成条件。
//   - 上一轮未进入终态时不能发起新一轮（不允许覆盖进行中的轮次）。
//   - 成员上报携带（任务、轮次、处理位置、状态摘要）。相同内容重复上报返回原结果
//     （幂等）；内容不同返回冲突错误；旧轮次的迟到上报不会被记入当前检查点。
//   - 仅当冻结集合全部成功上报，才在同一个临界区内一次性生成不可变清单与唯一完成
//     通知；并发上报既不会产生多份清单，外部查询也看不到“完成了一半”的结果。
//   - 中止、超时与末次上报互相竞争时，以临界区内先提交的状态迁移为准，只保留一个
//     终态；中止/超时的失败轮次不能作为后续恢复依据。
//   - 全部状态通过 JSON 快照原子落盘（临时文件 + fsync + rename），进程重启后
//     从磁盘完整恢复。
package gocheckpointcoordinator

import (
	"crypto/rand"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
	"time"
)

// ReportResult 是成员上报的处理结果。
type ReportResult struct {
	TaskID   string
	Round    int64
	MemberID string
	// Accepted 表示该上报内容已被（或曾被）该轮接受。
	Accepted bool
	// Replayed 为 true 表示这是相同内容的重复上报，返回的是首次上报的原结果。
	Replayed bool
	// Completed 为 true 表示该上报使本轮集齐冻结集合、一次性生成了检查点清单。
	Completed bool
	// Status 是处理后该轮所处的状态。
	Status RoundStatus
	// Checkpoint 在轮次已进入终态时非空（成功时含不可变清单）。
	Checkpoint *Checkpoint
}

// Coordinator 是检查点协调器。所有方法均可被多个 goroutine 并发调用。
type Coordinator struct {
	mu    sync.Mutex
	tasks map[string]*taskState
	store store
	now   func() time.Time
}

// Option 用于配置 Coordinator。
type Option func(*Coordinator)

// WithClock 注入自定义时钟，主要用于测试与确定性的超时推进。
func WithClock(now func() time.Time) Option {
	return func(c *Coordinator) {
		if now != nil {
			c.now = now
		}
	}
}

// NewCoordinator 创建协调器。snapshotPath 为空时只在内存中保存状态；
// 否则从该 JSON 快照文件恢复状态，后续每次变更都原子写回该文件。
func NewCoordinator(snapshotPath string, opts ...Option) (*Coordinator, error) {
	var st store = memoryStore{}
	if snapshotPath != "" {
		st = newFileStore(snapshotPath)
	}
	snap, err := st.load()
	if err != nil {
		return nil, err
	}
	c := &Coordinator{
		tasks: map[string]*taskState{},
		store: st,
		now:   time.Now,
	}
	if snap != nil && snap.Tasks != nil {
		c.tasks = snap.Tasks
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

func (c *Coordinator) persistLocked() error {
	snap := &snapshot{
		Version: snapshotVersion,
		SavedAt: c.now(),
		Tasks:   c.tasks,
	}
	return c.store.save(snap)
}

func newID(prefix string) string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 失败在正常运行环境中不应发生；退化为时间戳也保持唯一性。
		return prefix + "-" + time.Now().Format("20060102T150405.000000000")
	}
	return prefix + "-" + hex.EncodeToString(b[:])
}

// RegisterTask 登记一个任务及其固定执行成员。
func (c *Coordinator) RegisterTask(id, name string, members []string) (*Task, error) {
	if id == "" {
		return nil, ErrInvalidArgument
	}
	if len(members) == 0 {
		return nil, ErrInvalidArgument
	}
	seen := make(map[string]struct{}, len(members))
	for _, m := range members {
		if m == "" {
			return nil, ErrInvalidArgument
		}
		if _, dup := seen[m]; dup {
			return nil, ErrInvalidArgument
		}
		seen[m] = struct{}{}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.tasks[id]; ok {
		return nil, ErrTaskExists
	}
	frozen := append([]string(nil), members...)
	sort.Strings(frozen)
	ts := &taskState{
		ID:        id,
		Name:      name,
		Members:   frozen,
		CreatedAt: c.now(),
		NextRound: 1,
		Rounds:    map[int64]*roundState{},
	}
	c.tasks[id] = ts
	if err := c.persistLocked(); err != nil {
		delete(c.tasks, id)
		return nil, err
	}
	return taskView(ts), nil
}

// GetTask 返回任务登记视图。
func (c *Coordinator) GetTask(id string) (*Task, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ts, ok := c.tasks[id]
	if !ok {
		return nil, ErrTaskNotFound
	}
	return taskView(ts), nil
}

func taskView(ts *taskState) *Task {
	return &Task{
		ID:        ts.ID,
		Name:      ts.Name,
		Members:   append([]string(nil), ts.Members...),
		CreatedAt: ts.CreatedAt,
	}
}

// StartCheckpoint 发起一轮新检查点：生成递增轮次号并冻结等待成员集合。
// deadline 为零值表示该轮不设截止时间。上一轮尚未结束时返回 ErrRoundInProgress。
func (c *Coordinator) StartCheckpoint(taskID string, deadline time.Time) (*Checkpoint, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ts, ok := c.tasks[taskID]
	if !ok {
		return nil, ErrTaskNotFound
	}
	if latest, ok := ts.Rounds[ts.NextRound-1]; ok && latest.Status == RoundPending {
		return nil, ErrRoundInProgress
	}

	round := ts.NextRound
	rs := &roundState{
		Round:     round,
		Status:    RoundPending,
		Waiting:   append([]string(nil), ts.Members...), // 冻结：此后与任务成员解耦
		Reports:   map[string]*reportState{},
		Deadline:  deadline,
		CreatedAt: c.now(),
	}
	ts.Rounds[round] = rs
	ts.NextRound = round + 1
	if err := c.persistLocked(); err != nil {
		delete(ts.Rounds, round)
		ts.NextRound = round
		return nil, err
	}
	return checkpointView(taskID, rs), nil
}

// Report 处理成员上报。详细语义见包注释。
func (c *Coordinator) Report(rep Report) (*ReportResult, error) {
	if rep.TaskID == "" || rep.MemberID == "" || rep.Digest == "" {
		return nil, ErrInvalidArgument
	}
	if rep.Round <= 0 {
		return nil, ErrInvalidArgument
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	ts, ok := c.tasks[rep.TaskID]
	if !ok {
		return nil, ErrTaskNotFound
	}
	rs, ok := ts.Rounds[rep.Round]
	if !ok {
		return nil, ErrRoundNotFound
	}

	latest := ts.NextRound - 1
	isCurrent := rs.Round == latest

	// 旧轮次的迟到上报：除“相同内容回放”和“内容冲突”外一律拒绝，
	// 绝不写入任何状态，更不可能影响当前检查点。
	if !isCurrent {
		return c.replayOrRejectLocked(ts, rs, rep, true)
	}

	// 当前轮：成员必须在发起时冻结的集合内。
	if !memberIn(rs.Waiting, rep.MemberID) {
		return nil, ErrUnknownMember
	}

	if rs.Status.IsTerminal() {
		return c.replayOrRejectLocked(ts, rs, rep, false)
	}

	// PENDING：幂等 / 冲突 / 新上报。
	if prev, ok := rs.Reports[rep.MemberID]; ok {
		if prev.Position == rep.Position && prev.Digest == rep.Digest {
			return c.resultLocked(rep, true, false, rs), nil
		}
		return nil, ErrConflict
	}

	rs.Reports[rep.MemberID] = &reportState{
		Position:   rep.Position,
		Digest:     rep.Digest,
		ReportedAt: c.now(),
	}

	completed := false
	// 只有冻结集合中的成员能进入 Reports，因此数量相等即“全部成功上报”。
	if len(rs.Reports) == len(rs.Waiting) {
		c.completeLocked(rs)
		completed = true
	}
	if err := c.persistLocked(); err != nil {
		// 落盘失败：回滚内存中的本次变更，调用方可安全重试。
		delete(rs.Reports, rep.MemberID)
		if completed {
			rs.Status = RoundPending
			rs.EndedAt = time.Time{}
			rs.Manifest = nil
		}
		return nil, err
	}
	return c.resultLocked(rep, false, completed, rs), nil
}

// replayOrRejectLocked 处理重复/迟到上报。
// stale=true 表示目标轮次已经不是当前轮。
func (c *Coordinator) replayOrRejectLocked(ts *taskState, rs *roundState, rep Report, stale bool) (*ReportResult, error) {
	prev, memberKnown := rs.Reports[rep.MemberID]
	if !memberKnown {
		// 该成员在这轮从未被记录。
		if !memberIn(rs.Waiting, rep.MemberID) {
			// 非冻结成员：旧轮返回陈旧错误，当前轮返回未知成员错误。
			if stale {
				return nil, ErrStaleRound
			}
			return nil, ErrUnknownMember
		}
		// 冻结成员但此前从未上报：轮次已终态，拒绝补录。
		if stale {
			return nil, ErrStaleRound
		}
		return nil, ErrRoundTerminal
	}
	if prev.Position != rep.Position || prev.Digest != rep.Digest {
		return nil, ErrConflict
	}
	// 相同内容：无论轮次成功还是失败，都返回首次上报的原结果，不产生新副作用。
	return c.resultLocked(rep, true, false, rs), nil
}

func (c *Coordinator) resultLocked(rep Report, replayed, completedThisCall bool, rs *roundState) *ReportResult {
	res := &ReportResult{
		TaskID:   rep.TaskID,
		Round:    rs.Round,
		MemberID: rep.MemberID,
		Accepted: true,
		Replayed: replayed,
		// 仅当本次调用真正触发了一次性完成时才置 true；
		// 重复上报即使看到已完成清单，也返回“原结果”语义而不冒充完成者。
		Completed: completedThisCall,
		Status:    rs.Status,
	}
	if rs.Status.IsTerminal() {
		res.Checkpoint = checkpointView(rep.TaskID, rs)
	}
	return res
}

// completeLocked 在临界区内一次性把轮次从 PENDING 迁移到 SUCCEEDED，
// 生成不可变清单与唯一完成通知。并发上报下至多有一个调用者走到这里。
func (c *Coordinator) completeLocked(rs *roundState) {
	entries := make([]ManifestEntry, 0, len(rs.Waiting))
	for _, m := range rs.Waiting {
		r := rs.Reports[m]
		entries = append(entries, ManifestEntry{MemberID: m, Position: r.Position, Digest: r.Digest})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].MemberID < entries[j].MemberID })

	var sb strings.Builder
	for _, e := range entries {
		sb.WriteString(e.MemberID)
		sb.WriteByte(' ')
		sb.WriteString(intToString(e.Position))
		sb.WriteByte(' ')
		sb.WriteString(e.Digest)
		sb.WriteByte('\n')
	}
	now := c.now()
	rs.Status = RoundSucceeded
	rs.EndedAt = now
	rs.Manifest = &Manifest{
		Round:        rs.Round,
		Entries:      entries,
		Digest:       sha256Hex(sb.String()),
		CompletedAt:  now,
		Notification: newID("notif"),
	}
}

// Abort 主动中止一轮检查点。roundNo 传 0 表示中止任务的最新轮。
// 重复中止同一轮是幂等的（返回既有失败视图）；轮次若是其他终态则返回 ErrRoundTerminal。
func (c *Coordinator) Abort(taskID string, roundNo int64, reason string) (*Checkpoint, error) {
	if reason == "" {
		reason = "aborted"
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	ts, ok := c.tasks[taskID]
	if !ok {
		return nil, ErrTaskNotFound
	}
	if roundNo == 0 {
		roundNo = ts.NextRound - 1
	}
	rs, ok := ts.Rounds[roundNo]
	if !ok {
		return nil, ErrRoundNotFound
	}

	switch {
	case rs.Status == RoundAborted:
		return checkpointView(taskID, rs), nil // 幂等
	case rs.Status.IsTerminal():
		return nil, ErrRoundTerminal
	default:
		rs.Status = RoundAborted
		rs.Reason = reason
		rs.EndedAt = c.now()
		if err := c.persistLocked(); err != nil {
			rs.Status = RoundPending
			rs.Reason = ""
			rs.EndedAt = time.Time{}
			return nil, err
		}
		return checkpointView(taskID, rs), nil
	}
}

// AdvanceTimeouts 推进所有任务的截止时间：now 时刻已到期且仍在等待的最新轮
// 被一次性标记为 TIMED_OUT。超时与上报/中止在同一把互斥锁下竞争，先提交者胜，
// 因此终态唯一。返回本轮被标记超时的检查点（可能为空）。
func (c *Coordinator) AdvanceTimeouts(now time.Time) ([]*Checkpoint, error) {
	if now.IsZero() {
		now = c.now()
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	var timedOut []*Checkpoint
	var changedList []*roundState
	for taskID, ts := range c.tasks {
		latest := ts.NextRound - 1
		rs, ok := ts.Rounds[latest]
		if !ok || rs.Status != RoundPending {
			continue
		}
		if rs.Deadline.IsZero() || now.Before(rs.Deadline) {
			continue
		}
		rs.Status = RoundTimedOut
		rs.Reason = "timed out"
		rs.EndedAt = now
		timedOut = append(timedOut, checkpointView(taskID, rs))
		changedList = append(changedList, rs)
	}
	if len(changedList) > 0 {
		if err := c.persistLocked(); err != nil {
			for _, rs := range changedList {
				rs.Status = RoundPending
				rs.Reason = ""
				rs.EndedAt = time.Time{}
			}
			return nil, err
		}
	}
	return timedOut, nil
}

// LatestCheckpoint 返回任务最新的“可恢复”检查点，即最新一轮 SUCCEEDED 的不可变
// 清单。中止/超时的失败轮次不会成为恢复依据。不存在成功轮次时返回 (nil, nil)。
func (c *Coordinator) LatestCheckpoint(taskID string) (*Checkpoint, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ts, ok := c.tasks[taskID]
	if !ok {
		return nil, ErrTaskNotFound
	}
	for r := ts.NextRound - 1; r >= 1; r-- {
		rs := ts.Rounds[r]
		if rs != nil && rs.Status == RoundSucceeded {
			return checkpointView(taskID, rs), nil
		}
	}
	return nil, nil
}

// GetRound 返回指定轮次的检查点视图。
func (c *Coordinator) GetRound(taskID string, roundNo int64) (*Checkpoint, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ts, ok := c.tasks[taskID]
	if !ok {
		return nil, ErrTaskNotFound
	}
	rs, ok := ts.Rounds[roundNo]
	if !ok {
		return nil, ErrRoundNotFound
	}
	return checkpointView(taskID, rs), nil
}

// GetLatestRound 返回任务最新一轮（任意状态）的检查点视图。
func (c *Coordinator) GetLatestRound(taskID string) (*Checkpoint, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ts, ok := c.tasks[taskID]
	if !ok {
		return nil, ErrTaskNotFound
	}
	if ts.NextRound <= 1 {
		return nil, ErrRoundNotFound
	}
	rs, ok := ts.Rounds[ts.NextRound-1]
	if !ok {
		return nil, ErrRoundNotFound
	}
	return checkpointView(taskID, rs), nil
}

// History 按轮次升序返回任务全部检查点历史（含进行中、成功与失败轮次）。
func (c *Coordinator) History(taskID string) ([]*Checkpoint, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ts, ok := c.tasks[taskID]
	if !ok {
		return nil, ErrTaskNotFound
	}
	out := make([]*Checkpoint, 0, len(ts.Rounds))
	for r := int64(1); r < ts.NextRound; r++ {
		if rs, ok := ts.Rounds[r]; ok {
			out = append(out, checkpointView(taskID, rs))
		}
	}
	return out, nil
}

// checkpointView 构造深拷贝视图，确保调用方无法修改内部状态、查询永远一致。
func checkpointView(taskID string, rs *roundState) *Checkpoint {
	cp := &Checkpoint{
		TaskID:    taskID,
		Round:     rs.Round,
		Status:    rs.Status,
		Waiting:   append([]string(nil), rs.Waiting...),
		Deadline:  rs.Deadline,
		CreatedAt: rs.CreatedAt,
		EndedAt:   rs.EndedAt,
		Reason:    rs.Reason,
	}
	if rs.Manifest != nil {
		m := &Manifest{
			Round:        rs.Manifest.Round,
			Entries:      append([]ManifestEntry(nil), rs.Manifest.Entries...),
			Digest:       rs.Manifest.Digest,
			CompletedAt:  rs.Manifest.CompletedAt,
			Notification: rs.Manifest.Notification,
		}
		cp.Manifest = m
	}
	return cp
}

func memberIn(set []string, member string) bool {
	for _, m := range set {
		if m == member {
			return true
		}
	}
	return false
}
