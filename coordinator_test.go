package gocheckpointcoordinator

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

func newTestCoordinator(t *testing.T) (*Coordinator, *fakeClock) {
	t.Helper()
	fc := &fakeClock{t: time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)}
	c, err := NewCoordinator("", WithClock(fc.now))
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	return c, fc
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

func (f *fakeClock) advance(d time.Duration) time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
	return f.t
}

func mustRegister(t *testing.T, c *Coordinator, taskID string, members ...string) {
	t.Helper()
	if _, err := c.RegisterTask(taskID, "task-"+taskID, members); err != nil {
		t.Fatalf("RegisterTask(%q): %v", taskID, err)
	}
}

func rep(taskID string, round int64, member string, pos int64, digest string) Report {
	return Report{TaskID: taskID, Round: round, MemberID: member, Position: pos, Digest: digest}
}

// ---- 任务登记 ----

func TestRegisterTask_FreezesMembersSorted(t *testing.T) {
	c, _ := newTestCoordinator(t)
	if _, err := c.RegisterTask("t1", "n", []string{"b", "a", "c"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	task, err := c.GetTask("t1")
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	want := []string{"a", "b", "c"}
	if fmt.Sprint(task.Members) != fmt.Sprint(want) {
		t.Fatalf("members = %v, want sorted %v", task.Members, want)
	}

	if _, err := c.RegisterTask("t1", "dup", []string{"a"}); !errors.Is(err, ErrTaskExists) {
		t.Fatalf("dup register err = %v, want ErrTaskExists", err)
	}
	for _, bad := range [][]string{nil, {}, {""}, {"a", "a"}} {
		if _, err := c.RegisterTask("bad-"+fmt.Sprint(len(bad)), "n", bad); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("members %v: err = %v, want ErrInvalidArgument", bad, err)
		}
	}
}

// ---- 轮次：递增、冻结、不可覆盖 ----

func TestStartCheckpoint_RoundsIncrementAndCannotOverwrite(t *testing.T) {
	c, _ := newTestCoordinator(t)
	mustRegister(t, c, "t1", "a", "b")

	r1, err := c.StartCheckpoint("t1", time.Time{})
	if err != nil {
		t.Fatalf("start 1: %v", err)
	}
	if r1.Round != 1 || r1.Status != RoundPending {
		t.Fatalf("r1 = %+v", r1)
	}
	if got := fmt.Sprint(r1.Waiting); got != "[a b]" {
		t.Fatalf("frozen waiting = %s", got)
	}

	if _, err := c.StartCheckpoint("t1", time.Time{}); !errors.Is(err, ErrRoundInProgress) {
		t.Fatalf("overwrite err = %v, want ErrRoundInProgress", err)
	}

	// 上一轮中止后才能发起新一轮，轮次号递增，且新一轮仍按登记成员冻结。
	if _, err := c.Abort("t1", 0, ""); err != nil {
		t.Fatalf("abort: %v", err)
	}
	r2, err := c.StartCheckpoint("t1", time.Time{})
	if err != nil {
		t.Fatalf("start 2: %v", err)
	}
	if r2.Round != 2 {
		t.Fatalf("round = %d, want 2", r2.Round)
	}
}

func TestFrozenSetUnaffectedByMembershipChange(t *testing.T) {
	// 成员集合在登记时固定；没有提供变更 API（成员“临时退出”属于运行期行为），
	// 非冻结成员上报一律被拒，而冻结成员即使逻辑上已退出仍必须完成才能成功。
	c, _ := newTestCoordinator(t)
	mustRegister(t, c, "t1", "a", "b", "c")
	r, _ := c.StartCheckpoint("t1", time.Time{})

	// “临时退出”的成员 c 仍在冻结集合中：缺它不能完成。
	if _, err := c.Report(rep("t1", r.Round, "a", 1, "da")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Report(rep("t1", r.Round, "b", 1, "db")); err != nil {
		t.Fatal(err)
	}
	cur, err := c.GetRound("t1", r.Round)
	if err != nil {
		t.Fatal(err)
	}
	if cur.Status != RoundPending {
		t.Fatalf("status = %s, want PENDING (frozen member c still required)", cur.Status)
	}

	// 集合外成员（新加入/冒名）不能改变完成条件。
	if _, err := c.Report(rep("t1", r.Round, "newcomer", 1, "dx")); !errors.Is(err, ErrUnknownMember) {
		t.Fatalf("outsider report err = %v, want ErrUnknownMember", err)
	}

	// c 即便“临时退出”，回来上报仍被接受并完成本轮。
	res, err := c.Report(rep("t1", r.Round, "c", 1, "dc"))
	if err != nil {
		t.Fatalf("frozen member report: %v", err)
	}
	if !res.Completed || res.Status != RoundSucceeded {
		t.Fatalf("res = %+v, want completed+succeeded", res)
	}
}

// ---- 上报：幂等 / 冲突 / 陈旧 ----

func TestReport_IdempotentAndConflict(t *testing.T) {
	c, _ := newTestCoordinator(t)
	mustRegister(t, c, "t1", "a", "b")
	r, _ := c.StartCheckpoint("t1", time.Time{})

	first, err := c.Report(rep("t1", r.Round, "a", 10, "digest-a"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Replayed || first.Completed {
		t.Fatalf("first report = %+v", first)
	}

	// 相同内容重复上报：返回原结果（回放），不产生副作用。
	replay, err := c.Report(rep("t1", r.Round, "a", 10, "digest-a"))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replay.Replayed || replay.Completed {
		t.Fatalf("replay = %+v", replay)
	}

	// 位置不同 → 冲突。
	if _, err := c.Report(rep("t1", r.Round, "a", 11, "digest-a")); !errors.Is(err, ErrConflict) {
		t.Fatalf("position conflict err = %v", err)
	}
	// 摘要不同 → 冲突。
	if _, err := c.Report(rep("t1", r.Round, "a", 10, "digest-a-2")); !errors.Is(err, ErrConflict) {
		t.Fatalf("digest conflict err = %v", err)
	}

	// 冲突后首次内容仍然有效：b 上报即可完成，清单采用首次内容。
	if _, err := c.Report(rep("t1", r.Round, "b", 5, "digest-b")); err != nil {
		t.Fatal(err)
	}
	cp, _ := c.GetRound("t1", r.Round)
	if cp.Status != RoundSucceeded {
		t.Fatalf("status = %s", cp.Status)
	}
	got := map[string]ManifestEntry{}
	for _, e := range cp.Manifest.Entries {
		got[e.MemberID] = e
	}
	if got["a"].Position != 10 || got["a"].Digest != "digest-a" {
		t.Fatalf("manifest does not preserve first report: %+v", got["a"])
	}
}

func TestReport_LateReportForOldRoundRejected(t *testing.T) {
	c, _ := newTestCoordinator(t)
	mustRegister(t, c, "t1", "a", "b")
	r1, _ := c.StartCheckpoint("t1", time.Time{})

	// 第 1 轮：a 已上报，然后中止（失败轮）。
	if _, err := c.Report(rep("t1", r1.Round, "a", 1, "da1")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Abort("t1", r1.Round, "give up"); err != nil {
		t.Fatal(err)
	}

	// 发起第 2 轮。
	r2, err := c.StartCheckpoint("t1", time.Time{})
	if err != nil {
		t.Fatalf("start 2: %v", err)
	}

	// b 对旧轮（1）的迟到上报：不能混入第 2 轮。
	if _, err := c.Report(rep("t1", 1, "b", 1, "db1")); !errors.Is(err, ErrStaleRound) {
		t.Fatalf("late report err = %v, want ErrStaleRound", err)
	}
	// a 对旧轮的迟到上报若内容不同 → 冲突；相同内容 → 回放原结果，均不影响第 2 轮。
	if _, err := c.Report(rep("t1", 1, "a", 2, "da1-new")); !errors.Is(err, ErrConflict) {
		t.Fatalf("late conflicting report err = %v, want ErrConflict", err)
	}
	replay, err := c.Report(rep("t1", 1, "a", 1, "da1"))
	if err != nil {
		t.Fatalf("late replay: %v", err)
	}
	if !replay.Replayed || replay.Status != RoundAborted {
		t.Fatalf("late replay = %+v", replay)
	}

	// 第 2 轮仍等待 a 和 b，各需上报一次。
	if _, err := c.Report(rep("t1", r2.Round, "a", 2, "da2")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Report(rep("t1", r2.Round, "b", 2, "db2")); err != nil {
		t.Fatal(err)
	}
	cp, _ := c.GetRound("t1", 2)
	if cp.Status != RoundSucceeded || len(cp.Manifest.Entries) != 2 {
		t.Fatalf("round 2 = %+v", cp)
	}
}

// ---- 完成：不可变清单 + 唯一通知 + 原子可见性 ----

func TestCompletion_ManifestImmutableAndNotificationUnique(t *testing.T) {
	c, _ := newTestCoordinator(t)
	mustRegister(t, c, "t1", "a", "b")
	r, _ := c.StartCheckpoint("t1", time.Time{})

	c.Report(rep("t1", r.Round, "a", 1, "da"))
	res, err := c.Report(rep("t1", r.Round, "b", 2, "db"))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Completed {
		t.Fatal("last report must complete the round")
	}
	cp := res.Checkpoint
	if cp == nil || cp.Manifest == nil {
		t.Fatal("completed result must carry manifest")
	}
	if len(cp.Manifest.Entries) != 2 || cp.Manifest.Digest == "" || cp.Manifest.Notification == "" {
		t.Fatalf("bad manifest: %+v", cp.Manifest)
	}
	digest1, notif1 := cp.Manifest.Digest, cp.Manifest.Notification
	completedAt := cp.Manifest.CompletedAt

	// 重复查询 / 重复上报得到的是同一份不可变清单与同一个通知。
	cp2, _ := c.GetRound("t1", r.Round)
	if cp2.Manifest.Digest != digest1 || cp2.Manifest.Notification != notif1 {
		t.Fatal("manifest/notification must be stable")
	}
	replay, _ := c.Report(rep("t1", r.Round, "a", 1, "da"))
	if replay.Completed || !replay.Replayed {
		t.Fatalf("replay after success = %+v", replay)
	}
	if replay.Checkpoint.Manifest.Notification != notif1 {
		t.Fatal("replay must return the same unique notification")
	}
	if !replay.Checkpoint.Manifest.CompletedAt.Equal(completedAt) {
		t.Fatal("completed_at must be immutable")
	}

	// 成功后拒绝中止，终态唯一。
	if _, err := c.Abort("t1", r.Round, "too late"); !errors.Is(err, ErrRoundTerminal) {
		t.Fatalf("abort succeeded round err = %v", err)
	}
	// 调用方篡改返回的切片不能影响内部状态。
	cp2.Waiting[0] = "hacked"
	cp2.Manifest.Entries[0].Digest = "hacked"
	again, _ := c.GetRound("t1", r.Round)
	if again.Waiting[0] != "a" || again.Manifest.Entries[0].Digest != "da" {
		t.Fatal("internal state mutated through returned view")
	}
}

func TestCompletion_NoPartialVisibility(t *testing.T) {
	// 外部查询在成功之前只能看到 PENDING，绝不会看到“完成了一半的清单”。
	c, _ := newTestCoordinator(t)
	mustRegister(t, c, "t1", "a", "b", "c")
	r, _ := c.StartCheckpoint("t1", time.Time{})

	c.Report(rep("t1", r.Round, "a", 1, "da"))
	c.Report(rep("t1", r.Round, "b", 1, "db"))
	cp, _ := c.GetRound("t1", r.Round)
	if cp.Status != RoundPending || cp.Manifest != nil {
		t.Fatalf("partial state leaked: %+v", cp)
	}
	latest, _ := c.LatestCheckpoint("t1")
	if latest != nil {
		t.Fatal("recoverable checkpoint must not exist before full completion")
	}
}

func TestCompletion_ConcurrentLastReports(t *testing.T) {
	c, _ := newTestCoordinator(t)
	const n = 16
	members := make([]string, n)
	for i := range members {
		members[i] = fmt.Sprintf("m%02d", i)
	}
	mustRegister(t, c, "t1", members...)
	r, _ := c.StartCheckpoint("t1", time.Time{})

	var wg sync.WaitGroup
	completions := make(chan *ReportResult, n)
	errs := make(chan error, n)
	wg.Add(n)
	for i, m := range members {
		i, m := i, m
		go func() {
			defer wg.Done()
			res, err := c.Report(rep("t1", r.Round, m, int64(i), "d-"+m))
			if err != nil {
				errs <- err
				return
			}
			if res.Completed {
				completions <- res
			}
		}()
	}
	wg.Wait()
	close(completions)
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent report: %v", err)
	}
	var winners []*ReportResult
	for res := range completions {
		winners = append(winners, res)
	}
	if len(winners) != 1 {
		t.Fatalf("exactly one reporter must complete the round, got %d", len(winners))
	}
	cp, _ := c.GetRound("t1", r.Round)
	if cp.Status != RoundSucceeded {
		t.Fatalf("status = %s", cp.Status)
	}
	if len(cp.Manifest.Entries) != n {
		t.Fatalf("manifest entries = %d, want %d", len(cp.Manifest.Entries), n)
	}
	notifs := map[string]struct{}{}
	for i := 0; i < 50; i++ {
		q, _ := c.GetRound("t1", r.Round)
		if q.Status != RoundSucceeded {
			t.Fatalf("concurrent query saw non-succeeded status %s", q.Status)
		}
		if q.Manifest == nil {
			t.Fatal("concurrent query saw success without manifest")
		}
		notifs[q.Manifest.Notification] = struct{}{}
	}
	if len(notifs) != 1 {
		t.Fatalf("observed %d distinct notifications, want exactly 1", len(notifs))
	}
}

// ---- 中止 / 超时 / 末次上报竞争：唯一终态 ----

func TestTerminalRace_ReportVsAbort(t *testing.T) {
	const iterations = 200
	for i := 0; i < iterations; i++ {
		c, _ := newTestCoordinator(t)
		mustRegister(t, c, "t1", "a", "b")
		r, _ := c.StartCheckpoint("t1", time.Time{})
		c.Report(rep("t1", r.Round, "a", 1, "da"))

		var wg sync.WaitGroup
		wg.Add(2)
		var abortErr, reportErr error
		var reportRes *ReportResult
		go func() { defer wg.Done(); _, abortErr = c.Abort("t1", r.Round, "race") }()
		go func() {
			defer wg.Done()
			reportRes, reportErr = c.Report(rep("t1", r.Round, "b", 1, "db"))
		}()
		wg.Wait()

		cp, _ := c.GetRound("t1", r.Round)
		switch cp.Status {
		case RoundSucceeded:
			if abortErr == nil || !errors.Is(abortErr, ErrRoundTerminal) {
				t.Fatalf("iter %d: success but abort err = %v", i, abortErr)
			}
			if reportErr != nil || !reportRes.Completed {
				t.Fatalf("iter %d: success but report = %+v err=%v", i, reportRes, reportErr)
			}
		case RoundAborted:
			if abortErr != nil {
				t.Fatalf("iter %d: abort won but err = %v", i, abortErr)
			}
			if reportErr == nil {
				t.Fatalf("iter %d: abort won but report accepted: %+v", i, reportRes)
			}
		default:
			t.Fatalf("iter %d: non-terminal status %s", i, cp.Status)
		}
	}
}

func TestTerminalRace_ReportVsTimeout(t *testing.T) {
	const iterations = 200
	for i := 0; i < iterations; i++ {
		c, fc := newTestCoordinator(t)
		mustRegister(t, c, "t1", "a", "b")
		deadline := fc.now().Add(time.Minute)
		r, _ := c.StartCheckpoint("t1", deadline)
		c.Report(rep("t1", r.Round, "a", 1, "da"))
		at := fc.advance(time.Minute + time.Second) // 越过截止时间

		var wg sync.WaitGroup
		wg.Add(2)
		var reportErr error
		var reportRes *ReportResult
		go func() { defer wg.Done(); reportRes, reportErr = c.Report(rep("t1", r.Round, "b", 1, "db")) }()
		go func() { defer wg.Done(); c.AdvanceTimeouts(at) }()
		wg.Wait()

		cp, _ := c.GetRound("t1", r.Round)
		switch cp.Status {
		case RoundSucceeded:
			if reportErr != nil || !reportRes.Completed {
				t.Fatalf("iter %d: success but report err=%v res=%+v", i, reportErr, reportRes)
			}
		case RoundTimedOut:
			if reportErr == nil {
				t.Fatalf("iter %d: timeout won but report accepted: %+v", i, reportRes)
			}
		default:
			t.Fatalf("iter %d: unexpected status %s", i, cp.Status)
		}
	}
}

func TestTimeout_AdvanceAndRecoveryExcludesFailures(t *testing.T) {
	c, fc := newTestCoordinator(t)
	mustRegister(t, c, "t1", "a", "b")
	deadline := fc.now().Add(30 * time.Second)
	r1, _ := c.StartCheckpoint("t1", deadline)

	// 未到截止时间：不超时。
	if cps, _ := c.AdvanceTimeouts(fc.advance(10 * time.Second)); len(cps) != 0 {
		t.Fatalf("premature timeout: %+v", cps)
	}
	cur, _ := c.GetRound("t1", r1.Round)
	if cur.Status != RoundPending {
		t.Fatalf("status = %s before deadline", cur.Status)
	}

	// 到达截止时间：标记 TIMED_OUT，可重新发起轮次。
	cps, _ := c.AdvanceTimeouts(fc.advance(30 * time.Second))
	if len(cps) != 1 || cps[0].Status != RoundTimedOut {
		t.Fatalf("timeout result = %+v", cps)
	}
	if latest, _ := c.LatestCheckpoint("t1"); latest != nil {
		t.Fatal("timed-out round must not be recoverable")
	}

	// 超时后迟到的末次上报被拒，不能复活失败轮次。
	if _, err := c.Report(rep("t1", r1.Round, "a", 1, "da")); !errors.Is(err, ErrRoundTerminal) {
		t.Fatalf("post-timeout report err = %v, want ErrRoundTerminal", err)
	}
	if _, err := c.Abort("t1", r1.Round, "x"); !errors.Is(err, ErrRoundTerminal) {
		t.Fatalf("abort timed-out round err = %v, want ErrRoundTerminal", err)
	}

	// 新一轮成功后，LatestCheckpoint 指向成功轮而不是任何失败轮。
	r2, _ := c.StartCheckpoint("t1", fc.now().Add(time.Hour))
	c.Report(rep("t1", r2.Round, "a", 9, "da2"))
	c.Report(rep("t1", r2.Round, "b", 9, "db2"))
	latest, err := c.LatestCheckpoint("t1")
	if err != nil {
		t.Fatal(err)
	}
	if latest == nil || latest.Round != 2 || latest.Status != RoundSucceeded {
		t.Fatalf("latest recoverable = %+v, want round 2 succeeded", latest)
	}
}

func TestTimeout_OnlyExpiresLatestRound(t *testing.T) {
	c, fc := newTestCoordinator(t)
	mustRegister(t, c, "t1", "a")
	// 第 1 轮无截止时间且保持 PENDING 是不允许直接开第 2 轮的，因此先成功第 1 轮。
	r1, _ := c.StartCheckpoint("t1", fc.now().Add(time.Hour))
	c.Report(rep("t1", r1.Round, "a", 1, "d"))
	// 第 2 轮短截止时间并超时。
	r2, _ := c.StartCheckpoint("t1", fc.now().Add(time.Second))
	at := fc.advance(2 * time.Second)
	cps, _ := c.AdvanceTimeouts(at)
	if len(cps) != 1 || cps[0].Round != r2.Round {
		t.Fatalf("timeout must only apply to latest round: %+v", cps)
	}
	// 第 1 轮的成功清单仍可恢复。
	latest, _ := c.LatestCheckpoint("t1")
	if latest == nil || latest.Round != r1.Round {
		t.Fatalf("latest recoverable = %+v, want round %d", latest, r1.Round)
	}
}

// ---- 历史与恢复点 ----

func TestHistory_OrderedAndIncludesAllStates(t *testing.T) {
	c, _ := newTestCoordinator(t)
	mustRegister(t, c, "t1", "a", "b")

	r1, _ := c.StartCheckpoint("t1", time.Time{})
	c.Report(rep("t1", r1.Round, "a", 1, "da"))
	c.Abort("t1", r1.Round, "x")
	r2, _ := c.StartCheckpoint("t1", time.Time{})
	c.Report(rep("t1", r2.Round, "a", 1, "da"))
	c.Report(rep("t1", r2.Round, "b", 1, "db"))

	hist, err := c.History("t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 2 || hist[0].Round != 1 || hist[1].Round != 2 {
		t.Fatalf("history order = %+v", hist)
	}
	if hist[0].Status != RoundAborted || hist[1].Status != RoundSucceeded {
		t.Fatalf("history statuses = %s, %s", hist[0].Status, hist[1].Status)
	}
	if hist[0].Manifest != nil || hist[1].Manifest == nil {
		t.Fatal("only succeeded round may carry a manifest")
	}
}

func TestLatestCheckpoint_NotFoundAndSucceeds(t *testing.T) {
	c, _ := newTestCoordinator(t)
	if _, err := c.LatestCheckpoint("missing"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("err = %v", err)
	}
	mustRegister(t, c, "t1", "a")
	if cp, _ := c.LatestCheckpoint("t1"); cp != nil {
		t.Fatal("no rounds yet: want nil")
	}
	r, _ := c.StartCheckpoint("t1", time.Time{})
	c.Report(rep("t1", r.Round, "a", 1, "d"))
	cp, _ := c.LatestCheckpoint("t1")
	if cp == nil || cp.Round != 1 {
		t.Fatalf("latest = %+v", cp)
	}
}

// ---- 持久化：原子写盘与重启恢复 ----

func TestFileStore_RestoreAfterRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	c, err := NewCoordinator(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.RegisterTask("t1", "n", []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	r1, _ := c.StartCheckpoint("t1", time.Time{})
	c.Report(rep("t1", r1.Round, "a", 100, "da"))
	// 中止第 1 轮并成功第 2 轮，覆盖所有终态的持久化。
	c.Abort("t1", r1.Round, "stop")
	r2, _ := c.StartCheckpoint("t1", time.Time{})
	c.Report(rep("t1", r2.Round, "a", 200, "da2"))
	c.Report(rep("t1", r2.Round, "b", 200, "db2"))
	cpBefore, _ := c.GetRound("t1", r2.Round)
	digestBefore, notifBefore := cpBefore.Manifest.Digest, cpBefore.Manifest.Notification

	// 用同一路径重新打开：完整恢复。
	c2, err := NewCoordinator(path)
	if err != nil {
		t.Fatal(err)
	}
	task, err := c2.GetTask("t1")
	if err != nil {
		t.Fatalf("task lost: %v", err)
	}
	if !sort.StringsAreSorted(task.Members) {
		t.Fatal("members not restored sorted")
	}
	hist, _ := c2.History("t1")
	if len(hist) != 2 {
		t.Fatalf("history len = %d", len(hist))
	}
	if hist[0].Status != RoundAborted || hist[1].Status != RoundSucceeded {
		t.Fatalf("restored statuses = %s, %s", hist[0].Status, hist[1].Status)
	}
	latest, _ := c2.LatestCheckpoint("t1")
	if latest == nil || latest.Round != 2 || latest.Manifest == nil {
		t.Fatalf("restored latest = %+v", latest)
	}
	if latest.Manifest.Digest != digestBefore {
		t.Fatalf("manifest digest changed across restart: %s vs %s", latest.Manifest.Digest, digestBefore)
	}
	if latest.Manifest.Notification != notifBefore {
		t.Fatalf("notification changed across restart: %s vs %s", latest.Manifest.Notification, notifBefore)
	}

	// 恢复后不能覆盖轮次、轮次号继续递增。
	if _, err := c2.StartCheckpoint("t1", time.Time{}); err != nil {
		t.Fatalf("round 3 should start: %v", err)
	}
}

func TestFileStore_AtomicWriteLeavesNoTempFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	c, err := NewCoordinator(path)
	if err != nil {
		t.Fatal(err)
	}
	mustRegister(t, c, "t1", "a")
	c.StartCheckpoint("t1", time.Time{})

	matches, _ := filepath.Glob(filepath.Join(dir, "*"))
	for _, m := range matches {
		if filepath.Ext(m) == ".tmp" {
			t.Fatalf("temp file left behind: %s", m)
		}
	}
}
