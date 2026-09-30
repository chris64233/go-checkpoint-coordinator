package gocheckpointcoordinator

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// fakeClock is a concurrency-safe controllable Clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestCoordinator(c *fakeClock) *Coordinator {
	co, err := NewWithStore(newMemoryStore(), &Options{Clock: c.now})
	if err != nil {
		panic(err)
	}
	return co
}

func mustRegister(t *testing.T, co *Coordinator, taskID string, members ...string) {
	t.Helper()
	if _, err := co.RegisterTask(taskID, members); err != nil {
		t.Fatalf("RegisterTask(%q): %v", taskID, err)
	}
}

func mustStart(t *testing.T, co *Coordinator, taskID string, timeout time.Duration) int64 {
	t.Helper()
	r, err := co.StartCheckpoint(taskID, timeout)
	if err != nil {
		t.Fatalf("StartCheckpoint(%q): %v", taskID, err)
	}
	return r.Number
}

func reportAll(t *testing.T, co *Coordinator, taskID string, round int64, members []string) {
	t.Helper()
	for i, m := range members {
		if _, err := co.Report(taskID, round, m, int64(100+i), "digest-"+m); err != nil {
			t.Fatalf("Report(%q, round %d, %q): %v", taskID, round, m, err)
		}
	}
}

// ---- registration ----------------------------------------------------------

func TestRegisterTaskValidatesInput(t *testing.T) {
	co := newTestCoordinator(newFakeClock())

	if _, err := co.RegisterTask("t1", nil); !errors.Is(err, ErrEmptyMembers) {
		t.Fatalf("empty members: want ErrEmptyMembers, got %v", err)
	}
	mustRegister(t, co, "t1", "a", "b")
	if _, err := co.RegisterTask("t1", []string{"a"}); !errors.Is(err, ErrTaskExists) {
		t.Fatalf("duplicate task: want ErrTaskExists, got %v", err)
	}

	task, err := co.GetTask("t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(task.Members) != 2 || task.Members[0] != "a" || task.Members[1] != "b" {
		t.Fatalf("members not sorted: %v", task.Members)
	}
}

func TestRegisterTaskDeduplicatesMembers(t *testing.T) {
	co := newTestCoordinator(newFakeClock())
	task, err := co.RegisterTask("t1", []string{"c", "a", "a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a", "b", "c"}
	if fmt.Sprint(task.Members) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", task.Members, want)
	}
}

func TestStartCheckpointUnknownTask(t *testing.T) {
	co := newTestCoordinator(newFakeClock())
	if _, err := co.StartCheckpoint("nope", 0); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("want ErrTaskNotFound, got %v", err)
	}
}

// ---- frozen member set -----------------------------------------------------

func TestFrozenSetIsolatedFromCallerSlices(t *testing.T) {
	co := newTestCoordinator(newFakeClock())
	members := []string{"a", "b", "c"}
	task, err := co.RegisterTask("t1", members)
	if err != nil {
		t.Fatal(err)
	}
	// The caller mutating returned state must not affect coordination.
	task.Members[0] = "hacked"
	members[1] = "hacked"

	r, err := co.StartCheckpoint("t1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(r.FrozenMembers) != "[a b c]" {
		t.Fatalf("frozen set leaked caller mutation: %v", r.FrozenMembers)
	}

	// A member that temporarily stops participating does not shrink the
	// completion condition: all three frozen members are still required.
	reportAll(t, co, "t1", 1, []string{"a", "b"})
	cur, err := co.GetRound("t1")
	if err != nil {
		t.Fatal(err)
	}
	if cur.Status != RoundPending {
		t.Fatalf("round must stay pending without member c, got %s", cur.Status)
	}
	res, err := co.Report("t1", 1, "c", 102, "digest-c")
	if err != nil {
		t.Fatal(err)
	}
	if !res.Completed {
		t.Fatal("round completes only after the last frozen member reports")
	}
}

func TestCannotStartWhileRoundOpen(t *testing.T) {
	co := newTestCoordinator(newFakeClock())
	mustRegister(t, co, "t1", "a")
	n1 := mustStart(t, co, "t1", 0)
	if n1 != 1 {
		t.Fatalf("first round number = %d, want 1", n1)
	}
	if _, err := co.StartCheckpoint("t1", 0); !errors.Is(err, ErrRoundOpen) {
		t.Fatalf("want ErrRoundOpen, got %v", err)
	}
}

func TestRoundNumbersIncrementAfterTerminalRound(t *testing.T) {
	co := newTestCoordinator(newFakeClock())
	mustRegister(t, co, "t1", "a")

	if n := mustStart(t, co, "t1", 0); n != 1 {
		t.Fatalf("got %d", n)
	}
	if _, _, err := co.AbortCheckpoint("t1", "stop"); err != nil {
		t.Fatal(err)
	}
	if n := mustStart(t, co, "t1", 0); n != 2 {
		t.Fatalf("round after abort = %d, want 2", n)
	}
	if _, err := co.Report("t1", 2, "a", 1, "d"); err != nil {
		t.Fatal(err)
	}
	if n := mustStart(t, co, "t1", 0); n != 3 {
		t.Fatalf("round after completion = %d, want 3", n)
	}
}

// ---- report validation -----------------------------------------------------

func TestReportUnknownMember(t *testing.T) {
	co := newTestCoordinator(newFakeClock())
	mustRegister(t, co, "t1", "a")
	mustStart(t, co, "t1", 0)
	if _, err := co.Report("t1", 1, "outsider", 0, "d"); !errors.Is(err, ErrUnknownMember) {
		t.Fatalf("want ErrUnknownMember, got %v", err)
	}
}

func TestReportStaleRoundRejected(t *testing.T) {
	co := newTestCoordinator(newFakeClock())
	mustRegister(t, co, "t1", "a", "b")
	mustStart(t, co, "t1", 0)

	// Round 1 completes.
	if _, err := co.Report("t1", 1, "a", 1, "da"); err != nil {
		t.Fatal(err)
	}
	if _, err := co.Report("t1", 1, "b", 1, "db"); err != nil {
		t.Fatal(err)
	}
	// Round 2 opens.
	mustStart(t, co, "t1", 0)

	// A late report for the old current round 1 must not be accepted, nor mix
	// into round 2.
	if _, err := co.Report("t1", 1, "a", 2, "da2"); !errors.Is(err, ErrStaleRound) {
		t.Fatalf("late report for old round: want ErrStaleRound, got %v", err)
	}
	if _, err := co.Report("t1", 99, "a", 2, "da2"); !errors.Is(err, ErrStaleRound) {
		t.Fatalf("report for future round: want ErrStaleRound, got %v", err)
	}
}

func TestReportConflictAndIdempotency(t *testing.T) {
	co := newTestCoordinator(newFakeClock())
	mustRegister(t, co, "t1", "a", "b")
	mustStart(t, co, "t1", 0)

	first, err := co.Report("t1", 1, "a", 10, "digest-a")
	if err != nil {
		t.Fatal(err)
	}
	if first.AlreadyReported {
		t.Fatal("first report must not be flagged as duplicate")
	}

	// Identical repeat: idempotent, returns the original outcome.
	second, err := co.Report("t1", 1, "a", 10, "digest-a")
	if err != nil {
		t.Fatal(err)
	}
	if !second.AlreadyReported {
		t.Fatal("identical repeat must be flagged AlreadyReported")
	}

	// Different content: conflict. Position change.
	if _, err := co.Report("t1", 1, "a", 11, "digest-a"); !errors.Is(err, ErrConflict) {
		t.Fatalf("position change: want ErrConflict, got %v", err)
	}
	// Digest change.
	if _, err := co.Report("t1", 1, "a", 10, "digest-a2"); !errors.Is(err, ErrConflict) {
		t.Fatalf("digest change: want ErrConflict, got %v", err)
	}

	// Finish the round; the repeated identical report after completion returns
	// the same manifest and notification.
	finish, err := co.Report("t1", 1, "b", 20, "digest-b")
	if err != nil {
		t.Fatal(err)
	}
	if !finish.Completed || finish.Manifest == nil || finish.Notification == nil {
		t.Fatal("expected completion with manifest and notification")
	}
	again, err := co.Report("t1", 1, "a", 10, "digest-a")
	if err != nil {
		t.Fatal(err)
	}
	if !again.Completed || again.Manifest == nil || again.Notification == nil {
		t.Fatal("duplicate report on completed round must return the outcome")
	}
	if again.Manifest.ID != finish.Manifest.ID {
		t.Fatal("duplicate report returned a different manifest id")
	}
	if again.Notification.ID != finish.Notification.ID {
		t.Fatal("duplicate report returned a different notification id")
	}
}

// ---- atomic completion -----------------------------------------------------

func TestCompletionProducesOneManifestAndNotification(t *testing.T) {
	co := newTestCoordinator(newFakeClock())
	members := []string{"a", "b", "c"}
	mustRegister(t, co, "t1", members...)
	mustStart(t, co, "t1", 0)

	res, err := co.Report("t1", 1, "c", 30, "dc")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := co.Report("t1", 1, "a", 10, "da"); err != nil {
		t.Fatal(err)
	}
	last, err := co.Report("t1", 1, "b", 20, "db")
	if err != nil {
		t.Fatal(err)
	}
	if !last.Completed {
		t.Fatal("last report must complete the round")
	}
	if res.Completed {
		t.Fatal("earlier report must not complete the round")
	}

	notes, err := co.Notifications("t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(notes) != 1 {
		t.Fatalf("notifications = %d, want 1", len(notes))
	}
	if notes[0].Status != RoundCompleted || notes[0].ManifestID != last.Manifest.ID {
		t.Fatalf("unexpected notification: %+v", notes[0])
	}

	mani, err := co.GetLatestRecoverableCheckpoint("t1")
	if err != nil {
		t.Fatal(err)
	}
	if mani.ID != last.Manifest.ID || len(mani.Reports) != 3 {
		t.Fatalf("recoverable checkpoint mismatch: %+v", mani)
	}
	// The manifest is immutable: mutating the returned value has no effect.
	mani.Reports = mani.Reports[:1]
	again, err := co.GetLatestRecoverableCheckpoint("t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Reports) != 3 {
		t.Fatal("manifest returned to callers must be an immutable copy")
	}
}

func TestConcurrentLastReportsCompleteExactlyOnce(t *testing.T) {
	const iterations = 100
	const memberCount = 8
	members := make([]string, memberCount)
	for i := range members {
		members[i] = fmt.Sprintf("m%d", i)
	}

	for iter := 0; iter < iterations; iter++ {
		co := newTestCoordinator(newFakeClock())
		mustRegister(t, co, "task", members...)
		mustStart(t, co, "task", 0)

		// Reader continuously checks the no-partial-results invariant: a
		// recoverable checkpoint always contains every frozen member.
		stop := make(chan struct{})
		var readerWG sync.WaitGroup
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				mani, err := co.GetLatestRecoverableCheckpoint("task")
				if err != nil {
					t.Errorf("reader: %v", err)
					return
				}
				if mani != nil && len(mani.Reports) != memberCount {
					t.Errorf("partial checkpoint visible: %d/%d reports", len(mani.Reports), memberCount)
					return
				}
				r, err := co.GetRound("task")
				if err == nil && r.Status == RoundCompleted && len(r.Reports) != memberCount {
					t.Errorf("completed round visible with %d/%d reports", len(r.Reports), memberCount)
					return
				}
			}
		}()

		var wg sync.WaitGroup
		start := make(chan struct{})
		completions := make(chan bool, memberCount)
		errs := make(chan error, memberCount)
		for i, m := range members {
			wg.Add(1)
			go func(member string, idx int) {
				defer wg.Done()
				<-start
				res, err := co.Report("task", 1, member, int64(idx), "d-"+member)
				if err != nil {
					errs <- err
					return
				}
				completions <- res.Completed
			}(m, i)
		}
		close(start)
		wg.Wait()
		close(stop)
		readerWG.Wait()
		close(errs)
		close(completions)
		for err := range errs {
			t.Fatalf("concurrent report: %v", err)
		}
		completedCount := 0
		for c := range completions {
			if c {
				completedCount++
			}
		}
		if completedCount != 1 {
			t.Fatalf("iteration %d: %d reporters observed completion, want exactly 1", iter, completedCount)
		}

		notes, err := co.Notifications("task")
		if err != nil {
			t.Fatal(err)
		}
		if len(notes) != 1 || notes[0].Status != RoundCompleted {
			t.Fatalf("iteration %d: notifications = %+v", iter, notes)
		}
		mani, err := co.GetLatestRecoverableCheckpoint("task")
		if err != nil {
			t.Fatal(err)
		}
		if mani == nil || len(mani.Reports) != memberCount {
			t.Fatalf("iteration %d: manifest incomplete: %+v", iter, mani)
		}
	}
}

// ---- terminal-state races --------------------------------------------------

func TestAbortRacesWithLastReport(t *testing.T) {
	const iterations = 200
	members := []string{"a", "b", "c"}

	for iter := 0; iter < iterations; iter++ {
		co := newTestCoordinator(newFakeClock())
		mustRegister(t, co, "task", members...)
		mustStart(t, co, "task", 0)
		// All but the last frozen member are already in.
		reportAll(t, co, "task", 1, members[:len(members)-1])

		start := make(chan struct{})
		var wg sync.WaitGroup
		var reportRes *ReportResult
		var reportErr, abortErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			reportRes, reportErr = co.Report("task", 1, "c", 30, "dc")
		}()
		go func() {
			defer wg.Done()
			<-start
			_, _, abortErr = co.AbortCheckpoint("task", "user abort")
		}()
		close(start)
		wg.Wait()

		reportWon := reportErr == nil && reportRes.Completed
		abortWon := abortErr == nil
		if reportWon == abortWon {
			t.Fatalf("iteration %d: reportWon=%v abortWon=%v (reportErr=%v abortErr=%v)",
				iter, reportWon, abortWon, reportErr, abortErr)
		}

		r, err := co.GetRound("task")
		if err != nil {
			t.Fatal(err)
		}
		notes, err := co.Notifications("task")
		if err != nil {
			t.Fatal(err)
		}
		if len(notes) != 1 {
			t.Fatalf("iteration %d: %d notifications, want 1", iter, len(notes))
		}
		if reportWon {
			if r.Status != RoundCompleted || notes[0].Status != RoundCompleted {
				t.Fatalf("iteration %d: report won but status=%s note=%s", iter, r.Status, notes[0].Status)
			}
			if reportRes.Notification.ID != notes[0].ID {
				t.Fatalf("iteration %d: notification mismatch", iter)
			}
		} else {
			if !errors.Is(reportErr, ErrAlreadyTerminal) {
				t.Fatalf("iteration %d: lost report got %v, want ErrAlreadyTerminal", iter, reportErr)
			}
			if r.Status != RoundAborted || notes[0].Status != RoundAborted {
				t.Fatalf("iteration %d: abort won but status=%s note=%s", iter, r.Status, notes[0].Status)
			}
			if notes[0].ManifestID != "" {
				t.Fatalf("iteration %d: aborted round must not reference a manifest", iter)
			}
			mani, _ := co.GetLatestRecoverableCheckpoint("task")
			if mani != nil {
				t.Fatalf("iteration %d: aborted round must not be recoverable", iter)
			}
		}
	}
}

func TestTimeoutRacesWithLastReport(t *testing.T) {
	const iterations = 200
	members := []string{"a", "b", "c"}

	for iter := 0; iter < iterations; iter++ {
		clk := newFakeClock()
		co := newTestCoordinator(clk)
		mustRegister(t, co, "task", members...)
		mustStart(t, co, "task", 10*time.Millisecond)
		reportAll(t, co, "task", 1, members[:len(members)-1])

		start := make(chan struct{})
		var wg sync.WaitGroup
		var reportRes *ReportResult
		var reportErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			reportRes, reportErr = co.Report("task", 1, "c", 30, "dc")
		}()
		go func() {
			defer wg.Done()
			<-start
			clk.advance(time.Hour)
			_, _, _ = co.AdvanceTimeouts()
		}()
		close(start)
		wg.Wait()

		r, err := co.GetRound("task")
		if err != nil {
			t.Fatal(err)
		}
		notes, err := co.Notifications("task")
		if err != nil {
			t.Fatal(err)
		}
		if len(notes) != 1 {
			t.Fatalf("iteration %d: %d notifications, want 1", iter, len(notes))
		}

		switch r.Status {
		case RoundCompleted:
			if reportErr != nil || !reportRes.Completed {
				t.Fatalf("iteration %d: completed but report err=%v res=%+v", iter, reportErr, reportRes)
			}
			if notes[0].Status != RoundCompleted {
				t.Fatalf("iteration %d: notification status %s", iter, notes[0].Status)
			}
		case RoundTimedOut:
			if !errors.Is(reportErr, ErrAlreadyTerminal) {
				t.Fatalf("iteration %d: timed out but report err=%v", iter, reportErr)
			}
			if notes[0].Status != RoundTimedOut || notes[0].ManifestID != "" {
				t.Fatalf("iteration %d: bad timeout notification %+v", iter, notes[0])
			}
		default:
			t.Fatalf("iteration %d: unexpected status %s", iter, r.Status)
		}
	}
}

// ---- timeout behaviour -----------------------------------------------------

func TestLazyTimeoutOnReport(t *testing.T) {
	clk := newFakeClock()
	co := newTestCoordinator(clk)
	mustRegister(t, co, "t1", "a")
	mustStart(t, co, "t1", 5*time.Second)

	clk.advance(6 * time.Second)
	if _, err := co.Report("t1", 1, "a", 1, "d"); !errors.Is(err, ErrAlreadyTerminal) {
		t.Fatalf("report after deadline: want ErrAlreadyTerminal, got %v", err)
	}
	r, _ := co.GetRound("t1")
	if r.Status != RoundTimedOut {
		t.Fatalf("status = %s, want timed_out", r.Status)
	}
	// A second timeout transition must not create another notification.
	if _, _, err := co.AdvanceTimeouts(); err != nil {
		t.Fatal(err)
	}
	notes, _ := co.Notifications("t1")
	if len(notes) != 1 || notes[0].Status != RoundTimedOut {
		t.Fatalf("notifications after repeat timeout: %+v", notes)
	}
}

func TestAdvanceTimeoutsMultipleTasks(t *testing.T) {
	clk := newFakeClock()
	co := newTestCoordinator(clk)
	mustRegister(t, co, "t1", "a")
	mustRegister(t, co, "t2", "a")
	mustStart(t, co, "t1", time.Second)
	mustStart(t, co, "t2", 10*time.Second)

	clk.advance(2 * time.Second)
	rounds, notes, err := co.AdvanceTimeouts()
	if err != nil {
		t.Fatal(err)
	}
	if len(rounds) != 1 || rounds[0].TaskID != "t1" || rounds[0].Status != RoundTimedOut {
		t.Fatalf("unexpected transitions: %+v", rounds)
	}
	if len(notes) != 1 {
		t.Fatalf("notes = %d", len(notes))
	}
	r2, _ := co.GetRound("t2")
	if r2.Status != RoundPending {
		t.Fatalf("t2 must stay pending, got %s", r2.Status)
	}
	// After the timeout the task can start a fresh round.
	mustStart(t, co, "t1", 0)
}

func TestAbortTerminalRoundRejected(t *testing.T) {
	co := newTestCoordinator(newFakeClock())
	mustRegister(t, co, "t1", "a")
	mustStart(t, co, "t1", 0)
	if _, _, err := co.AbortCheckpoint("missing", "x"); !errors.Is(err, ErrNoRound) {
		t.Fatalf("abort without round: want ErrNoRound, got %v", err)
	}
	if _, _, err := co.AbortCheckpoint("t1", "x"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := co.AbortCheckpoint("t1", "x"); !errors.Is(err, ErrAlreadyTerminal) {
		t.Fatalf("double abort: want ErrAlreadyTerminal, got %v", err)
	}
	if _, err := co.Report("t1", 1, "a", 1, "d"); !errors.Is(err, ErrAlreadyTerminal) {
		t.Fatalf("report into aborted round: want ErrAlreadyTerminal, got %v", err)
	}
}

// ---- recovery point & history ----------------------------------------------

func TestFailedRoundsAreNotRecoveryPoints(t *testing.T) {
	clk := newFakeClock()
	co := newTestCoordinator(clk)
	members := []string{"a", "b"}
	mustRegister(t, co, "t1", members...)

	// Round 1 completes and is the first recovery point.
	mustStart(t, co, "t1", 0)
	reportAll(t, co, "t1", 1, members)
	first, err := co.GetLatestRecoverableCheckpoint("t1")
	if err != nil {
		t.Fatal(err)
	}
	if first == nil || first.Round != 1 {
		t.Fatalf("latest recoverable = %+v, want round 1", first)
	}

	// Round 2 aborts: recovery point must remain round 1.
	mustStart(t, co, "t1", 0)
	if _, _, err := co.AbortCheckpoint("t1", "boom"); err != nil {
		t.Fatal(err)
	}
	afterAbort, err := co.GetLatestRecoverableCheckpoint("t1")
	if err != nil {
		t.Fatal(err)
	}
	if afterAbort == nil || afterAbort.Round != 1 {
		t.Fatalf("after abort latest recoverable = %+v, want round 1", afterAbort)
	}

	// Round 3 times out: still round 1.
	mustStart(t, co, "t1", time.Second)
	clk.advance(2 * time.Second)
	if _, _, err := co.AdvanceTimeouts(); err != nil {
		t.Fatal(err)
	}
	afterTimeout, err := co.GetLatestRecoverableCheckpoint("t1")
	if err != nil {
		t.Fatal(err)
	}
	if afterTimeout == nil || afterTimeout.Round != 1 {
		t.Fatalf("after timeout latest recoverable = %+v, want round 1", afterTimeout)
	}

	// Round 4 completes: recovery moves forward to round 4.
	mustStart(t, co, "t1", 0)
	reportAll(t, co, "t1", 4, members)
	latest, err := co.GetLatestRecoverableCheckpoint("t1")
	if err != nil {
		t.Fatal(err)
	}
	if latest == nil || latest.Round != 4 {
		t.Fatalf("latest recoverable = %+v, want round 4", latest)
	}
	if latest.ID == first.ID {
		t.Fatal("different completed rounds must produce different manifests")
	}

	history, err := co.GetCheckpointHistory("t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 4 {
		t.Fatalf("history length = %d, want 4", len(history))
	}
	wantStatus := []RoundStatus{RoundCompleted, RoundAborted, RoundTimedOut, RoundCompleted}
	for i, want := range wantStatus {
		if history[i].Round.Number != int64(i+1) || history[i].Round.Status != want {
			t.Fatalf("history[%d] = round %d/%s, want %d/%s", i, history[i].Round.Number, history[i].Round.Status, i+1, want)
		}
		if history[i].Notification == nil {
			t.Fatalf("history[%d] missing notification", i)
		}
	}
	if history[0].Manifest == nil || history[0].Manifest.Round != 1 {
		t.Fatal("history entry for round 1 must carry its manifest")
	}
	if history[1].Manifest != nil {
		t.Fatal("aborted round must not carry a manifest")
	}
}

// ---- persistence -----------------------------------------------------------

func TestFileStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	members := []string{"a", "b", "c"}
	co, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	mustRegister(t, co, "t1", members...)
	mustStart(t, co, "t1", 0)
	if _, err := co.Report("t1", 1, "a", 10, "da"); err != nil {
		t.Fatal(err)
	}
	if _, err := co.Report("t1", 1, "b", 20, "db"); err != nil {
		t.Fatal(err)
	}
	// Reopen mid-round: pending state and partial reports survive.
	co, err = New(path)
	if err != nil {
		t.Fatal(err)
	}
	r, err := co.GetRound("t1")
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != RoundPending || len(r.Reports) != 2 {
		t.Fatalf("after reopen: %+v", r)
	}
	// Duplicate report after reload keeps returning the same accepted content;
	// conflicting content is still rejected.
	if _, err := co.Report("t1", 1, "a", 10, "da"); err != nil {
		t.Fatalf("idempotent report after reload: %v", err)
	}
	if _, err := co.Report("t1", 1, "a", 11, "da"); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict after reload: want ErrConflict, got %v", err)
	}

	finish, err := co.Report("t1", 1, "c", 30, "dc")
	if err != nil {
		t.Fatal(err)
	}
	if !finish.Completed {
		t.Fatal("round must complete after the final frozen member reports")
	}
	manifestID := finish.Manifest.ID

	// A failed round must remain failed across restart.
	if _, err := co.StartCheckpoint("t1", 0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := co.AbortCheckpoint("t1", "restart-test"); err != nil {
		t.Fatal(err)
	}

	co, err = New(path)
	if err != nil {
		t.Fatal(err)
	}
	mani, err := co.GetLatestRecoverableCheckpoint("t1")
	if err != nil {
		t.Fatal(err)
	}
	if mani == nil || mani.ID != manifestID || mani.Round != 1 {
		t.Fatalf("recoverable checkpoint after restart = %+v", mani)
	}
	history, err := co.GetCheckpointHistory("t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 ||
		history[0].Round.Status != RoundCompleted ||
		history[1].Round.Status != RoundAborted {
		t.Fatalf("history after restart = %+v", history)
	}
	notes, err := co.Notifications("t1")
	if err != nil || len(notes) != 2 {
		t.Fatalf("notifications after restart: %v %+v", err, notes)
	}
	// A new round after restart gets the next sequential number and can
	// complete; manifest ids remain content-derived and stable.
	mustStart(t, co, "t1", 0)
	reportAll(t, co, "t1", 3, members)
	latest, _ := co.GetLatestRecoverableCheckpoint("t1")
	if latest.Round != 3 {
		t.Fatalf("latest after round 3 = round %d", latest.Round)
	}
	again, _ := co.GetManifest(manifestID)
	if again == nil || again.ID != manifestID {
		t.Fatal("original manifest must remain addressable after restart")
	}
}

func TestNewFileStoreMissingFileStartsEmpty(t *testing.T) {
	co, err := New(filepath.Join(t.TempDir(), "nested", "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	mustRegister(t, co, "t1", "a")
	if got, err := co.GetLatestRecoverableCheckpoint("t1"); err != nil || got != nil {
		t.Fatalf("fresh coordinator recoverable checkpoint = %+v, %v", got, err)
	}
	hist, err := co.GetCheckpointHistory("t1")
	if err != nil || len(hist) != 0 {
		t.Fatalf("fresh history = %+v, %v", hist, err)
	}
}

// ---- round-2 edge cases ----------------------------------------------------

// While a later round is pending with only part of the frozen set reported,
// the previous completed checkpoint must remain the visible recovery point:
// partially reported progress is never observable as a checkpoint.
func TestPendingRoundDoesNotHidePreviousCheckpoint(t *testing.T) {
	co := newTestCoordinator(newFakeClock())
	members := []string{"a", "b", "c"}
	mustRegister(t, co, "t1", members...)

	mustStart(t, co, "t1", 0)
	reportAll(t, co, "t1", 1, members)
	first, err := co.GetLatestRecoverableCheckpoint("t1")
	if err != nil {
		t.Fatal(err)
	}

	// Round 2 is open with two of three members reported.
	mustStart(t, co, "t1", 0)
	if _, err := co.Report("t1", 2, "a", 1, "d2a"); err != nil {
		t.Fatal(err)
	}
	if _, err := co.Report("t1", 2, "b", 2, "d2b"); err != nil {
		t.Fatal(err)
	}

	latest, err := co.GetLatestRecoverableCheckpoint("t1")
	if err != nil {
		t.Fatal(err)
	}
	if latest == nil || latest.ID != first.ID || latest.Round != 1 {
		t.Fatalf("pending round must not replace the last completed checkpoint: %+v", latest)
	}

	// History exposes the pending round and its partial reports, but never a
	// manifest or a notification for it.
	hist, err := co.GetCheckpointHistory("t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 2 {
		t.Fatalf("history len = %d, want 2", len(hist))
	}
	if hist[1].Round.Status != RoundPending {
		t.Fatalf("history[1] status = %s, want pending", hist[1].Round.Status)
	}
	if hist[1].Manifest != nil {
		t.Fatal("pending round must not carry a manifest")
	}
	if hist[1].Notification != nil {
		t.Fatal("pending round must not carry a notification")
	}
	if got := len(hist[1].Round.Reports); got != 2 {
		t.Fatalf("partial reports = %d, want 2", got)
	}

	// Completing round 2 atomically moves the recovery point forward.
	finish, err := co.Report("t1", 2, "c", 3, "d2c")
	if err != nil {
		t.Fatal(err)
	}
	if !finish.Completed {
		t.Fatal("final report must complete round 2")
	}
	latest, _ = co.GetLatestRecoverableCheckpoint("t1")
	if latest == nil || latest.Round != 2 {
		t.Fatalf("recovery point = %+v, want round 2", latest)
	}
}

// The deadline comparison is inclusive: a report arriving exactly at the
// deadline loses to the timeout, so the report/timeout race has a
// deterministic outcome at the boundary (only one terminal state survives).
func TestDeadlineBoundaryReportLosesToTimeout(t *testing.T) {
	clk := newFakeClock()
	start := clk.now()
	co := newTestCoordinator(clk)
	mustRegister(t, co, "t1", "a")
	mustStart(t, co, "t1", 10*time.Millisecond)

	if got := clk.now().UnixMilli(); got != start.UnixMilli() {
		t.Fatalf("clock moved unexpectedly: %v", got)
	}

	// Exactly at the deadline: timeout wins.
	clk.advance(10 * time.Millisecond)
	if _, err := co.Report("t1", 1, "a", 1, "d"); !errors.Is(err, ErrAlreadyTerminal) {
		t.Fatalf("report at the deadline: want ErrAlreadyTerminal, got %v", err)
	}
	r, _ := co.GetRound("t1")
	if r.Status != RoundTimedOut {
		t.Fatalf("status = %s, want timed_out", r.Status)
	}
	notes, _ := co.Notifications("t1")
	if len(notes) != 1 || notes[0].Status != RoundTimedOut || notes[0].ManifestID != "" {
		t.Fatalf("boundary notification = %+v", notes)
	}

	// A report strictly before the deadline completes the round and survives:
	// a fresh round is needed since the first one is terminal.
	mustStart(t, co, "t1", 10*time.Millisecond)
	clk.advance(-time.Millisecond) // now one millisecond before the new deadline
	res, err := co.Report("t1", 2, "a", 1, "d")
	if err != nil || !res.Completed {
		t.Fatalf("report strictly before deadline must complete: err=%v res=%+v", err, res)
	}
	r2, _ := co.GetRound("t1")
	if r2.Status != RoundCompleted {
		t.Fatalf("status = %s, want completed", r2.Status)
	}
}

// Abort, timeout advancement and the final report all race at once. Whatever
// happens, the round ends with exactly one terminal status, one notification
// and at most one manifest; failed outcomes must never become recovery points.
func TestAbortTimeoutAndLastReportRace(t *testing.T) {
	const iterations = 200
	members := []string{"a", "b", "c"}

	for iter := 0; iter < iterations; iter++ {
		clk := newFakeClock()
		co := newTestCoordinator(clk)
		mustRegister(t, co, "task", members...)
		mustStart(t, co, "task", 10*time.Millisecond)
		// Every member except the last one has already reported.
		reportAll(t, co, "task", 1, members[:len(members)-1])

		start := make(chan struct{})
		var wg sync.WaitGroup
		var reportRes *ReportResult
		var reportErr, abortErr error
		var timedOut []Round
		wg.Add(3)
		go func() {
			defer wg.Done()
			<-start
			reportRes, reportErr = co.Report("task", 1, "c", 30, "dc")
		}()
		go func() {
			defer wg.Done()
			<-start
			_, _, abortErr = co.AbortCheckpoint("task", "user abort")
		}()
		go func() {
			defer wg.Done()
			<-start
			clk.advance(time.Hour)
			timedOut, _, _ = co.AdvanceTimeouts()
		}()
		close(start)
		wg.Wait()

		r, err := co.GetRound("task")
		if err != nil {
			t.Fatal(err)
		}
		if !r.Status.Terminal() {
			t.Fatalf("iteration %d: round not terminal: %s", iter, r.Status)
		}
		notes, err := co.Notifications("task")
		if err != nil {
			t.Fatal(err)
		}
		if len(notes) != 1 || notes[0].Status != r.Status {
			t.Fatalf("iteration %d: status=%s notifications=%+v", iter, r.Status, notes)
		}

		switch r.Status {
		case RoundCompleted:
			if reportErr != nil || reportRes == nil || !reportRes.Completed {
				t.Fatalf("iteration %d: completed but report err=%v res=%+v", iter, reportErr, reportRes)
			}
			if !errors.Is(abortErr, ErrAlreadyTerminal) {
				t.Fatalf("iteration %d: completed but abort err=%v", iter, abortErr)
			}
			if len(timedOut) != 0 {
				t.Fatalf("iteration %d: completed but timeout transitions=%d", iter, len(timedOut))
			}
			if notes[0].ManifestID == "" {
				t.Fatalf("iteration %d: completed without manifest reference", iter)
			}
			mani, err := co.GetLatestRecoverableCheckpoint("task")
			if err != nil || mani == nil || mani.ID != notes[0].ManifestID {
				t.Fatalf("iteration %d: completed round must be recoverable: %+v %v", iter, mani, err)
			}
		case RoundAborted:
			if abortErr != nil {
				t.Fatalf("iteration %d: aborted but abort err=%v", iter, abortErr)
			}
			if len(timedOut) != 0 {
				t.Fatalf("iteration %d: aborted but timeout transitions=%d", iter, len(timedOut))
			}
			if !errors.Is(reportErr, ErrAlreadyTerminal) {
				t.Fatalf("iteration %d: lost report err=%v", iter, reportErr)
			}
			if notes[0].ManifestID != "" {
				t.Fatalf("iteration %d: aborted notification references a manifest", iter)
			}
		case RoundTimedOut:
			// The timeout may win either via AdvanceTimeouts or lazily inside
			// the report call; both paths must leave the other two operations
			// as losers.
			if len(timedOut) > 1 {
				t.Fatalf("iteration %d: %d timeout transitions, want 0 or 1", iter, len(timedOut))
			}
			if !errors.Is(reportErr, ErrAlreadyTerminal) {
				t.Fatalf("iteration %d: lost report err=%v", iter, reportErr)
			}
			if !errors.Is(abortErr, ErrAlreadyTerminal) {
				t.Fatalf("iteration %d: lost abort err=%v", iter, abortErr)
			}
			if notes[0].ManifestID != "" {
				t.Fatalf("iteration %d: timed-out notification references a manifest", iter)
			}
		}
		if r.Status != RoundCompleted {
			if mani, _ := co.GetLatestRecoverableCheckpoint("task"); mani != nil {
				t.Fatalf("iteration %d: failed round must not be recoverable", iter)
			}
		}
	}
}
