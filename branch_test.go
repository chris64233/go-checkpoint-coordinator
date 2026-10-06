package gocheckpointcoordinator

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// setupBranchTask registers a task and completes round 1 so branches can be
// forked from it.
func setupBranchTask(t *testing.T, c *Coordinator, taskID string) {
	t.Helper()
	if _, err := c.RegisterTask(taskID, []string{"m1", "m2"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := c.StartCheckpoint(taskID, 0); err != nil {
		t.Fatalf("start: %v", err)
	}
	for _, m := range []string{"m1", "m2"} {
		if _, err := c.Report(taskID, 1, m, 100, "digest-"+m); err != nil {
			t.Fatalf("report: %v", err)
		}
	}
}

func TestCreateBranchRequiresCompletedParent(t *testing.T) {
	c := NewInMemory()
	if _, err := c.RegisterTask("t", []string{"m1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.StartCheckpoint("t", 0); err != nil {
		t.Fatal(err)
	}
	// Round 1 is still pending: no publishable branch may fork from it.
	if _, err := c.CreateBranch("t", 1, 1, 0, 100); !errors.Is(err, ErrParentNotComplete) {
		t.Fatalf("want ErrParentNotComplete, got %v", err)
	}
	if _, err := c.CreateBranch("t", 9, 1, 0, 100); !errors.Is(err, ErrParentNotComplete) {
		t.Fatalf("missing parent: want ErrParentNotComplete, got %v", err)
	}
	if _, err := c.Report("t", 1, "m1", 10, "d"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateBranch("t", 1, 1, 0, 100); err != nil {
		t.Fatalf("create after completion: %v", err)
	}
	if _, err := c.CreateBranch("t", 1, 1, 0, 50); !errors.Is(err, ErrBranchExists) {
		t.Fatalf("want ErrBranchExists, got %v", err)
	}
	if _, err := c.CreateBranch("t", 1, 2, 50, 50); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("want ErrInvalidRange, got %v", err)
	}
}

func TestBranchReportStaleGeneration(t *testing.T) {
	c := NewInMemory()
	setupBranchTask(t, c, "t")
	if _, err := c.CreateBranch("t", 1, 1, 0, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReportBranch("t", 1, 2, 50, "d2"); err != nil {
		t.Fatal(err)
	}
	// Stale receipt must not write back.
	if _, err := c.ReportBranch("t", 1, 1, 10, "d1"); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("want ErrStaleGeneration, got %v", err)
	}
	// Same generation, identical content: idempotent.
	if _, err := c.ReportBranch("t", 1, 2, 50, "d2"); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	// Same generation, different content: conflict.
	if _, err := c.ReportBranch("t", 1, 2, 51, "d2"); !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
	b, err := c.GetBranches("t")
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != 1 || b[0].Generation != 2 || b[0].Position != 50 {
		t.Fatalf("stale receipt leaked into branch: %+v", b)
	}
}

func TestMergeBranchRangeOverlapConflict(t *testing.T) {
	c := NewInMemory()
	setupBranchTask(t, c, "t")
	if _, err := c.CreateBranch("t", 1, 1, 0, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateBranch("t", 1, 2, 50, 150); err != nil {
		t.Fatal(err)
	}
	rec, err := c.MergeBranches("t", "m1", 1, 2)
	if !errors.Is(err, ErrMergeConflict) {
		t.Fatalf("want ErrMergeConflict, got %v", err)
	}
	if rec == nil || len(rec.Conflicts) != 1 {
		t.Fatalf("want one conflict, got %+v", rec)
	}
	cf := rec.Conflicts[0]
	if cf.Kind != "range_overlap" || cf.Position != 50 {
		t.Fatalf("conflict should point at overlap start 50, got %+v", cf)
	}
	// Failed merge keeps both branches open.
	bs, _ := c.GetBranches("t")
	for _, b := range bs {
		if b.Status != BranchOpen {
			t.Fatalf("branch %d should stay open after failed merge", b.BranchNo)
		}
	}
	// No mainline version was published.
	if r, _ := c.GetRound("t"); r.Number != 1 {
		t.Fatalf("failed merge must not publish a mainline round, got %d", r.Number)
	}
}

func TestMergeBranchParentMismatchConflict(t *testing.T) {
	c := NewInMemory()
	setupBranchTask(t, c, "t")
	if _, err := c.StartCheckpoint("t", 0); err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{"m1", "m2"} {
		if _, err := c.Report("t", 2, m, 200, "d"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.CreateBranch("t", 1, 1, 0, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateBranch("t", 2, 2, 100, 200); err != nil {
		t.Fatal(err)
	}
	rec, err := c.MergeBranches("t", "m1", 1, 2)
	if !errors.Is(err, ErrMergeConflict) {
		t.Fatalf("want ErrMergeConflict, got %v", err)
	}
	if rec.Conflicts[0].Kind != "parent_mismatch" || rec.Conflicts[0].Position != 1 {
		t.Fatalf("conflict should point at parent round 1, got %+v", rec.Conflicts[0])
	}
}

func TestMergePublishesSingleMainlineVersion(t *testing.T) {
	c := NewInMemory()
	setupBranchTask(t, c, "t")
	if _, err := c.CreateBranch("t", 1, 1, 0, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateBranch("t", 1, 2, 100, 200); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReportBranch("t", 1, 2, 100, "da"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReportBranch("t", 2, 2, 200, "db"); err != nil {
		t.Fatal(err)
	}
	rec, err := c.MergeBranches("t", "m1", 1, 2)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if rec.Status != MergeApplied || rec.MainlineRound != 2 {
		t.Fatalf("want applied merge at round 2, got %+v", rec)
	}
	mani, err := c.GetLatestRecoverableCheckpoint("t")
	if err != nil || mani == nil || mani.Round != 2 {
		t.Fatalf("mainline should be round 2, got %+v, %v", mani, err)
	}
	if len(mani.Reports) != 2 {
		t.Fatalf("merged manifest should carry both branch reports, got %+v", mani.Reports)
	}
	bs, _ := c.GetBranches("t")
	for _, b := range bs {
		if b.Status != BranchMerged {
			t.Fatalf("branch %d should be merged", b.BranchNo)
		}
	}
	// Merged branches no longer accept receipts.
	if _, err := c.ReportBranch("t", 1, 3, 150, "x"); !errors.Is(err, ErrBranchNotOpen) {
		t.Fatalf("want ErrBranchNotOpen, got %v", err)
	}
}

func TestMergeIdempotentReplayAndKeyConflict(t *testing.T) {
	c := NewInMemory()
	setupBranchTask(t, c, "t")
	mustBranch := func(no, start, end int64) {
		t.Helper()
		if _, err := c.CreateBranch("t", 1, no, start, end); err != nil {
			t.Fatal(err)
		}
	}
	mustBranch(1, 0, 100)
	mustBranch(2, 100, 200)
	mustBranch(3, 200, 300)

	first, err := c.MergeBranches("t", "m1", 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	// Same merge id, same branches and ranges: original result returned.
	again, err := c.MergeBranches("t", "m1", 1, 2)
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if again.MainlineRound != first.MainlineRound || again.ManifestID != first.ManifestID {
		t.Fatalf("replay diverged: %+v vs %+v", again, first)
	}
	if r, _ := c.GetRound("t"); r.Number != 2 {
		t.Fatalf("replay must not publish another round, got %d", r.Number)
	}
	// Same merge id, different branches: conflict.
	if _, err := c.MergeBranches("t", "m1", 1, 3); !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
}

func TestMergeRaceSingleMainlineVersion(t *testing.T) {
	c := NewInMemory()
	setupBranchTask(t, c, "t")
	for i := int64(1); i <= 4; i++ {
		if _, err := c.CreateBranch("t", 1, i, i*100, i*100+100); err != nil {
			t.Fatal(err)
		}
		if _, err := c.ReportBranch("t", i, 2, i*100+50, "d"); err != nil {
			t.Fatal(err)
		}
	}
	// Concurrent merges of disjoint branch pairs plus concurrent branch
	// reports: merges are serialised, so at most one mainline version per
	// merge and reports never resurrect merged branches.
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 2; i++ {
		a, b := int64(1+2*i), int64(2+2*i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.MergeBranches("t", fmt.Sprintf("m-%d-%d", a, b), a, b); err != nil {
				errs <- err
			}
		}()
	}
	for i := int64(1); i <= 4; i++ {
		wg.Add(1)
		go func(no int64) {
			defer wg.Done()
			// Stale or post-merge receipts may fail; they must not corrupt
			// state. Only unexpected errors are collected.
			_, err := c.ReportBranch("t", no, 3, no*100+60, "d3")
			if err != nil && !errors.Is(err, ErrBranchNotOpen) {
				errs <- fmt.Errorf("branch %d report: %w", no, err)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	hist, err := c.GetCheckpointHistory("t")
	if err != nil {
		t.Fatal(err)
	}
	completed := 0
	for _, h := range hist {
		if h.Round.Status == RoundCompleted {
			completed++
		}
	}
	if completed != 3 { // round 1 + two merges
		t.Fatalf("want 3 completed rounds, got %d", completed)
	}
	merges, _ := c.GetMergeHistory("t")
	if len(merges) != 2 {
		t.Fatalf("want 2 merge records, got %d", len(merges))
	}
	seen := map[int64]bool{}
	for _, m := range merges {
		if m.Status != MergeApplied || seen[m.MainlineRound] {
			t.Fatalf("duplicate or failed mainline version: %+v", merges)
		}
		seen[m.MainlineRound] = true
	}
}

func TestMergeBlockedWhileRoundPending(t *testing.T) {
	c := NewInMemory()
	setupBranchTask(t, c, "t")
	if _, err := c.CreateBranch("t", 1, 1, 0, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateBranch("t", 1, 2, 100, 200); err != nil {
		t.Fatal(err)
	}
	if _, err := c.StartCheckpoint("t", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.MergeBranches("t", "m1", 1, 2); !errors.Is(err, ErrRoundOpen) {
		t.Fatalf("want ErrRoundOpen, got %v", err)
	}
}

func TestBranchQueriesStableOrderAndPersistence(t *testing.T) {
	path := statePath(t)
	c, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	setupBranchTask(t, c, "t")
	if _, err := c.CreateBranch("t", 1, 2, 100, 200); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateBranch("t", 1, 1, 0, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReportBranch("t", 1, 3, 90, "d"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.MergeBranches("t", "m1", 1, 2); err != nil {
		t.Fatal(err)
	}

	// Reopen from disk and verify query output.
	c2, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	bs, err := c2.GetBranches("t")
	if err != nil {
		t.Fatal(err)
	}
	if len(bs) != 2 || bs[0].BranchNo != 2 || bs[1].BranchNo != 1 {
		t.Fatalf("branches not sorted by generation: %+v", bs)
	}
	if bs[0].ParentRound != 1 || bs[1].ParentRound != 1 {
		t.Fatalf("parent checkpoint missing: %+v", bs)
	}
	merges, err := c2.GetMergeHistory("t")
	if err != nil {
		t.Fatal(err)
	}
	if len(merges) != 1 || merges[0].MainlineRound != 2 || merges[0].Status != MergeApplied {
		t.Fatalf("merge history missing mainline version: %+v", merges)
	}
}

func statePath(t *testing.T) string {
	t.Helper()
	return t.TempDir() + "/state.json"
}
