package gocheckpointcoordinator

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// completedCheckpoint registers a task, runs one round to completion and
// returns the resulting manifest id.
func completedCheckpoint(t *testing.T, co *Coordinator, taskID string, members []string) string {
	t.Helper()
	mustRegister(t, co, taskID, members...)
	mustStart(t, co, taskID, 0)
	var manifestID string
	for i, m := range members {
		res, err := co.Report(taskID, 1, m, int64((i+1)*100), "digest:"+m)
		if err != nil {
			t.Fatalf("Report(%q): %v", m, err)
		}
		if res.Completed {
			manifestID = res.Manifest.ID
		}
	}
	if manifestID == "" {
		t.Fatalf("round for %q never completed", taskID)
	}
	return manifestID
}

func mustBranch(t *testing.T, co *Coordinator, taskID, parent string, n int, start, end int64) *Branch {
	t.Helper()
	b, err := co.CreateBranch(taskID, parent, n, Range{Start: start, End: end})
	if err != nil {
		t.Fatalf("CreateBranch(%d): %v", n, err)
	}
	return b
}

func branchReport(t *testing.T, co *Coordinator, taskID, branchID string, gen int64, prefix string, tip int64, digest string) *BranchResult {
	t.Helper()
	res, err := co.BranchReport(taskID, branchID, gen, prefix, tip, digest)
	if err != nil {
		t.Fatalf("BranchReport(%s): %v", branchID, err)
	}
	return res
}

func TestCreateBranchRequiresCompletedParent(t *testing.T) {
	co := NewInMemory()
	mustRegister(t, co, "t", "m1", "m2")
	mustStart(t, co, "t", 0)

	if _, err := co.CreateBranch("t", "missing-manifest", 1, Range{0, 10}); !errors.Is(err, ErrParentNotCompleted) {
		t.Fatalf("unknown parent: want ErrParentNotCompleted, got %v", err)
	}
	if _, err := co.Report("t", 1, "m1", 1, "d1"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := co.AbortCheckpoint("t", "stop"); err != nil {
		t.Fatal(err)
	}

	parent := completedCheckpoint(t, co, "done", []string{"m1"})
	if _, err := co.CreateBranch("done", parent, 1, Range{10, 5}); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("inverted range: want ErrInvalidRange, got %v", err)
	}
	b := mustBranch(t, co, "done", parent, 1, 0, 10)
	if b.Generation != 1 || b.Status != BranchActive || b.ParentCheckpoint != parent {
		t.Fatalf("unexpected initial branch: %+v", b)
	}
	if _, err := co.CreateBranch("done", parent, 1, Range{0, 10}); !errors.Is(err, ErrBranchExists) {
		t.Fatalf("duplicate branch number: want ErrBranchExists, got %v", err)
	}
}

func TestBranchReportGenerationAndStaleReceipts(t *testing.T) {
	co := NewInMemory()
	parent := completedCheckpoint(t, co, "t", []string{"m1"})
	b := mustBranch(t, co, "t", parent, 1, 0, 10)

	res := branchReport(t, co, "t", b.ID, 1, "pfx", 5, "tip-5")
	if res.Duplicate || res.Branch.Generation != 2 {
		t.Fatalf("first report should advance to gen 2, got %+v", res)
	}
	res2, err := co.BranchReport("t", b.ID, 2, "pfx", 5, "tip-5")
	if err != nil || !res2.Duplicate || res2.Branch.Generation != 2 {
		t.Fatalf("replay want duplicate, gen 2: err=%v res=%+v", err, res2)
	}
	if _, err := co.BranchReport("t", b.ID, 1, "evil", 99, "old"); !errors.Is(err, ErrStaleBranch) {
		t.Fatalf("old receipt: want ErrStaleBranch, got %v", err)
	}
	got, _ := co.GetBranch("t", b.ID)
	if got.Generation != 2 || got.TipPosition != 5 {
		t.Fatalf("stale receipt mutated branch: %+v", got)
	}
}

func TestMergePrefixAndRangeConflictsAreSpecific(t *testing.T) {
	co := NewInMemory()
	parent := completedCheckpoint(t, co, "t", []string{"m1"})

	a := mustBranch(t, co, "t", parent, 1, 0, 20)
	b := mustBranch(t, co, "t", parent, 2, 10, 30)
	branchReport(t, co, "t", a.ID, 1, "pfx", 10, "ta")
	branchReport(t, co, "t", b.ID, 1, "pfx", 20, "tb")

	req, err := co.RequestMerge("m-1", "t", a.ID, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if req.Status != MergeConflicted || len(req.Conflicts) != 1 {
		t.Fatalf("want one conflict, got %+v", req)
	}
	cf := req.Conflicts[0]
	if cf.Kind != ConflictRange || cf.Location != "[10,20)" {
		t.Fatalf("want exact overlap [10,20), got %+v", cf)
	}
	ga, _ := co.GetBranch("t", a.ID)
	gb, _ := co.GetBranch("t", b.ID)
	if ga.Status != BranchActive || gb.Status != BranchActive {
		t.Fatalf("branches must remain active after failed merge")
	}

	c2 := mustBranch(t, co, "t", parent, 3, 100, 200)
	branchReport(t, co, "t", a.ID, 2, "pfx-A", 12, "ta2")
	branchReport(t, co, "t", c2.ID, 1, "pfx-X", 150, "tc")
	req2, err := co.RequestMerge("m-2", "t", a.ID, c2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if req2.Status != MergeConflicted || len(req2.Conflicts) != 1 || req2.Conflicts[0].Kind != ConflictPrefix {
		t.Fatalf("want single prefix conflict, got %+v", req2)
	}
	if req2.Conflicts[0].Location != "pfx-A != pfx-X" {
		t.Fatalf("conflict must pinpoint both prefixes: %+v", req2.Conflicts[0])
	}
}

func TestMergeSucceedsPublishesSingleMainlineAndRejectsOldReceipts(t *testing.T) {
	co := NewInMemory()
	parent := completedCheckpoint(t, co, "t", []string{"m1"})
	a := mustBranch(t, co, "t", parent, 1, 0, 10)
	b := mustBranch(t, co, "t", parent, 2, 10, 20)
	branchReport(t, co, "t", a.ID, 1, "pfx", 5, "ta")
	branchReport(t, co, "t", b.ID, 1, "pfx", 15, "tb")

	req, err := co.RequestMerge("m-1", "t", a.ID, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if req.Status != MergeMerged || req.MainlineVersionID == "" {
		t.Fatalf("merge failed: %+v", req)
	}
	v, err := co.GetMainlineVersion(req.MainlineVersionID)
	if err != nil {
		t.Fatal(err)
	}
	if v.Generation != 1 || len(v.Ranges) != 2 || v.Ranges[0].Start != 0 || v.Ranges[1].Start != 10 {
		t.Fatalf("unexpected mainline version: %+v", v)
	}

	lineage, err := co.GetLineage("t")
	if err != nil {
		t.Fatal(err)
	}
	if lineage.Mainline == nil || lineage.Mainline.ID != v.ID {
		t.Fatalf("lineage must expose the effective mainline version")
	}
	if lineage.Entries[0].Kind != "checkpoint" {
		t.Fatalf("want checkpoint first, got %s", lineage.Entries[0].Kind)
	}
	for _, e := range lineage.Entries {
		if e.Kind == "branch" && e.MainlineVersionID != v.ID {
			t.Fatalf("merged branch must reference the mainline version: %+v", e)
		}
	}

	if _, err := co.BranchReport("t", a.ID, 2, "pfx", 6, "late"); !errors.Is(err, ErrBranchMerged) {
		t.Fatalf("report after merge: want ErrBranchMerged, got %v", err)
	}
	other := mustBranch(t, co, "t", parent, 9, 50, 60)
	branchReport(t, co, "t", other.ID, 1, "pfx", 55, "to")
	if _, err := co.RequestMerge("m-x", "t", a.ID, other.ID); !errors.Is(err, ErrBranchMerged) {
		t.Fatalf("merge with merged branch: want ErrBranchMerged, got %v", err)
	}
}

func TestDuplicateMergeIDIsIdempotentAndDetectsChanges(t *testing.T) {
	co := NewInMemory()
	parent := completedCheckpoint(t, co, "t", []string{"m1"})
	a := mustBranch(t, co, "t", parent, 1, 0, 10)
	b := mustBranch(t, co, "t", parent, 2, 10, 20)
	branchReport(t, co, "t", a.ID, 1, "pfx", 5, "ta")
	branchReport(t, co, "t", b.ID, 1, "pfx", 15, "tb")

	first, err := co.RequestMerge("dup", "t", a.ID, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	again, err := co.RequestMerge("dup", "t", b.ID, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.MainlineVersionID != first.MainlineVersionID || again.Status != first.Status {
		t.Fatalf("resubmit must return the original result")
	}

	c := mustBranch(t, co, "t", parent, 3, 100, 110)
	d := mustBranch(t, co, "t", parent, 4, 105, 120)
	branchReport(t, co, "t", c.ID, 1, "pfx", 102, "tc")
	branchReport(t, co, "t", d.ID, 1, "pfx", 110, "td")
	bad, err := co.RequestMerge("bad", "t", c.ID, d.ID)
	if err != nil || bad.Status != MergeConflicted {
		t.Fatalf("want conflicted merge, err=%v req=%+v", err, bad)
	}
	badAgain, err := co.RequestMerge("bad", "t", c.ID, d.ID)
	if err != nil || len(badAgain.Conflicts) != 1 || badAgain.Conflicts[0].Location != "[105,110)" {
		t.Fatalf("conflicted merge must replay exact conflicts: %v %+v", err, badAgain)
	}

	g := mustBranch(t, co, "t", parent, 7, 300, 310)
	h := mustBranch(t, co, "t", parent, 8, 310, 320)
	branchReport(t, co, "t", g.ID, 1, "pfx-g", 305, "tg")
	branchReport(t, co, "t", h.ID, 1, "pfx-h", 315, "th")
	moved, err := co.RequestMerge("move", "t", g.ID, h.ID)
	if err != nil || moved.Status != MergeConflicted {
		t.Fatalf("prefix mismatch merge: %v %+v", err, moved)
	}
	branchReport(t, co, "t", g.ID, 2, "pfx-h", 306, "tg2")
	if _, err := co.RequestMerge("move", "t", g.ID, h.ID); !errors.Is(err, ErrMergeRequestChanged) {
		t.Fatalf("advanced branch resubmit: want ErrMergeRequestChanged, got %v", err)
	}
}

func TestFailedMergeRetainsBranchesForLaterMerge(t *testing.T) {
	co := NewInMemory()
	parent := completedCheckpoint(t, co, "t", []string{"m1"})
	a := mustBranch(t, co, "t", parent, 1, 0, 10)
	b := mustBranch(t, co, "t", parent, 2, 20, 30)
	branchReport(t, co, "t", a.ID, 1, "pfx-a", 5, "ta")
	branchReport(t, co, "t", b.ID, 1, "pfx-b", 25, "tb")

	failed, err := co.RequestMerge("fail", "t", a.ID, b.ID)
	if err != nil || failed.Status != MergeConflicted {
		t.Fatalf("want failed merge kept on record, got %v %+v", err, failed)
	}

	// Both branches survive, advance to agree on the prefix and retry under a
	// new merge id: this succeeds and publishes the mainline version.
	branchReport(t, co, "t", a.ID, 2, "pfx", 6, "ta2")
	branchReport(t, co, "t", b.ID, 2, "pfx", 26, "tb2")
	ok, err := co.RequestMerge("retry", "t", a.ID, b.ID)
	if err != nil || ok.Status != MergeMerged {
		t.Fatalf("retry after fixing prefix: %v %+v", err, ok)
	}
	lineage, _ := co.GetLineage("t")
	if len(lineage.Merges) != 2 || lineage.Merges[0].Status != MergeConflicted || lineage.Merges[1].Status != MergeMerged {
		t.Fatalf("lineage must show failed then successful merge: %+v", lineage.Merges)
	}
	if lineage.Mainline == nil || lineage.Mainline.MergeID != "retry" {
		t.Fatalf("effective mainline must come from the retry: %+v", lineage.Mainline)
	}
}

func TestConcurrentMergeRacePublishesExactlyOneMainline(t *testing.T) {
	co := NewInMemory()
	parent := completedCheckpoint(t, co, "t", []string{"m1"})

	const pairs = 40
	type pair struct{ a, b string }
	var pairsList []pair
	for i := 0; i < pairs; i++ {
		base := int64(i * 10)
		a := mustBranch(t, co, "t", parent, 2*i+1, base, base+5)
		b := mustBranch(t, co, "t", parent, 2*i+2, base+5, base+10)
		branchReport(t, co, "t", a.ID, 1, "pfx", base+2, "x")
		branchReport(t, co, "t", b.ID, 1, "pfx", base+7, "y")
		pairsList = append(pairsList, pair{a.ID, b.ID})
	}

	var wg sync.WaitGroup
	errs := make(chan error, pairs*2)
	merged := make(chan string, pairs)
	for i, p := range pairsList {
		wg.Add(1)
		go func(i int, p pair) {
			defer wg.Done()
			id := fmt.Sprintf("merge-%d", i)
			req, err := co.RequestMerge(id, "t", p.a, p.b)
			if err != nil {
				errs <- err
				return
			}
			if req.Status == MergeMerged {
				merged <- req.MainlineVersionID
			}
		}(i, p)
	}

	// Concurrent late receipts against branches racing with merges: none may
	// mutate a branch once it is merged.
	for i, p := range pairsList {
		wg.Add(1)
		go func(i int, p pair) {
			defer wg.Done()
			_, err := co.BranchReport("t", p.a, 1, "pfx", 3, "late")
			if err != nil && !errors.Is(err, ErrStaleBranch) && !errors.Is(err, ErrBranchMerged) {
				errs <- err
			}
		}(i, p)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent merge/report error: %v", err)
	}

	lineage, err := co.GetLineage("t")
	if err != nil {
		t.Fatal(err)
	}
	if lineage.Mainline == nil || lineage.Mainline.Generation != int64(pairs) {
		t.Fatalf("mainline generation must count exactly %d serialised merges, got %+v", pairs, lineage.Mainline)
	}
	seen := map[string]struct{}{}
	close(merged)
	for id := range merged {
		if _, dup := seen[id]; dup {
			t.Fatalf("mainline version published twice: %s", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != pairs {
		t.Fatalf("want %d distinct mainline versions, got %d", pairs, len(seen))
	}
}

func TestBranchMergeStateSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	co, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	parent := completedCheckpoint(t, co, "t", []string{"m1"})
	a := mustBranch(t, co, "t", parent, 1, 0, 10)
	b := mustBranch(t, co, "t", parent, 2, 10, 20)
	branchReport(t, co, "t", a.ID, 1, "pfx", 5, "ta")
	branchReport(t, co, "t", b.ID, 1, "pfx", 15, "tb")
	req, err := co.RequestMerge("m", "t", a.ID, b.ID)
	if err != nil {
		t.Fatal(err)
	}

	reopened, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	lineage, err := reopened.GetLineage("t")
	if err != nil {
		t.Fatal(err)
	}
	if lineage.Mainline == nil || lineage.Mainline.ID != req.MainlineVersionID {
		t.Fatalf("mainline version must survive restart")
	}
	if _, err := reopened.RequestMerge("m", "t", a.ID, b.ID); err != nil {
		t.Fatalf("idempotent merge replay after restart: %v", err)
	}
}
