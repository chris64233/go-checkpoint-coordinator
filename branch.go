package gocheckpointcoordinator

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
)

// BranchResult is returned by BranchReport.
type BranchResult struct {
	// Duplicate is true when the call replayed the branch's most recent
	// report at the same generation; no state changed.
	Duplicate bool
	// Branch is the branch state after the report.
	Branch Branch
}

// CreateBranch forks a publishable processing branch from a confirmed
// checkpoint.
//
// The branch records its parent checkpoint (the manifest id), the
// caller-supplied branch number (unique per task/parent), its processing range
// and its initial generation (1). A branch can only be created from a
// completed checkpoint: a pending, aborted or timed out parent, or an unknown
// manifest, fails with ErrParentNotCompleted.
func (c *Coordinator) CreateBranch(taskID string, parentManifestID string, branchNumber int, r Range) (*Branch, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.snap.Tasks[taskID]; !ok {
		return nil, ErrTaskNotFound
	}
	if !r.Valid() {
		return nil, ErrInvalidRange
	}
	manifest, err := c.completedManifestLocked(taskID, parentManifestID)
	if err != nil {
		return nil, err
	}
	id := branchID(taskID, manifest.ID, branchNumber)
	if _, exists := c.snap.Branches[id]; exists {
		return nil, ErrBranchExists
	}
	now := c.now().UnixMilli()
	branch := &Branch{
		ID:               id,
		TaskID:           taskID,
		ParentCheckpoint: manifest.ID,
		Number:           branchNumber,
		Range:            r,
		Generation:       1,
		Status:           BranchActive,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	c.snap.Branches[id] = branch
	if err := c.persistLocked(); err != nil {
		delete(c.snap.Branches, id)
		return nil, err
	}
	return cloneBranch(branch), nil
}

// BranchReport advances a branch with a confirmed common-prefix digest and the
// branch's new tip summary.
//
// expectedGeneration must equal the branch's current generation; replaying the
// exact same prefix/tip at that generation is idempotent (Duplicate == true).
// A receipt aimed at an older generation is stale (ErrStaleBranch) and can
// never overwrite newer state. Reports against merged branches fail with
// ErrBranchMerged: once a branch is part of a published mainline version its
// old receipts cannot be written back to the main line.
func (c *Coordinator) BranchReport(taskID string, branchID string, expectedGeneration int64, prefixDigest string, tipPosition int64, tipDigest string) (*BranchResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.snap.Tasks[taskID]; !ok {
		return nil, ErrTaskNotFound
	}
	branch, ok := c.snap.Branches[branchID]
	if !ok || branch.TaskID != taskID {
		return nil, ErrBranchNotFound
	}
	if branch.Status == BranchMerged {
		return nil, ErrBranchMerged
	}
	if expectedGeneration != branch.Generation {
		return nil, ErrStaleBranch
	}

	if branch.Generation > 1 &&
		branch.PrefixDigest == prefixDigest &&
		branch.TipPosition == tipPosition &&
		branch.TipDigest == tipDigest {
		return &BranchResult{Duplicate: true, Branch: *cloneBranch(branch)}, nil
	}

	branch.PrefixDigest = prefixDigest
	branch.TipPosition = tipPosition
	branch.TipDigest = tipDigest
	branch.Generation++
	branch.UpdatedAt = c.now().UnixMilli()
	if err := c.persistLocked(); err != nil {
		c.reloadAfterPersistErrorLocked(err)
		return nil, err
	}
	return &BranchResult{Duplicate: false, Branch: *cloneBranch(branch)}, nil
}

// RequestMerge applies to merge two branches back into the main line under the
// caller-supplied merge id.
//
// Before merging, the common prefix and the non-overlapping ranges are
// reconfirmed:
//
//   - both branches must fork from the same parent checkpoint and have
//     confirmed the same prefix digest;
//   - the two ranges must be disjoint;
//   - every disagreement is returned as a concrete Conflict on the persisted
//     MergeRequest (Status == MergeConflicted) - nothing is silently
//     overwritten, and both branches stay active for a later retry.
//
// The merge id is an idempotency key: resubmitting the exact same branches,
// ranges and generations returns the original MergeRequest; resubmitting the
// id after either branch moved (a new generation) fails with
// ErrMergeRequestChanged. A successful merge publishes exactly one
// MainlineVersion, marks both branches merged and makes the version the task's
// single effective main line.
func (c *Coordinator) RequestMerge(mergeID string, taskID string, branchAID string, branchBID string) (*MergeRequest, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if mergeID == "" {
		return nil, fmt.Errorf("merge id is required")
	}
	if _, ok := c.snap.Tasks[taskID]; !ok {
		return nil, ErrTaskNotFound
	}
	if branchAID == branchBID {
		return nil, ErrMergeSameBranch
	}

	if existing, ok := c.snap.Merges[mergeID]; ok {
		if !c.sameMergeRequestLocked(existing, taskID, branchAID, branchBID) {
			return nil, ErrMergeRequestChanged
		}
		return cloneMergeRequest(existing), nil
	}

	branchA, branchB, err := c.mergeBranchesLocked(taskID, branchAID, branchBID)
	if err != nil {
		return nil, err
	}
	// Store the pair in a canonical order so resubmissions that only swap the
	// argument order are still identical requests.
	if branchA.ID > branchB.ID {
		branchA, branchB = branchB, branchA
	}

	now := c.now().UnixMilli()
	req := &MergeRequest{
		MergeID:          mergeID,
		TaskID:           taskID,
		BranchA:          branchA.ID,
		BranchB:          branchB.ID,
		ParentCheckpoint: branchA.ParentCheckpoint,
		RangeA:           branchA.Range,
		RangeB:           branchB.Range,
		GenerationA:      branchA.Generation,
		GenerationB:      branchB.Generation,
		PrefixA:          branchA.PrefixDigest,
		PrefixB:          branchB.PrefixDigest,
		CreatedAt:        now,
	}

	conflicts := validateMergeLocked(branchA, branchB)
	if len(conflicts) > 0 {
		req.Status = MergeConflicted
		req.Conflicts = conflicts
		c.snap.Merges[mergeID] = req
		if err := c.persistLocked(); err != nil {
			delete(c.snap.Merges, mergeID)
			return nil, err
		}
		// Failed merges keep both branches untouched and available for a
		// later merge under a new merge id.
		return cloneMergeRequest(req), nil
	}

	seq := c.snap.MergeSeq[taskID] + 1
	version := &MainlineVersion{
		ID:               mainlineVersionID(taskID, mergeID, branchA, branchB),
		TaskID:           taskID,
		ParentCheckpoint: branchA.ParentCheckpoint,
		MergeID:          mergeID,
		BranchA:          branchA.ID,
		BranchB:          branchB.ID,
		Ranges:           sortedRanges(branchA.Range, branchB.Range),
		Generation:       seq,
		CreatedAt:        now,
	}
	c.snap.MainlineVersions[version.ID] = version
	req.Status = MergeMerged
	req.MainlineVersionID = version.ID

	branchA.Status = BranchMerged
	branchA.MergeID = mergeID
	branchB.Status = BranchMerged
	branchB.MergeID = mergeID

	c.snap.Merges[mergeID] = req
	c.snap.MergeSeq[taskID] = seq
	c.snap.MainlineHead[taskID] = version.ID

	if err := c.persistLocked(); err != nil {
		c.reloadAfterPersistErrorLocked(err)
		return nil, err
	}
	return cloneMergeRequest(req), nil
}

// GetBranch returns a branch by id.
func (c *Coordinator) GetBranch(taskID string, branchID string) (*Branch, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.snap.Tasks[taskID]; !ok {
		return nil, ErrTaskNotFound
	}
	b, ok := c.snap.Branches[branchID]
	if !ok || b.TaskID != taskID {
		return nil, ErrBranchNotFound
	}
	return cloneBranch(b), nil
}

// GetMerge returns a merge request record by its idempotency key.
func (c *Coordinator) GetMerge(mergeID string) (*MergeRequest, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	req, ok := c.snap.Merges[mergeID]
	if !ok {
		return nil, fmt.Errorf("merge not found: %s", mergeID)
	}
	return cloneMergeRequest(req), nil
}

// GetMainlineVersion returns a mainline version by id.
func (c *Coordinator) GetMainlineVersion(versionID string) (*MainlineVersion, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.snap.MainlineVersions[versionID]
	if !ok {
		return nil, fmt.Errorf("mainline version not found: %s", versionID)
	}
	return cloneMainlineVersion(v), nil
}

// LineageEntry is one node in a task's checkpoint/branch lineage.
type LineageEntry struct {
	// Kind is "checkpoint" or "branch".
	Kind string
	// Generation orders nodes stably: checkpoints sort by round number,
	// branches by their current generation.
	Generation int64
	// Checkpoint is present when Kind == "checkpoint".
	Checkpoint *Manifest
	// Branch is present when Kind == "branch".
	Branch *Branch
	// MainlineVersionID is non-empty when the node was folded into a
	// published mainline version.
	MainlineVersionID string
}

// Lineage is the stable, generation-ordered view of a task's checkpoints,
// branches, conflicts and final main line.
type Lineage struct {
	TaskID string
	// Entries are sorted by (generation, kind, id) so the order is stable
	// across queries and restarts.
	Entries []LineageEntry
	// Merges lists every merge request (successful and failed), in submission
	// order, each carrying its conflict locations.
	Merges []MergeRequest
	// Mainline is the task's currently effective mainline version, or nil when
	// no merge has succeeded yet.
	Mainline *MainlineVersion
}

// GetLineage returns the parent/child checkpoint tree, branch ranges, merge
// conflicts and the final mainline version of a task, ordered stably by
// generation.
func (c *Coordinator) GetLineage(taskID string) (*Lineage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.snap.Tasks[taskID]; !ok {
		return nil, ErrTaskNotFound
	}

	out := &Lineage{TaskID: taskID}
	for _, r := range c.snap.Rounds {
		if r.TaskID != taskID || r.ManifestID == "" {
			continue
		}
		out.Entries = append(out.Entries, LineageEntry{
			Kind:       "checkpoint",
			Generation: r.Number,
			Checkpoint: cloneManifest(c.snap.Manifests[r.ManifestID]),
		})
	}
	for _, b := range c.snap.Branches {
		if b.TaskID != taskID {
			continue
		}
		entry := LineageEntry{
			Kind:       "branch",
			Generation: b.Generation,
			Branch:     cloneBranch(b),
		}
		if b.Status == BranchMerged {
			if req := c.mergeForBranchLocked(taskID, b.ID); req != nil {
				entry.MainlineVersionID = req.MainlineVersionID
			}
		}
		out.Entries = append(out.Entries, entry)
	}
	sort.SliceStable(out.Entries, func(i, j int) bool {
		if out.Entries[i].Generation != out.Entries[j].Generation {
			return out.Entries[i].Generation < out.Entries[j].Generation
		}
		if out.Entries[i].Kind != out.Entries[j].Kind {
			return out.Entries[i].Kind == "branch"
		}
		return lineageNodeID(out.Entries[i]) < lineageNodeID(out.Entries[j])
	})

	var reqs []*MergeRequest
	for _, req := range c.snap.Merges {
		if req.TaskID == taskID {
			reqs = append(reqs, req)
		}
	}
	sort.Slice(reqs, func(i, j int) bool { return reqs[i].CreatedAt < reqs[j].CreatedAt })
	for _, req := range reqs {
		out.Merges = append(out.Merges, *cloneMergeRequest(req))
	}
	if headID := c.snap.MainlineHead[taskID]; headID != "" {
		out.Mainline = cloneMainlineVersion(c.snap.MainlineVersions[headID])
	}
	return out, nil
}

// ---- internal helpers (caller must hold c.mu) ----

func (c *Coordinator) completedManifestLocked(taskID string, manifestID string) (*Manifest, error) {
	manifest, ok := c.snap.Manifests[manifestID]
	if !ok || manifest.TaskID != taskID {
		return nil, ErrParentNotCompleted
	}
	for _, r := range c.snap.Rounds {
		if r.TaskID == taskID && r.ManifestID == manifestID {
			if r.Status != RoundCompleted {
				return nil, ErrParentNotCompleted
			}
			return manifest, nil
		}
	}
	return nil, ErrParentNotCompleted
}

func (c *Coordinator) mergeBranchesLocked(taskID string, aID string, bID string) (*Branch, *Branch, error) {
	a, ok := c.snap.Branches[aID]
	if !ok || a.TaskID != taskID {
		return nil, nil, ErrBranchNotFound
	}
	b, ok := c.snap.Branches[bID]
	if !ok || b.TaskID != taskID {
		return nil, nil, ErrBranchNotFound
	}
	if a.Status == BranchMerged || b.Status == BranchMerged {
		return nil, nil, ErrBranchMerged
	}
	return a, b, nil
}

// validateMergeLocked reconfirms the common prefix and non-overlapping ranges
// of two merge candidates. Every disagreement becomes a concrete Conflict; it
// never silently picks one side.
func validateMergeLocked(a *Branch, b *Branch) []Conflict {
	var conflicts []Conflict

	if a.ParentCheckpoint != b.ParentCheckpoint {
		conflicts = append(conflicts, Conflict{
			Kind:     ConflictPrefix,
			BranchA:  a.ID,
			BranchB:  b.ID,
			Location: a.ParentCheckpoint + " != " + b.ParentCheckpoint,
			Detail:   "branches fork from different parent checkpoints",
		})
	}
	if a.PrefixDigest == "" || b.PrefixDigest == "" {
		conflicts = append(conflicts, Conflict{
			Kind:     ConflictPrefix,
			BranchA:  a.ID,
			BranchB:  b.ID,
			Location: "prefix=<unconfirmed>",
			Detail:   "a branch has not confirmed its common prefix",
		})
	} else if a.PrefixDigest != b.PrefixDigest {
		conflicts = append(conflicts, Conflict{
			Kind:     ConflictPrefix,
			BranchA:  a.ID,
			BranchB:  b.ID,
			Location: a.PrefixDigest + " != " + b.PrefixDigest,
			Detail:   "confirmed common-prefix digests diverge",
		})
	}

	if a.Range.Overlaps(b.Range) {
		lo := maxInt64(a.Range.Start, b.Range.Start)
		hi := minInt64(a.Range.End, b.Range.End)
		conflicts = append(conflicts, Conflict{
			Kind:     ConflictRange,
			BranchA:  a.ID,
			BranchB:  b.ID,
			Location: fmt.Sprintf("[%d,%d)", lo, hi),
			Detail: fmt.Sprintf("branch ranges overlap: [%d,%d) vs [%d,%d)",
				a.Range.Start, a.Range.End, b.Range.Start, b.Range.End),
		})
	}
	return conflicts
}

func (c *Coordinator) mergeForBranchLocked(taskID string, branchID string) *MergeRequest {
	for _, req := range c.snap.Merges {
		if req.TaskID == taskID && req.Status == MergeMerged &&
			(req.BranchA == branchID || req.BranchB == branchID) {
			return req
		}
	}
	return nil
}

// sameMergeRequestLocked reports whether a resubmission names the same
// branches at the same generations/ranges as the original request. Branch ids
// are stored sorted, so submission order does not matter, but a branch that
// advanced (a new report) or a swapped-in branch is a changed request.
func (c *Coordinator) sameMergeRequestLocked(req *MergeRequest, taskID string, aID string, bID string) bool {
	if req.TaskID != taskID {
		return false
	}
	idA, idB := orderTwo(aID, bID)
	if req.BranchA != idA || req.BranchB != idB {
		return false
	}
	curA := c.snap.Branches[idA]
	curB := c.snap.Branches[idB]
	if curA == nil || curB == nil {
		return false
	}
	return curA.Generation == req.GenerationA && curB.Generation == req.GenerationB &&
		curA.Range == req.RangeA && curB.Range == req.RangeB
}

func lineageNodeID(e LineageEntry) string {
	if e.Branch != nil {
		return e.Branch.ID
	}
	if e.Checkpoint != nil {
		return e.Checkpoint.ID
	}
	return ""
}

func sortedRanges(a Range, b Range) []Range {
	out := []Range{a, b}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Start != out[j].Start {
			return out[i].Start < out[j].Start
		}
		return out[i].End < out[j].End
	})
	return out
}

func orderTwo(a string, b string) (string, string) {
	if a <= b {
		return a, b
	}
	return b, a
}

func maxInt64(a int64, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minInt64(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func branchID(taskID string, parentManifestID string, branchNumber int) string {
	h := sha256.New()
	h.Write([]byte("branch\n"))
	h.Write([]byte(taskID))
	h.Write([]byte{'\n'})
	h.Write([]byte(parentManifestID))
	h.Write([]byte{'\n'})
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(branchNumber))
	h.Write(buf[:])
	return hex.EncodeToString(h.Sum(nil))
}

func mainlineVersionID(taskID string, mergeID string, a *Branch, b *Branch) string {
	h := sha256.New()
	h.Write([]byte("mainline\n"))
	h.Write([]byte(taskID))
	h.Write([]byte{'\n'})
	h.Write([]byte(mergeID))
	h.Write([]byte{'\n'})
	writeRange := func(r Range) {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(r.Start))
		h.Write(buf[:])
		binary.BigEndian.PutUint64(buf[:], uint64(r.End))
		h.Write(buf[:])
	}
	first, second := a, b
	if first.ID > second.ID {
		first, second = second, first
	}
	h.Write([]byte(first.ID))
	h.Write([]byte{'\n'})
	writeRange(first.Range)
	h.Write([]byte(second.ID))
	h.Write([]byte{'\n'})
	writeRange(second.Range)
	return hex.EncodeToString(h.Sum(nil))
}
