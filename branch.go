package gocheckpointcoordinator

import (
	"fmt"
	"sort"
)

// BranchStatus is the lifecycle state of a processing branch.
type BranchStatus string

const (
	// BranchOpen means the branch is actively processing and can accept
	// reports and merges.
	BranchOpen BranchStatus = "open"
	// BranchMerged means the branch was merged back into the mainline.
	BranchMerged BranchStatus = "merged"
)

// MergeStatus is the outcome of a merge request.
type MergeStatus string

const (
	// MergeApplied means the merge produced a new mainline checkpoint.
	MergeApplied MergeStatus = "applied"
	// MergeConflicted means the merge was rejected; both branches are kept.
	MergeConflicted MergeStatus = "conflicted"
)

// Branch is one processing branch forked from a confirmed parent checkpoint.
type Branch struct {
	// TaskID is the owning task.
	TaskID string
	// ParentRound is the completed checkpoint round the branch forked from.
	ParentRound int64
	// BranchNo identifies the branch within the task.
	BranchNo int64
	// RangeStart and RangeEnd delimit the branch's processing range
	// ([RangeStart, RangeEnd)).
	RangeStart int64
	RangeEnd   int64
	// Generation is the branch's current generation; every accepted report
	// advances it and stale generations can never write back.
	Generation int64
	// Status is the current lifecycle status.
	Status BranchStatus
	// Position is the branch's latest reported processing position.
	Position int64
	// Digest is the branch's latest reported state digest.
	Digest string
	// HasReport is true once the branch accepted at least one receipt.
	HasReport bool
	// CreatedAt is the wall-clock time the branch was created (unix millis).
	CreatedAt int64
	// UpdatedAt is the wall-clock time of the last accepted report
	// (unix millis).
	UpdatedAt int64
}

// MergeConflict pinpoints one divergence found while merging two branches.
type MergeConflict struct {
	// Kind is "parent_mismatch" or "range_overlap".
	Kind string
	// Position is the exact location of the conflict: the differing parent
	// round for parent mismatches, or the first overlapping offset for range
	// overlaps.
	Position int64
	// Detail describes the conflict.
	Detail string
}

// MergeRecord is the durable, idempotent outcome of one merge request.
type MergeRecord struct {
	// MergeID is the caller-supplied idempotency key.
	MergeID string
	// TaskID is the owning task.
	TaskID string
	// BranchA and BranchB are the merged branch numbers (BranchA < BranchB).
	BranchA int64
	BranchB int64
	// RangeStartA/RangeEndA and RangeStartB/RangeEndB snapshot the branch
	// ranges at merge time; resubmitting the same MergeID with different
	// branches or ranges is a conflict.
	RangeStartA int64
	RangeEndA   int64
	RangeStartB int64
	RangeEndB   int64
	// Status is the merge outcome.
	Status MergeStatus
	// Conflicts lists every divergence found; empty when Status is applied.
	Conflicts []MergeConflict
	// MainlineRound is the new mainline checkpoint round produced by a
	// successful merge; zero otherwise.
	MainlineRound int64
	// ManifestID references the merged mainline manifest when applied.
	ManifestID string
	// CreatedAt is the wall-clock time the merge was decided (unix millis).
	CreatedAt int64
}

// CreateBranch forks a processing branch from a confirmed (completed) parent
// checkpoint round.
//
// The parent round must exist and be completed: a branch that could be
// published later cannot fork from an unconfirmed checkpoint. The processing
// range must be non-empty (rangeStart < rangeEnd). Branch numbers are unique
// per task. New branches start at generation 1.
func (c *Coordinator) CreateBranch(taskID string, parentRound, branchNo, rangeStart, rangeEnd int64) (*Branch, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.snap.Tasks[taskID]; !ok {
		return nil, ErrTaskNotFound
	}
	parent := c.findRoundLocked(taskID, parentRound)
	if parent == nil || parent.Status != RoundCompleted {
		return nil, ErrParentNotComplete
	}
	if rangeStart >= rangeEnd {
		return nil, ErrInvalidRange
	}
	if c.findBranchLocked(taskID, branchNo) != nil {
		return nil, ErrBranchExists
	}
	now := c.now().UnixMilli()
	b := &Branch{
		TaskID:      taskID,
		ParentRound: parentRound,
		BranchNo:    branchNo,
		RangeStart:  rangeStart,
		RangeEnd:    rangeEnd,
		Generation:  1,
		Status:      BranchOpen,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	c.snap.Branches = append(c.snap.Branches, b)
	if err := c.persistLocked(); err != nil {
		c.snap.Branches = c.snap.Branches[:len(c.snap.Branches)-1]
		return nil, err
	}
	return cloneBranch(b), nil
}

// ReportBranch records a branch processing receipt.
//
// The receipt must carry the branch's next generation. A generation older
// than the branch's current generation is rejected with ErrStaleGeneration:
// stale receipts can never write back to the branch or the mainline.
// Repeating the current generation with identical position and digest is
// idempotent; different content at the same generation is ErrConflict.
func (c *Coordinator) ReportBranch(taskID string, branchNo, generation, position int64, digest string) (*Branch, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	b := c.findBranchLocked(taskID, branchNo)
	if b == nil {
		if _, ok := c.snap.Tasks[taskID]; !ok {
			return nil, ErrTaskNotFound
		}
		return nil, ErrBranchNotFound
	}
	if b.Status != BranchOpen {
		return nil, ErrBranchNotOpen
	}
	switch {
	case generation < b.Generation:
		return nil, ErrStaleGeneration
	case generation == b.Generation:
		if b.HasReport {
			if b.Position == position && b.Digest == digest {
				return cloneBranch(b), nil
			}
			return nil, ErrConflict
		}
		// First receipt at the initial generation: accept below.
	}
	b.Generation = generation
	b.Position = position
	b.Digest = digest
	b.HasReport = true
	b.UpdatedAt = c.now().UnixMilli()
	if err := c.persistLocked(); err != nil {
		c.reloadAfterPersistErrorLocked(err)
		return nil, err
	}
	return cloneBranch(b), nil
}

// MergeBranches merges two branches of a task back into the mainline.
//
// Before merging, the common prefix (both branches must fork from the same
// parent checkpoint) and the non-overlap of the two processing ranges are
// re-verified. Any divergence is reported with its exact position in the
// returned MergeRecord (with ErrMergeConflict); nothing is silently
// overwritten and both branches are kept open for later handling.
//
// A successful merge atomically publishes exactly one new mainline checkpoint
// round (with its manifest and notification) and marks both branches merged.
// The merge is serialised with branch reports and new checkpoint rounds under
// the coordinator lock, so only one mainline version ever takes effect. A
// merge cannot be applied while another round is pending (ErrRoundOpen).
//
// MergeID is an idempotency key: resubmitting the same MergeID with the same
// branches and ranges returns the original record; the same MergeID with
// different branches or ranges is rejected with ErrConflict.
func (c *Coordinator) MergeBranches(taskID, mergeID string, branchA, branchB int64) (*MergeRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.snap.Tasks[taskID]; !ok {
		return nil, ErrTaskNotFound
	}
	if branchA == branchB {
		return nil, ErrConflict
	}
	if branchB < branchA {
		branchA, branchB = branchB, branchA
	}
	a := c.findBranchLocked(taskID, branchA)
	b := c.findBranchLocked(taskID, branchB)
	if a == nil || b == nil {
		return nil, ErrBranchNotFound
	}

	if prev := c.findMergeLocked(taskID, mergeID); prev != nil {
		if prev.BranchA == branchA && prev.BranchB == branchB &&
			prev.RangeStartA == a.RangeStart && prev.RangeEndA == a.RangeEnd &&
			prev.RangeStartB == b.RangeStart && prev.RangeEndB == b.RangeEnd {
			return cloneMergeRecord(prev), nil
		}
		return nil, ErrConflict
	}

	now := c.now().UnixMilli()
	rec := &MergeRecord{
		MergeID:     mergeID,
		TaskID:      taskID,
		BranchA:     branchA,
		BranchB:     branchB,
		RangeStartA: a.RangeStart,
		RangeEndA:   a.RangeEnd,
		RangeStartB: b.RangeStart,
		RangeEndB:   b.RangeEnd,
		CreatedAt:   now,
	}

	// Re-verify the common prefix and non-overlapping ranges.
	if a.ParentRound != b.ParentRound {
		pos := a.ParentRound
		if b.ParentRound < pos {
			pos = b.ParentRound
		}
		rec.Conflicts = append(rec.Conflicts, MergeConflict{
			Kind:     "parent_mismatch",
			Position: pos,
			Detail: fmt.Sprintf("branch %d forks from round %d, branch %d forks from round %d",
				branchA, a.ParentRound, branchB, b.ParentRound),
		})
	}
	if a.RangeStart < b.RangeEnd && b.RangeStart < a.RangeEnd {
		pos := a.RangeStart
		if b.RangeStart > pos {
			pos = b.RangeStart
		}
		rec.Conflicts = append(rec.Conflicts, MergeConflict{
			Kind:     "range_overlap",
			Position: pos,
			Detail: fmt.Sprintf("branch %d range [%d,%d) overlaps branch %d range [%d,%d)",
				branchA, a.RangeStart, a.RangeEnd, branchB, b.RangeStart, b.RangeEnd),
		})
	}

	if len(rec.Conflicts) > 0 {
		// Failed merge: record the conflicts and keep both branches open.
		rec.Status = MergeConflicted
		c.snap.Merges = append(c.snap.Merges, rec)
		if err := c.persistLocked(); err != nil {
			c.snap.Merges = c.snap.Merges[:len(c.snap.Merges)-1]
			return nil, err
		}
		return cloneMergeRecord(rec), ErrMergeConflict
	}

	if a.Status != BranchOpen || b.Status != BranchOpen {
		return nil, ErrBranchNotOpen
	}
	if cur := c.currentByTask[taskID]; cur != nil && cur.Status == RoundPending {
		return nil, ErrRoundOpen
	}

	// Publish exactly one new mainline version from the two branches.
	var nextNumber int64 = 1
	if cur := c.currentByTask[taskID]; cur != nil {
		nextNumber = cur.Number + 1
	}
	round := &Round{
		TaskID:        taskID,
		Number:        nextNumber,
		Status:        RoundPending,
		FrozenMembers: []string{branchMember(branchA), branchMember(branchB)},
		Reports: map[string]Report{
			branchMember(branchA): {TaskID: taskID, Round: nextNumber, Member: branchMember(branchA), Position: a.Position, Digest: a.Digest, ReportedAt: now},
			branchMember(branchB): {TaskID: taskID, Round: nextNumber, Member: branchMember(branchB), Position: b.Position, Digest: b.Digest, ReportedAt: now},
		},
		CreatedAt: now,
	}
	c.snap.Rounds = append(c.snap.Rounds, round)
	c.currentByTask[taskID] = round
	c.completeLocked(round, now)

	a.Status = BranchMerged
	b.Status = BranchMerged
	rec.Status = MergeApplied
	rec.MainlineRound = nextNumber
	rec.ManifestID = round.ManifestID
	c.snap.Merges = append(c.snap.Merges, rec)

	if err := c.persistLocked(); err != nil {
		c.reloadAfterPersistErrorLocked(err)
		return nil, err
	}
	return cloneMergeRecord(rec), nil
}

// GetBranches returns all branches of a task in stable generation order
// (ascending generation, ties broken by branch number).
func (c *Coordinator) GetBranches(taskID string) ([]Branch, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.snap.Tasks[taskID]; !ok {
		return nil, ErrTaskNotFound
	}
	var out []Branch
	for _, b := range c.snap.Branches {
		if b.TaskID == taskID {
			out = append(out, *cloneBranch(b))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Generation != out[j].Generation {
			return out[i].Generation < out[j].Generation
		}
		return out[i].BranchNo < out[j].BranchNo
	})
	return out, nil
}

// GetMergeHistory returns all merge records of a task in decision order,
// including conflict positions and the resulting mainline version.
func (c *Coordinator) GetMergeHistory(taskID string) ([]MergeRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.snap.Tasks[taskID]; !ok {
		return nil, ErrTaskNotFound
	}
	var out []MergeRecord
	for _, m := range c.snap.Merges {
		if m.TaskID == taskID {
			out = append(out, *cloneMergeRecord(m))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt != out[j].CreatedAt {
			return out[i].CreatedAt < out[j].CreatedAt
		}
		return out[i].MergeID < out[j].MergeID
	})
	return out, nil
}

// ---- internal helpers (caller must hold c.mu) ----

func (c *Coordinator) findRoundLocked(taskID string, number int64) *Round {
	for _, r := range c.snap.Rounds {
		if r.TaskID == taskID && r.Number == number {
			return r
		}
	}
	return nil
}

func (c *Coordinator) findBranchLocked(taskID string, branchNo int64) *Branch {
	for _, b := range c.snap.Branches {
		if b.TaskID == taskID && b.BranchNo == branchNo {
			return b
		}
	}
	return nil
}

func (c *Coordinator) findMergeLocked(taskID, mergeID string) *MergeRecord {
	for _, m := range c.snap.Merges {
		if m.TaskID == taskID && m.MergeID == mergeID {
			return m
		}
	}
	return nil
}

func branchMember(branchNo int64) string {
	return fmt.Sprintf("branch-%d", branchNo)
}
