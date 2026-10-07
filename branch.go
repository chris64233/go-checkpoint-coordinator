package gocheckpointcoordinator

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
)

// ForkResult is returned by ForkBranch.
type ForkResult struct {
	Branch     Branch
	Checkpoint MainlineCheckpoint // the confirmed parent checkpoint
}

// BranchReportResult is returned by ReportBranch.
type BranchReportResult struct {
	// AlreadyReported is true when the identical report was already accepted
	// for the branch generation; the original branch state is returned.
	AlreadyReported bool
	// Ready is true when every frozen member has reported for the branch's
	// current generation.
	Ready  bool
	Branch Branch
}

// MergeResult is the outcome of ApplyMerge. Replaying a merge number with the
// identical request returns the same result.
type MergeResult struct {
	// Replayed is true when this call returned a previously stored outcome for
	// the same merge number and identical request.
	Replayed bool
	// Merge is the stored merge record (success or failure).
	Merge MergeRecord
	// Checkpoint is the new mainline checkpoint on success.
	Checkpoint *MainlineCheckpoint
}

// BranchView is a query row showing a branch together with its parent
// checkpoint and conflict information.
type BranchView struct {
	Branch    Branch
	Parent    *MainlineCheckpoint
	Conflicts []MergeConflict // conflicts recorded against this branch, if any
}

// ForkBranch creates a publishable processing branch forked from a confirmed
// mainline checkpoint.
//
// The branch records its parent checkpoint, a task-local branch number, its
// declared closed processing range and the generation it works at. A branch
// can only be forked from a confirmed checkpoint: forking from a pending or
// failed round returns ErrParentNotConfirmed.
func (c *Coordinator) ForkBranch(taskID, parentCheckpointID string, rng Range) (*ForkResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.snap.Tasks[taskID]; !ok {
		return nil, ErrTaskNotFound
	}
	if rng.End < rng.Start {
		return nil, ErrInvalidRange
	}
	parent, ok := c.mainlineByIDLocked(taskID, parentCheckpointID)
	if !ok {
		// The checkpoint either does not exist or belongs to a round that never
		// completed: no publishable branch can start from it.
		return nil, ErrParentNotConfirmed
	}
	if rng.Start <= parent.tipPositionLocked() {
		// A branch extends the common prefix; covering the prefix itself would
		// make the merge ambiguous.
		return nil, ErrInvalidRange
	}

	task := c.snap.Tasks[taskID]
	num := c.snap.NextBranchNum[taskID] + 1
	c.snap.NextBranchNum[taskID] = num
	branch := &Branch{
		TaskID:             taskID,
		Number:             num,
		ParentCheckpointID: parent.ID,
		ParentGeneration:   parent.Generation,
		Range:              rng,
		Generation:         parent.Generation + 1,
		Status:             BranchOpen,
		FrozenMembers:      cloneStrings(task.Members),
		Reports:            map[string]Report{},
		CreatedAt:          c.now().UnixMilli(),
	}
	c.snap.Branches[taskID] = append(c.snap.Branches[taskID], branch)
	if err := c.persistLocked(); err != nil {
		list := c.snap.Branches[taskID]
		c.snap.Branches[taskID] = list[:len(list)-1]
		if len(c.snap.Branches[taskID]) == 0 {
			delete(c.snap.Branches, taskID)
		}
		c.snap.NextBranchNum[taskID] = num - 1
		return nil, err
	}
	return &ForkResult{Branch: *cloneBranch(branch), Checkpoint: *cloneMainline(parent)}, nil
}

// ReportBranch records a member report for a branch generation.
//
// Reports must target the branch's current generation: late receipts for an
// old generation return ErrStaleGeneration and can never be written back onto
// the mainline. Re-reporting identical content is idempotent; different
// content conflicts; positions outside the branch range are rejected.
func (c *Coordinator) ReportBranch(taskID string, branchNum, generation int64, member string, position int64, digest string) (*BranchReportResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.snap.Tasks[taskID]; !ok {
		return nil, ErrTaskNotFound
	}
	b := c.branchByNumLocked(taskID, branchNum)
	if b == nil {
		return nil, ErrBranchNotFound
	}
	if generation != b.Generation {
		return nil, ErrStaleGeneration
	}
	if b.Status != BranchOpen {
		return nil, ErrBranchClosed
	}
	if !containsString(b.FrozenMembers, member) {
		return nil, ErrUnknownMember
	}
	if position < b.Range.Start || position > b.Range.End {
		return nil, ErrPositionOutOfRange
	}
	if prev, ok := b.Reports[member]; ok {
		if prev.Position == position && prev.Digest == digest {
			return c.buildBranchResultLocked(b, true), nil
		}
		return nil, ErrConflict
	}
	b.Reports[member] = Report{
		TaskID:     taskID,
		Round:      b.Number,
		Member:     member,
		Position:   position,
		Digest:     digest,
		ReportedAt: c.now().UnixMilli(),
	}
	if err := c.persistLocked(); err != nil {
		c.reloadAfterPersistErrorLocked(err)
		return nil, err
	}
	return c.buildBranchResultLocked(b, false), nil
}

// ApplyMerge requests merging two branches back onto the mainline.
//
// mergeID is a client-supplied merge number. Semantics:
//
//   - Before merging, the common prefix (identical confirmed parent, still the
//     current mainline tip) and the non-overlapping, reconfirmed ranges are
//     checked; every mismatch is returned as a concrete MergeConflict inside
//     *MergeError — nothing is silently overwritten.
//   - On success exactly one new mainline checkpoint is published and both
//     branches become "merged".
//   - On failure the merge record (with all conflict locations) is stored and
//     both branches are preserved in "failed" state for follow-up handling.
//   - The same merge number with the identical request returns the original
//     result (Replayed == true). The same merge number with different
//     branches or ranges returns ErrMergeRequestChanged.
//
// The whole check-and-commit sequence runs under the coordinator lock, so a
// branch report, a merge request and a new checkpoint publish cannot produce
// two effective mainline versions.
func (c *Coordinator) ApplyMerge(taskID, mergeID string, branchA, branchB int64, rangeA, rangeB Range) (*MergeResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.snap.Tasks[taskID]; !ok {
		return nil, ErrTaskNotFound
	}
	if mergeID == "" {
		return nil, fmt.Errorf("merge id is required")
	}
	fp := mergeFingerprint(branchA, branchB, rangeA, rangeB)
	if existing, ok := c.snap.Merges[taskID][mergeID]; ok {
		if existing.Fingerprint != fp {
			return nil, ErrMergeRequestChanged
		}
		res := &MergeResult{Replayed: true, Merge: *cloneMerge(existing)}
		if existing.CheckpointID != "" {
			if cp, ok := c.mainlineByIDLocked(taskID, existing.CheckpointID); ok {
				res.Checkpoint = cloneMainline(cp)
			}
		}
		return res, nil
	}
	if c.snap.Merges[taskID] == nil {
		c.snap.Merges[taskID] = map[string]*MergeRecord{}
	}

	record := &MergeRecord{
		ID:          mergeID,
		TaskID:      taskID,
		BranchA:     branchA,
		BranchB:     branchB,
		RangeA:      rangeA,
		RangeB:      rangeB,
		Fingerprint: fp,
		CreatedAt:   c.now().UnixMilli(),
	}

	ba := c.branchByNumLocked(taskID, branchA)
	bb := c.branchByNumLocked(taskID, branchB)
	conflicts := make([]MergeConflict, 0)

	if ba == nil {
		conflicts = append(conflicts, MergeConflict{Kind: "missing", Location: fmt.Sprintf("branch.%d", branchA), Detail: "branch does not exist"})
	} else if ba.Status != BranchOpen {
		conflicts = append(conflicts, MergeConflict{Kind: "state", Location: fmt.Sprintf("branch.%d", branchA), Detail: "branch is " + string(ba.Status)})
	}
	if bb == nil {
		conflicts = append(conflicts, MergeConflict{Kind: "missing", Location: fmt.Sprintf("branch.%d", branchB), Detail: "branch does not exist"})
	} else if bb.Status != BranchOpen {
		conflicts = append(conflicts, MergeConflict{Kind: "state", Location: fmt.Sprintf("branch.%d", branchB), Detail: "branch is " + string(bb.Status)})
	}
	if ba != nil && bb != nil {
		if ba.Number == bb.Number {
			conflicts = append(conflicts, MergeConflict{Kind: "range", Location: fmt.Sprintf("branch.%d", ba.Number), Detail: "a branch cannot merge with itself"})
		}
		// Common prefix: both branches must fork from the same confirmed
		// checkpoint.
		if ba.ParentCheckpointID != bb.ParentCheckpointID {
			conflicts = append(conflicts, MergeConflict{
				Kind:     "prefix",
				Location: fmt.Sprintf("branch.%d.parent/branch.%d.parent", ba.Number, bb.Number),
				Detail:   fmt.Sprintf("parent checkpoints differ: %s != %s", ba.ParentCheckpointID, bb.ParentCheckpointID),
			})
		}
		// Reconfirmed ranges must match the declared branch ranges.
		if ba.Range != rangeA {
			conflicts = append(conflicts, rangeConflict(ba.Number, ba.Range, rangeA))
		}
		if bb.Range != rangeB {
			conflicts = append(conflicts, rangeConflict(bb.Number, bb.Range, rangeB))
		}
		if rangeA.End < rangeA.Start {
			conflicts = append(conflicts, MergeConflict{Kind: "range", Location: fmt.Sprintf("branch.%d.range", ba.Number), Detail: "reconfirmed range is empty or reversed"})
		}
		if rangeB.End < rangeB.Start {
			conflicts = append(conflicts, MergeConflict{Kind: "range", Location: fmt.Sprintf("branch.%d.range", bb.Number), Detail: "reconfirmed range is empty or reversed"})
		}
		if point := overlapPoint(rangeA, rangeB); point != nil {
			conflicts = append(conflicts, MergeConflict{
				Kind:     "overlap",
				Location: fmt.Sprintf("position.%d", *point),
				Detail:   fmt.Sprintf("branch.%d range [%d,%d] overlaps branch.%d range [%d,%d] at %d", ba.Number, rangeA.Start, rangeA.End, bb.Number, rangeB.Start, rangeB.End, *point),
			})
		}
		if !branchReadyLocked(ba) {
			conflicts = append(conflicts, missingReports(ba)...)
		}
		if !branchReadyLocked(bb) {
			conflicts = append(conflicts, missingReports(bb)...)
		}
		// The common prefix must still be the current mainline tip: a newer
		// published mainline means these receipts belong to an old generation
		// and cannot be written back.
		if len(conflicts) == 0 {
			tip := c.mainlineTipLocked(taskID)
			if tip == nil || tip.ID != ba.ParentCheckpointID {
				conflicts = append(conflicts, MergeConflict{
					Kind:     "prefix",
					Location: "mainline.tip",
					Detail:   "common prefix is no longer the current mainline version",
				})
			}
		}
	}

	if len(conflicts) > 0 {
		c.failMergeLocked(record, ba, bb, conflicts)
		if err := c.persistLocked(); err != nil {
			c.reloadAfterPersistErrorLocked(err)
			return nil, err
		}
		return nil, &MergeError{Conflicts: cloneConflicts(conflicts)}
	}

	parent, _ := c.mainlineByIDLocked(taskID, ba.ParentCheckpointID)
	c.commitMergeLocked(record, ba, bb, parent)
	if err := c.persistLocked(); err != nil {
		c.reloadAfterPersistErrorLocked(err)
		return nil, err
	}
	cp, _ := c.mainlineByIDLocked(taskID, record.CheckpointID)
	return &MergeResult{Merge: *cloneMerge(record), Checkpoint: cloneMainline(cp)}, nil
}

// GetMainline returns the task's mainline checkpoints in ascending generation
// order, each with its parent checkpoint id, contributing branches and the
// round/merge reference it came from.
func (c *Coordinator) GetMainline(taskID string) ([]MainlineCheckpoint, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.snap.Tasks[taskID]; !ok {
		return nil, ErrTaskNotFound
	}
	src := c.snap.Mainline[taskID]
	out := make([]MainlineCheckpoint, len(src))
	for i, cp := range src {
		out[i] = *cloneMainline(cp)
	}
	return out, nil
}

// GetBranches returns a task's branches in stable ascending branch-number
// order, each annotated with its parent checkpoint and any conflicts recorded
// against it by failed merges.
func (c *Coordinator) GetBranches(taskID string) ([]BranchView, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.snap.Tasks[taskID]; !ok {
		return nil, ErrTaskNotFound
	}
	var out []BranchView
	for _, b := range c.snap.Branches[taskID] {
		view := BranchView{Branch: *cloneBranch(b)}
		if parent, ok := c.mainlineByIDLocked(taskID, b.ParentCheckpointID); ok {
			view.Parent = cloneMainline(parent)
		}
		for _, m := range c.snap.Merges[taskID] {
			if m.Status != MergeFailed {
				continue
			}
			if m.BranchA == b.Number || m.BranchB == b.Number {
				view.Conflicts = append(view.Conflicts, cloneConflicts(m.Conflicts)...)
			}
		}
		out = append(out, view)
	}
	return out, nil
}

// GetMerge returns the stored record for a merge number, including concrete
// conflict locations for failed merges.
func (c *Coordinator) GetMerge(taskID, mergeID string) (*MergeRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.snap.Tasks[taskID]; !ok {
		return nil, ErrTaskNotFound
	}
	m, ok := c.snap.Merges[taskID][mergeID]
	if !ok {
		return nil, ErrMergeRequestChanged
	}
	return cloneMerge(m), nil
}

// ---- internal helpers (caller must hold c.mu) ----

func (c *Coordinator) branchByNumLocked(taskID string, num int64) *Branch {
	for _, b := range c.snap.Branches[taskID] {
		if b.Number == num {
			return b
		}
	}
	return nil
}

func (c *Coordinator) mainlineTipLocked(taskID string) *MainlineCheckpoint {
	list := c.snap.Mainline[taskID]
	if len(list) == 0 {
		return nil
	}
	return list[len(list)-1]
}

func (c *Coordinator) mainlineByIDLocked(taskID, id string) (*MainlineCheckpoint, bool) {
	for _, cp := range c.snap.Mainline[taskID] {
		if cp.ID == id {
			return cp, true
		}
	}
	return nil, false
}

// publishMainlineLocked appends a new mainline checkpoint with the next
// generation and returns it. Generations are assigned strictly in commit
// order, so concurrent publishers serialised by the lock can never produce two
// effective versions for the same generation.
func (c *Coordinator) publishMainlineLocked(cp *MainlineCheckpoint) *MainlineCheckpoint {
	gen := c.snap.MainlineGen[cp.TaskID] + 1
	c.snap.MainlineGen[cp.TaskID] = gen
	cp.Generation = gen
	c.snap.Mainline[cp.TaskID] = append(c.snap.Mainline[cp.TaskID], cp)
	return cp
}

// publishRoundCheckpointLocked materialises the mainline checkpoint that
// corresponds to a completed round.
func (c *Coordinator) publishRoundCheckpointLocked(r *Round, manifest *Manifest, atMillis int64) *MainlineCheckpoint {
	taskID := r.TaskID
	var parentID string
	if tip := c.mainlineTipLocked(taskID); tip != nil {
		parentID = tip.ID
	}
	reports := make([]Report, len(manifest.Reports))
	copy(reports, manifest.Reports)
	cp := &MainlineCheckpoint{
		TaskID:     taskID,
		ParentID:   parentID,
		ManifestID: manifest.ID,
		Round:      r.Number,
		Source:     "round",
		Reports:    reports,
		CreatedAt:  atMillis,
	}
	cp.ID = mainlineID(cp, nil)
	return c.publishMainlineLocked(cp)
}

// commitMergeLocked publishes the merged mainline checkpoint and closes both
// branches as merged.
func (c *Coordinator) commitMergeLocked(record *MergeRecord, ba, bb *Branch, parent *MainlineCheckpoint) {
	now := c.now().UnixMilli()

	mergedReports := make(map[string]Report, len(parent.Reports))
	for _, rep := range parent.Reports {
		mergedReports[rep.Member] = rep
	}
	// Non-overlapping ranges guarantee each member contributes at most one
	// branch report; the parent copy remains for members not covered by a
	// branch range.
	for _, b := range []*Branch{ba, bb} {
		for _, rep := range b.Reports {
			mergedReports[rep.Member] = rep
		}
	}
	members := make([]string, 0, len(mergedReports))
	for m := range mergedReports {
		members = append(members, m)
	}
	sort.Strings(members)
	reports := make([]Report, 0, len(members))
	for _, m := range members {
		reports = append(reports, mergedReports[m])
	}

	cp := &MainlineCheckpoint{
		TaskID:    record.TaskID,
		ParentID:  parent.ID,
		MergeID:   record.ID,
		Source:    "merge",
		Reports:   reports,
		Branches:  []int64{ba.Number, bb.Number},
		CreatedAt: now,
	}
	cp.ID = mainlineID(cp, []*Branch{ba, bb})
	c.publishMainlineLocked(cp)

	record.Status = MergeSucceeded
	record.CheckpointID = cp.ID
	record.DecidedAt = now
	for _, b := range []*Branch{ba, bb} {
		b.Status = BranchMerged
		b.ClosedAt = now
	}
}

// failMergeLocked stores the failed outcome and preserves both branches in
// failed state so they stay available for follow-up handling. A nil branch
// (missing) is simply left untouched.
func (c *Coordinator) failMergeLocked(record *MergeRecord, ba, bb *Branch, conflicts []MergeConflict) {
	now := c.now().UnixMilli()
	record.Status = MergeFailed
	record.Conflicts = cloneConflicts(conflicts)
	record.DecidedAt = now
	for _, b := range []*Branch{ba, bb} {
		if b != nil && b.Status == BranchOpen {
			b.Status = BranchFailed
			b.ClosedAt = now
		}
	}
	if c.snap.Merges[record.TaskID] == nil {
		c.snap.Merges[record.TaskID] = map[string]*MergeRecord{}
	}
	c.snap.Merges[record.TaskID][record.ID] = record
}

func (c *Coordinator) buildBranchResultLocked(b *Branch, duplicate bool) *BranchReportResult {
	return &BranchReportResult{
		AlreadyReported: duplicate,
		Ready:           branchReadyLocked(b),
		Branch:          *cloneBranch(b),
	}
}

func branchReadyLocked(b *Branch) bool {
	for _, m := range b.FrozenMembers {
		if _, ok := b.Reports[m]; !ok {
			return false
		}
	}
	return true
}

func missingReports(b *Branch) []MergeConflict {
	var missing []string
	for _, m := range b.FrozenMembers {
		if _, ok := b.Reports[m]; !ok {
			missing = append(missing, m)
		}
	}
	sort.Strings(missing)
	out := make([]MergeConflict, 0, len(missing))
	for _, m := range missing {
		out = append(out, MergeConflict{
			Kind:     "missing",
			Location: fmt.Sprintf("branch.%d.member.%s", b.Number, m),
			Detail:   "member has not reported for the branch generation",
		})
	}
	return out
}

func rangeConflict(branchNum int64, declared, reconfirmed Range) MergeConflict {
	return MergeConflict{
		Kind:     "range",
		Location: fmt.Sprintf("branch.%d.range", branchNum),
		Detail:   fmt.Sprintf("reconfirmed range [%d,%d] differs from declared range [%d,%d]", reconfirmed.Start, reconfirmed.End, declared.Start, declared.End),
	}
}

// overlapPoint returns the first position where two closed ranges overlap, or
// nil when they are disjoint (adjacency is allowed).
func overlapPoint(a, b Range) *int64 {
	if a.End < b.Start || b.End < a.Start {
		return nil
	}
	p := a.Start
	if b.Start > p {
		p = b.Start
	}
	return &p
}

// tipPositionLocked returns the largest processing position included in the
// checkpoint, i.e. the extent of the confirmed common prefix.
func (cp *MainlineCheckpoint) tipPositionLocked() int64 {
	var tip int64
	for _, r := range cp.Reports {
		if r.Position > tip {
			tip = r.Position
		}
	}
	return tip
}

// mergeFingerprint identifies the exact content of a merge request: the same
// merge number with different branches or reconfirmed ranges hashes differently.
func mergeFingerprint(branchA, branchB int64, rangeA, rangeB Range) string {
	h := sha256.New()
	h.Write([]byte("merge-request\n"))
	lo, hi := branchA, branchB
	loR, hiR := rangeA, rangeB
	if branchB < branchA {
		lo, hi = branchB, branchA
		loR, hiR = rangeB, rangeA
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(lo))
	h.Write(buf[:])
	binary.BigEndian.PutUint64(buf[:], uint64(hi))
	h.Write(buf[:])
	writeRangeHash(h, loR)
	writeRangeHash(h, hiR)
	return hex.EncodeToString(h.Sum(nil))
}

func writeRangeHash(h interface{ Write([]byte) (int, error) }, r Range) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(r.Start))
	h.Write(buf[:])
	binary.BigEndian.PutUint64(buf[:], uint64(r.End))
	h.Write(buf[:])
}

// mainlineID derives a deterministic content identifier for a mainline
// checkpoint. Round checkpoints inherit the manifest id; merge checkpoints hash
// the parent, contributing branch ids/generations and every included report.
func mainlineID(cp *MainlineCheckpoint, branches []*Branch) string {
	if cp.Source == "round" {
		return cp.ManifestID
	}
	h := sha256.New()
	h.Write([]byte("mainline-merge\n"))
	h.Write([]byte(cp.TaskID))
	h.Write([]byte{'\n'})
	h.Write([]byte(cp.ParentID))
	h.Write([]byte{'\n'})
	for _, b := range branches {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(b.Number))
		h.Write(buf[:])
		binary.BigEndian.PutUint64(buf[:], uint64(b.Generation))
		h.Write(buf[:])
	}
	h.Write([]byte{'\n'})
	for _, r := range cp.Reports {
		h.Write([]byte(r.Member))
		h.Write([]byte{'\n'})
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(r.Position))
		h.Write(buf[:])
		h.Write([]byte(r.Digest))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}
