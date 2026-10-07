package gocheckpointcoordinator

import "errors"

// Sentinel errors returned by coordinator operations. Callers can match them
// with errors.Is.
var (
	// ErrTaskExists is returned when registering a task whose ID is in use.
	ErrTaskExists = errors.New("task already exists")
	// ErrTaskNotFound is returned when a referenced task does not exist.
	ErrTaskNotFound = errors.New("task not found")
	// ErrEmptyMembers is returned when a task is registered without members.
	ErrEmptyMembers = errors.New("task must have at least one member")
	// ErrRoundOpen is returned when a checkpoint is started while the task's
	// previous round has not reached a terminal state.
	ErrRoundOpen = errors.New("previous round is still open")
	// ErrNoRound is returned when no round exists for the referenced task.
	ErrNoRound = errors.New("no round exists for task")
	// ErrUnknownMember is returned when a report comes from a member that is
	// not part of the round's frozen member set.
	ErrUnknownMember = errors.New("member is not part of the frozen round set")
	// ErrStaleRound is returned when a report targets a round other than the
	// task's current round, including late reports for older rounds.
	ErrStaleRound = errors.New("report does not target the current round")
	// ErrConflict is returned when a member reports again for the same round
	// with a different position or digest than its previous report.
	ErrConflict = errors.New("conflicting report for member in round")
	// ErrAlreadyTerminal is returned when an operation (report, abort) targets
	// a round that already has a terminal state.
	ErrAlreadyTerminal = errors.New("round already reached a terminal state")
	// ErrParentNotConfirmed is returned when a branch is forked from a
	// checkpoint that is not a confirmed mainline checkpoint (for example the
	// round is still pending or failed). Such a branch could never be merged
	// back, so publishable branches cannot be created from it.
	ErrParentNotConfirmed = errors.New("parent checkpoint is not confirmed")
	// ErrInvalidRange is returned when a declared processing range is invalid
	// (empty, reversed or positioned before the fork point).
	ErrInvalidRange = errors.New("invalid processing range")
	// ErrBranchNotFound is returned when a referenced branch does not exist.
	ErrBranchNotFound = errors.New("branch not found")
	// ErrBranchNotReady is returned when a merge is requested before every
	// frozen member of both branches has reported for the branch generation.
	ErrBranchNotReady = errors.New("branch is not fully reported")
	// ErrBranchClosed is returned when a report targets a branch that already
	// reached a terminal branch state (merged or failed).
	ErrBranchClosed = errors.New("branch already reached a terminal state")
	// ErrStaleGeneration is returned when a branch report targets an
	// out-of-date generation (for example a late receipt for a branch that was
	// published), or when a merge request forks from a prefix that is no longer
	// the current mainline tip. Old branch receipts can never be written back
	// onto the mainline.
	ErrStaleGeneration = errors.New("report or merge targets a stale generation")
	// ErrPositionOutOfRange is returned when a member report position falls
	// outside the reporting branch's declared processing range.
	ErrPositionOutOfRange = errors.New("report position is outside branch range")
	// ErrMergeRequestChanged is returned when the same merge number is
	// submitted again with different branches or ranges. The original result
	// stays in effect; the changed request is treated as a conflict rather than
	// silently replacing it.
	ErrMergeRequestChanged = errors.New("merge request with same id changed branches or ranges")
)

// MergeConflict describes one concrete difference that prevents two branches
// from merging. Conflicts never cause silent overwrites: the caller learns the
// exact kind and location of every mismatch.
type MergeConflict struct {
	// Kind classifies the conflict: "prefix", "overlap", "missing" or
	// "range".
	Kind string
	// Location pinpoints the conflicting position/key, for example the member
	// whose prefix digests differ or the inclusive [Start, End] overlap point.
	Location string
	// Detail is a human-readable description of the mismatch.
	Detail string
}

// MergeError is returned by ApplyMerge when the two branches cannot be merged.
// It carries every concrete conflict location. A failed merge keeps both
// branches available for a later retry under a new merge number.
type MergeError struct {
	Conflicts []MergeConflict
}

func (e *MergeError) Error() string {
	if e == nil || len(e.Conflicts) == 0 {
		return "merge failed"
	}
	return "merge failed: " + e.Conflicts[0].Location + " (" + e.Conflicts[0].Kind + ")"
}
