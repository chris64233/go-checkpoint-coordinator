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
	// ErrBranchExists is returned when a branch number is already in use for
	// a task's parent checkpoint.
	ErrBranchExists = errors.New("branch already exists")
	// ErrBranchNotFound is returned when a referenced branch does not exist.
	ErrBranchNotFound = errors.New("branch not found")
	// ErrInvalidRange is returned when a branch is created with a malformed
	// range (start after end).
	ErrInvalidRange = errors.New("invalid branch range")
	// ErrParentNotCompleted is returned when a branch is requested from a
	// checkpoint that is not a completed manifest. A publishable branch can
	// only fork from a confirmed checkpoint.
	ErrParentNotCompleted = errors.New("parent checkpoint is not a completed checkpoint")
	// ErrBranchMerged is returned when an already merged branch receives a
	// report or participates in another merge. A merged branch belongs to a
	// published mainline version and its old receipts cannot be written back.
	ErrBranchMerged = errors.New("branch is already merged")
	// ErrStaleBranch is returned when a branch report carries a generation
	// older than the branch's current generation, so a stale receipt can never
	// overwrite newer branch state.
	ErrStaleBranch = errors.New("stale branch report")
	// ErrMergeRequestChanged is returned when a merge id is resubmitted with
	// different branches, ranges or generations than the original request.
	ErrMergeRequestChanged = errors.New("merge request changed: branches or ranges differ")
	// ErrMergeSameBranch is returned when a merge application names the same
	// branch twice.
	ErrMergeSameBranch = errors.New("merge requires two distinct branches")
	// ErrBranchPrefixUnconfirmed is returned when a branch participating in a
	// merge has never confirmed its common prefix.
	ErrBranchPrefixUnconfirmed = errors.New("branch has not confirmed its common prefix")
)
