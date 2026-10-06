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
	// ErrParentNotComplete is returned when creating a branch whose parent
	// checkpoint round has not completed yet.
	ErrParentNotComplete = errors.New("parent checkpoint round is not completed")
	// ErrInvalidRange is returned when a branch processing range is empty or
	// inverted.
	ErrInvalidRange = errors.New("branch range is empty or inverted")
	// ErrBranchExists is returned when creating a branch whose number is
	// already in use for the task.
	ErrBranchExists = errors.New("branch already exists for task")
	// ErrBranchNotFound is returned when a referenced branch does not exist.
	ErrBranchNotFound = errors.New("branch not found")
	// ErrBranchNotOpen is returned when an operation targets a branch that is
	// no longer open (e.g. already merged).
	ErrBranchNotOpen = errors.New("branch is not open")
	// ErrStaleGeneration is returned when a branch report carries a generation
	// older than the branch's current generation; stale receipts never write
	// back to the mainline.
	ErrStaleGeneration = errors.New("branch report generation is stale")
	// ErrMergeConflict is returned when a merge detects divergent prefixes or
	// overlapping ranges; the returned MergeRecord carries the exact conflict
	// positions.
	ErrMergeConflict = errors.New("branch merge conflict")
)
