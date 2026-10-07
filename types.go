package gocheckpointcoordinator

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
)

// RoundStatus is the lifecycle state of a checkpoint round.
type RoundStatus string

const (
	// RoundPending means the round is open and waiting for member reports.
	RoundPending RoundStatus = "pending"
	// RoundCompleted means every frozen member reported and a manifest exists.
	RoundCompleted RoundStatus = "completed"
	// RoundAborted means the round was aborted before completion.
	RoundAborted RoundStatus = "aborted"
	// RoundTimedOut means the deadline elapsed before completion.
	RoundTimedOut RoundStatus = "timed_out"
)

// Terminal reports whether the status is a final state.
func (s RoundStatus) Terminal() bool {
	switch s {
	case RoundCompleted, RoundAborted, RoundTimedOut:
		return true
	default:
		return false
	}
}

// Task is a registered distributed task with its fixed member set.
type Task struct {
	// ID uniquely identifies the task.
	ID string
	// Members are the task's participating members. The slice is sorted and
	// deduplicated at registration time and never changes afterwards.
	Members []string
	// CreatedAt is the wall-clock time the task was registered (unix millis).
	CreatedAt int64
}

// Report is one member's report for one round.
type Report struct {
	// TaskID is the task the report belongs to.
	TaskID string
	// Round is the round number the report belongs to.
	Round int64
	// Member is the reporting member's identifier.
	Member string
	// Position is the member's processing position (e.g. an offset).
	Position int64
	// Digest is the status summary of the member's processed state.
	Digest string
	// ReportedAt is the wall-clock time the report was accepted (unix millis).
	ReportedAt int64
}

// Manifest is the immutable materialisation of a completed checkpoint round.
//
// A manifest is created exactly once per completed round; reopening the store
// or replaying reports never produces a second manifest for the same round.
type Manifest struct {
	// ID uniquely identifies the manifest.
	ID string
	// TaskID is the task this checkpoint belongs to.
	TaskID string
	// Round is the completed round number.
	Round int64
	// Members is the frozen member set the round waited for (sorted).
	Members []string
	// Reports holds every member report included in the checkpoint, sorted by
	// member. The slice and its contents never change after creation.
	Reports []Report
	// CreatedAt is the wall-clock time the manifest was created (unix millis).
	CreatedAt int64
}

// Notification announces that a round reached a terminal state.
//
// For a completed round the notification references the manifest. Each round
// produces exactly one notification.
type Notification struct {
	// ID uniquely identifies the notification.
	ID string
	// TaskID is the task the round belongs to.
	TaskID string
	// Round is the round that reached a terminal state.
	Round int64
	// Status is the terminal status of the round.
	Status RoundStatus
	// ManifestID references the manifest when Status is RoundCompleted and is
	// empty otherwise.
	ManifestID string
	// CreatedAt is the wall-clock time the notification was emitted (unix millis).
	CreatedAt int64
}

// Round is the full persisted state of one checkpoint attempt.
type Round struct {
	// TaskID is the owning task.
	TaskID string
	// Number is the monotonically increasing round number within the task.
	Number int64
	// Status is the current lifecycle status.
	Status RoundStatus
	// FrozenMembers is the member set the round waits for, captured when the
	// round was opened and never mutated afterwards (sorted).
	FrozenMembers []string
	// Reports holds accepted reports keyed by member.
	Reports map[string]Report
	// Deadline is the wall-clock time after which the round times out while
	// still pending (unix millis). A zero value means no deadline.
	Deadline int64
	// Reason is recorded for failed (aborted/timed out) rounds.
	Reason string
	// ManifestID is set exactly once when the round completes.
	ManifestID string
	// NotificationID is set exactly once when a terminal state is reached.
	NotificationID string
	// CreatedAt is the wall-clock time the round was opened (unix millis).
	CreatedAt int64
	// CompletedAt is the wall-clock time the round reached its terminal state.
	CompletedAt int64
}

// snapshot is the on-disk representation of all coordinator state.
type snapshot struct {
	Tasks     map[string]*Task
	Rounds    []*Round // ascending by (task id, round number)
	Manifests map[string]*Manifest
	// Notifications are stored per task in ascending round order.
	Notifications map[string][]*Notification
	// Branches holds every processing branch keyed by branch id.
	Branches map[string]*Branch
	// Merges holds every merge request (successful and failed) keyed by
	// caller-supplied merge id.
	Merges map[string]*MergeRequest
	// MainlineVersions holds every mainline version produced by a successful
	// merge, keyed by version id.
	MainlineVersions map[string]*MainlineVersion
	// MainlineHead maps a task id to its currently effective mainline
	// version id. Exactly one mainline version is effective per task.
	MainlineHead map[string]string
	// MergeSeq assigns each task a monotonically increasing mainline
	// generation, advanced once per successful merge.
	MergeSeq map[string]int64
}

func newSnapshot() *snapshot {
	return &snapshot{
		Tasks:            map[string]*Task{},
		Manifests:        map[string]*Manifest{},
		Notifications:    map[string][]*Notification{},
		Branches:         map[string]*Branch{},
		Merges:           map[string]*MergeRequest{},
		MainlineVersions: map[string]*MainlineVersion{},
		MainlineHead:     map[string]string{},
		MergeSeq:         map[string]int64{},
	}
}

// Range is a half-open processing interval [Start, End) claimed by a branch.
// Two ranges overlap when each one starts strictly before the other ends.
type Range struct {
	// Start is the inclusive beginning of the processing interval.
	Start int64
	// End is the exclusive end of the processing interval.
	End int64
}

// Valid reports whether the range is well formed (Start <= End).
func (r Range) Valid() bool {
	return r.Start <= r.End
}

// Overlaps reports whether two half-open ranges share any position.
func (r Range) Overlaps(other Range) bool {
	return r.Start < other.End && other.Start < r.End
}

// BranchStatus is the lifecycle state of a processing branch.
type BranchStatus string

const (
	// BranchActive means the branch can still report and be merged.
	BranchActive BranchStatus = "active"
	// BranchMerged means the branch was folded into a mainline version;
	// it accepts no further reports.
	BranchMerged BranchStatus = "merged"
)

// Branch is one processing branch forked from a confirmed checkpoint.
//
// A branch records its parent checkpoint, its branch number within the task,
// the processing range it owns and its current generation. The generation
// starts at 1 and advances on every accepted branch report, so merges can
// detect that a branch moved between requests.
type Branch struct {
	// ID uniquely identifies the branch and is derived deterministically from
	// the task, parent checkpoint and branch number.
	ID string
	// TaskID is the owning task.
	TaskID string
	// ParentCheckpoint is the manifest id of the confirmed checkpoint the
	// branch forked from. It is the branch's common-prefix anchor.
	ParentCheckpoint string
	// Number is the branch's number within the task.
	Number int
	// Range is the processing interval owned by the branch.
	Range Range
	// Generation is the branch's current generation (1 at creation,
	// incremented per accepted report).
	Generation int64
	// Status is active or merged.
	Status BranchStatus
	// PrefixDigest is the common-prefix digest confirmed by the branch's most
	// recent report. It is empty until the first report.
	PrefixDigest string
	// TipPosition / TipDigest summarise the branch tip from the most recent
	// report.
	TipPosition int64
	TipDigest   string
	// MergeID is set when the branch becomes merged.
	MergeID string
	// CreatedAt is the wall-clock time the branch was created (unix millis).
	CreatedAt int64
	// UpdatedAt is the wall-clock time of the last accepted report (unix
	// millis); equals CreatedAt before the first report.
	UpdatedAt int64
}

// MergeStatus is the outcome of a merge request.
type MergeStatus string

const (
	// MergeMerged means the branches were folded into a new mainline version.
	MergeMerged MergeStatus = "merged"
	// MergeConflicted means validation failed; the branches are preserved.
	MergeConflicted MergeStatus = "conflicted"
)

// ConflictKind enumerates the concrete conflict types reported by a merge.
type ConflictKind string

const (
	// ConflictPrefix means the branches' confirmed common prefixes diverge.
	ConflictPrefix ConflictKind = "prefix"
	// ConflictRange means the branches' processing ranges overlap.
	ConflictRange ConflictKind = "range"
)

// Conflict pinpoints one concrete disagreement between two merge candidates.
// Conflicts are never silent: Location carries the exact offending prefixes or
// the exact overlapping interval.
type Conflict struct {
	// Kind is ConflictPrefix or ConflictRange.
	Kind ConflictKind
	// BranchA / BranchB are the conflicting branch ids.
	BranchA string
	BranchB string
	// Location pinpoints the conflict: for prefix conflicts the two confirmed
	// prefix digests; for range conflicts the overlapping interval.
	Location string
	// Detail is a human-readable description of the conflict.
	Detail string
}

// MergeRequest is the persisted record of one merge application.
//
// It is keyed by the caller-supplied merge id, which makes merge submission
// idempotent: resubmitting the exact same branches/ranges/generations returns
// the original result, while any change is rejected as a conflict.
type MergeRequest struct {
	// MergeID is the caller-supplied idempotency key.
	MergeID string
	// TaskID is the owning task.
	TaskID string
	// BranchA / BranchB are the candidate branch ids (stored sorted).
	BranchA string
	BranchB string
	// ParentCheckpoint is the common prefix required by the request.
	ParentCheckpoint string
	// RangeA / RangeB are the ranges captured at submission.
	RangeA Range
	RangeB Range
	// GenerationA / GenerationB are the branch generations captured at
	// submission; they are part of the request fingerprint.
	GenerationA int64
	GenerationB int64
	// PrefixA / PrefixB are the prefix digests confirmed at submission.
	PrefixA string
	PrefixB string
	// Status is merged or conflicted.
	Status MergeStatus
	// Conflicts holds every detected disagreement for conflicted requests.
	Conflicts []Conflict
	// MainlineVersionID is set exactly once when the merge succeeds.
	MainlineVersionID string
	// CreatedAt is the wall-clock time of the first submission (unix millis).
	CreatedAt int64
}

// MainlineVersion is the one effective mainline version produced by a
// successful merge. Its id is derived deterministically from the merge inputs,
// so the same merge can never materialise two different mainline versions.
type MainlineVersion struct {
	// ID uniquely identifies the mainline version.
	ID string
	// TaskID is the owning task.
	TaskID string
	// ParentCheckpoint is the common-prefix manifest id the version extends.
	ParentCheckpoint string
	// MergeID is the idempotency key of the merge that produced the version.
	MergeID string
	// BranchA / BranchB are the merged branch ids (sorted).
	BranchA string
	BranchB string
	// Ranges are the merged branches' non-overlapping ranges, sorted by start.
	Ranges []Range
	// Generation is the task-wide mainline generation (1 for the first
	// successful merge, then monotonically increasing).
	Generation int64
	// CreatedAt is the wall-clock time the version became effective.
	CreatedAt int64
}

// cloneMembers returns a sorted, deduplicated copy of members. ok is false when
// the input is empty.
func cloneMembers(members []string) ([]string, bool) {
	if len(members) == 0 {
		return nil, false
	}
	seen := make(map[string]struct{}, len(members))
	out := make([]string, 0, len(members))
	for _, m := range members {
		if _, ok := seen[m]; ok {
			continue
		}
		seen[m] = struct{}{}
		out = append(out, m)
	}
	sort.Strings(out)
	return out, true
}

// manifestID derives a deterministic, content-derived identifier for a
// completed round. The same reports for the same task/round always produce the
// same ID, which keeps completion idempotent across persistence reloads.
func manifestID(taskID string, round int64, reports map[string]Report) string {
	h := sha256.New()
	h.Write([]byte("manifest\n"))
	h.Write([]byte(taskID))
	h.Write([]byte{'\n'})
	var buf [8]byte
	putInt64(buf[:], round)
	h.Write(buf[:])
	members := make([]string, 0, len(reports))
	for m := range reports {
		members = append(members, m)
	}
	sort.Strings(members)
	for _, m := range members {
		r := reports[m]
		h.Write([]byte(m))
		h.Write([]byte{'\n'})
		putInt64(buf[:], r.Position)
		h.Write(buf[:])
		h.Write([]byte(r.Digest))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// putInt64 writes v as a big-endian 8-byte value.
func putInt64(buf []byte, v int64) {
	binary.BigEndian.PutUint64(buf, uint64(v))
}
