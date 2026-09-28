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
}

func newSnapshot() *snapshot {
	return &snapshot{
		Tasks:         map[string]*Task{},
		Manifests:     map[string]*Manifest{},
		Notifications: map[string][]*Notification{},
	}
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
