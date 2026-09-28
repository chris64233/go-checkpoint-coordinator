package gocheckpointcoordinator

import (
	"crypto/rand"
	"encoding/hex"
	"sort"
	"sync"
	"time"
)

// Clock returns the current wall-clock time. Tests substitute a controllable
// clock to exercise deadline behaviour deterministically.
type Clock func() time.Time

// Options configures a Coordinator.
type Options struct {
	// Clock supplies the current time. Defaults to time.Now.
	Clock Clock
}

// ReportResult is returned by Report.
type ReportResult struct {
	// AlreadyReported is true when this call repeated a member's identical
	// earlier report for the round; the existing outcome is returned.
	AlreadyReported bool
	// Completed is true when this report made the round complete.
	Completed bool
	// Round is the round the report belongs to.
	Round Round
	// Manifest is present when the round completed.
	Manifest *Manifest
	// Notification is present when the round reached a terminal state.
	Notification *Notification
}

// Coordinator runs consistent checkpoint coordination for any number of tasks.
//
// All methods are safe for concurrent use. Every mutating operation takes a
// single global lock, applies the whole state transition and then persists
// atomically, so concurrent callers never observe partially applied results
// and the durable state never contradicts the in-memory state.
type Coordinator struct {
	mu    sync.Mutex
	store store
	now   Clock

	snap *snapshot
	// currentByTask maps a task id to its current (most recently opened) round.
	currentByTask map[string]*Round
}

// New opens a coordinator backed by a JSON file at path, creating it if needed.
func New(path string) (*Coordinator, error) {
	return NewWithStore(newFileStore(path), nil)
}

// NewInMemory returns a coordinator that does not touch disk. State survives
// only for the coordinator's lifetime; it is intended for tests.
func NewInMemory() *Coordinator {
	c, err := NewWithStore(newMemoryStore(), nil)
	if err != nil {
		panic(err) // memoryStore.load never fails
	}
	return c
}

// NewWithStore opens a coordinator with a custom store and options.
func NewWithStore(st store, opts *Options) (*Coordinator, error) {
	clock := time.Now
	if opts != nil && opts.Clock != nil {
		clock = opts.Clock
	}
	snap, err := st.load()
	if err != nil {
		return nil, err
	}
	c := &Coordinator{
		store:         st,
		now:           clock,
		snap:          snap,
		currentByTask: map[string]*Round{},
	}
	for _, r := range snap.Rounds {
		c.currentByTask[r.TaskID] = r // rounds are ascending: last one wins
		if r.Reports == nil {
			r.Reports = map[string]Report{}
		}
	}
	return c, nil
}

// RegisterTask registers a task with a fixed set of participating members.
//
// members are sorted and deduplicated; duplicates in the input are harmless.
// The member set is immutable for the task's lifetime — members temporarily
// leaving do not alter the completion condition of any round, because each
// round freezes its own copy of the set.
func (c *Coordinator) RegisterTask(taskID string, members []string) (*Task, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.snap.Tasks[taskID]; ok {
		return nil, ErrTaskExists
	}
	frozen, ok := cloneMembers(members)
	if !ok {
		return nil, ErrEmptyMembers
	}
	task := &Task{
		ID:        taskID,
		Members:   frozen,
		CreatedAt: c.now().UnixMilli(),
	}
	c.snap.Tasks[taskID] = task
	if err := c.persistLocked(); err != nil {
		delete(c.snap.Tasks, taskID)
		return nil, err
	}
	return cloneTask(task), nil
}

// StartCheckpoint opens a new checkpoint round for a task.
//
// The round number is one greater than the task's previous round and the
// member set to wait for is frozen from the task's registered members at this
// point. A new round cannot be opened while the previous one is still pending.
// timeout <= 0 means the round never times out on its own.
func (c *Coordinator) StartCheckpoint(taskID string, timeout time.Duration) (*Round, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	task, ok := c.snap.Tasks[taskID]
	if !ok {
		return nil, ErrTaskNotFound
	}
	if cur := c.currentByTask[taskID]; cur != nil && cur.Status == RoundPending {
		return nil, ErrRoundOpen
	}
	var nextNumber int64 = 1
	if cur := c.currentByTask[taskID]; cur != nil {
		nextNumber = cur.Number + 1
	}
	now := c.now()
	round := &Round{
		TaskID:        taskID,
		Number:        nextNumber,
		Status:        RoundPending,
		FrozenMembers: cloneStrings(task.Members),
		Reports:       map[string]Report{},
		CreatedAt:     now.UnixMilli(),
	}
	if timeout > 0 {
		round.Deadline = now.Add(timeout).UnixMilli()
	}
	c.snap.Rounds = append(c.snap.Rounds, round)
	c.currentByTask[taskID] = round
	if err := c.persistLocked(); err != nil {
		c.snap.Rounds = c.snap.Rounds[:len(c.snap.Rounds)-1]
		c.rewindCurrentLocked(taskID)
		return nil, err
	}
	return cloneRound(round), nil
}

// Report records a member's report for its task's current round.
//
// The report must carry the task id, round number, processing position and
// status digest. Semantics:
//
//   - Reports for a non-current (old) round are rejected with ErrStaleRound;
//     late reports can never mix into a later checkpoint.
//   - Members outside the round's frozen set are rejected with ErrUnknownMember.
//   - Re-reporting the same position and digest for the same round is
//     idempotent and returns the original result (AlreadyReported == true),
//     even after the round has completed.
//   - Re-reporting a different position or digest is rejected with ErrConflict.
//   - When the last required member reports, exactly one immutable manifest and
//     one notification are created atomically.
//   - Reports against aborted or timed out rounds fail with
//     ErrAlreadyTerminal; such rounds are failed and accept nothing.
func (c *Coordinator) Report(taskID string, round int64, member string, position int64, digest string) (*ReportResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	r, err := c.roundForReportLocked(taskID, round, member)
	if err != nil {
		return nil, err
	}

	now := c.now()

	// A completed round keeps accepting idempotent repeats of its members'
	// reports: resending the exact same position and digest returns the
	// original outcome (manifest and notification included) without creating
	// any new state. Different content is still a conflict; a report from a
	// frozen member that never reported before cannot be accepted.
	if r.Status == RoundCompleted {
		prev, ok := r.Reports[member]
		if !ok {
			return nil, ErrAlreadyTerminal
		}
		if prev.Position != position || prev.Digest != digest {
			return nil, ErrConflict
		}
		return c.buildReportResultLocked(r, true), nil
	}

	if r.Status.Terminal() {
		// Aborted and timed out rounds cannot accept reports under any
		// circumstances: they are failed rounds, not recovery points.
		return nil, ErrAlreadyTerminal
	}

	if r.Deadline != 0 && now.UnixMilli() >= r.Deadline {
		// The deadline has arrived: the last successful report and the timeout
		// race, and the timeout wins once the deadline is reached.
		c.failRoundLocked(r, RoundTimedOut, "deadline reached")
		if err := c.persistLocked(); err != nil {
			c.reloadAfterPersistErrorLocked(err)
			return nil, err
		}
		return nil, ErrAlreadyTerminal
	}

	if prev, ok := r.Reports[member]; ok {
		if prev.Position == position && prev.Digest == digest {
			return c.buildReportResultLocked(r, true), nil
		}
		return nil, ErrConflict
	}

	report := Report{
		TaskID:     taskID,
		Round:      round,
		Member:     member,
		Position:   position,
		Digest:     digest,
		ReportedAt: now.UnixMilli(),
	}
	r.Reports[member] = report

	if allReported(r) {
		c.completeLocked(r, now.UnixMilli())
	}

	if err := c.persistLocked(); err != nil {
		c.reloadAfterPersistErrorLocked(err)
		return nil, err
	}
	return c.buildReportResultLocked(r, false), nil
}

// AbortCheckpoint aborts a task's current round with a recorded reason.
//
// Aborting an already terminal round returns ErrAlreadyTerminal. An abort and
// the last successful report are serialised by the coordinator lock, so a
// round ends either completed or aborted, never both; aborted rounds can never
// be used as recovery points.
func (c *Coordinator) AbortCheckpoint(taskID string, reason string) (*Round, *Notification, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	r := c.currentByTask[taskID]
	if r == nil {
		return nil, nil, ErrNoRound
	}
	if r.Status.Terminal() {
		return nil, nil, ErrAlreadyTerminal
	}
	c.failRoundLocked(r, RoundAborted, reason)
	if err := c.persistLocked(); err != nil {
		c.reloadAfterPersistErrorLocked(err)
		return nil, nil, err
	}
	return cloneRound(r), cloneNotification(c.notificationForLocked(taskID, r.Number)), nil
}

// AdvanceTimeouts transitions every pending round whose deadline has passed to
// timed out. It returns the rounds that transitioned (each with its
// notification), in task-id/round-number order.
//
// Deadlines are also evaluated lazily on Report, but callers with no further
// reports coming can drive timeouts explicitly. All transitions in one call
// are persisted together.
func (c *Coordinator) AdvanceTimeouts() ([]Round, []Notification, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now().UnixMilli()
	var timedOut []*Round
	for _, r := range c.snap.Rounds {
		if r.Status == RoundPending && r.Deadline != 0 && now >= r.Deadline {
			c.failRoundLocked(r, RoundTimedOut, "deadline reached")
			timedOut = append(timedOut, r)
		}
	}
	if len(timedOut) > 0 {
		if err := c.persistLocked(); err != nil {
			c.reloadAfterPersistErrorLocked(err)
			return nil, nil, err
		}
	}
	rounds := make([]Round, len(timedOut))
	notes := make([]Notification, len(timedOut))
	for i, r := range timedOut {
		rounds[i] = *cloneRound(r)
		notes[i] = *cloneNotification(c.notificationForLocked(r.TaskID, r.Number))
	}
	return rounds, notes, nil
}

// GetLatestRecoverableCheckpoint returns the manifest of the task's latest
// completed round, or nil when no round has completed.
//
// Aborted and timed out rounds are not recovery points, and a pending round is
// never visible even if some (or almost all) members have reported.
func (c *Coordinator) GetLatestRecoverableCheckpoint(taskID string) (*Manifest, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.snap.Tasks[taskID]; !ok {
		return nil, ErrTaskNotFound
	}
	for i := len(c.snap.Rounds) - 1; i >= 0; i-- {
		r := c.snap.Rounds[i]
		if r.TaskID != taskID {
			continue
		}
		if r.Status == RoundCompleted {
			return cloneManifest(c.snap.Manifests[r.ManifestID]), nil
		}
		// Earlier completed rounds may exist after a later failed round.
	}
	return nil, nil
}

// GetCheckpointHistory returns all rounds of a task in ascending round order
// with their manifests and notifications.
type HistoryEntry struct {
	Round        Round
	Manifest     *Manifest     // nil unless the round completed
	Notification *Notification // always present for terminal rounds
}

// GetCheckpointHistory returns the full round history of a task in ascending
// round order.
func (c *Coordinator) GetCheckpointHistory(taskID string) ([]HistoryEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.snap.Tasks[taskID]; !ok {
		return nil, ErrTaskNotFound
	}
	var out []HistoryEntry
	for _, r := range c.snap.Rounds {
		if r.TaskID != taskID {
			continue
		}
		entry := HistoryEntry{Round: *cloneRound(r)}
		if r.ManifestID != "" {
			entry.Manifest = cloneManifest(c.snap.Manifests[r.ManifestID])
		}
		entry.Notification = cloneNotification(c.notificationForLocked(taskID, r.Number))
		out = append(out, entry)
	}
	return out, nil
}

// GetTask returns a registered task by id.
func (c *Coordinator) GetTask(taskID string) (*Task, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	task, ok := c.snap.Tasks[taskID]
	if !ok {
		return nil, ErrTaskNotFound
	}
	return cloneTask(task), nil
}

// GetRound returns a task's current round.
func (c *Coordinator) GetRound(taskID string) (*Round, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.currentByTask[taskID]
	if r == nil {
		if _, ok := c.snap.Tasks[taskID]; !ok {
			return nil, ErrTaskNotFound
		}
		return nil, ErrNoRound
	}
	return cloneRound(r), nil
}

// GetManifest returns a manifest by id.
func (c *Coordinator) GetManifest(manifestID string) (*Manifest, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m, ok := c.snap.Manifests[manifestID]
	if !ok {
		return nil, ErrTaskNotFound
	}
	return cloneManifest(m), nil
}

// Notifications returns all notifications for a task in ascending round order.
func (c *Coordinator) Notifications(taskID string) ([]Notification, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.snap.Tasks[taskID]; !ok {
		return nil, ErrTaskNotFound
	}
	src := c.snap.Notifications[taskID]
	out := make([]Notification, len(src))
	for i, n := range src {
		out[i] = *cloneNotification(n)
	}
	return out, nil
}

// ---- internal helpers (caller must hold c.mu) ----

func (c *Coordinator) roundForReportLocked(taskID string, round int64, member string) (*Round, error) {
	if _, ok := c.snap.Tasks[taskID]; !ok {
		return nil, ErrTaskNotFound
	}
	r := c.currentByTask[taskID]
	if r == nil || r.Number != round {
		return nil, ErrStaleRound
	}
	if !containsString(r.FrozenMembers, member) {
		return nil, ErrUnknownMember
	}
	return r, nil
}

// completeLocked materialises the manifest and completion notification for a
// pending round whose frozen members have all reported. It is called only while
// applying the transition under the lock, so the manifest/notification pair is
// created exactly once and cannot interleave with an abort or timeout.
func (c *Coordinator) completeLocked(r *Round, atMillis int64) {
	r.Status = RoundCompleted
	r.CompletedAt = atMillis

	members := make([]string, 0, len(r.Reports))
	reports := make([]Report, 0, len(r.Reports))
	for m := range r.Reports {
		members = append(members, m)
	}
	sort.Strings(members)
	for _, m := range members {
		reports = append(reports, r.Reports[m])
	}

	id := manifestID(r.TaskID, r.Number, r.Reports)
	manifest := &Manifest{
		ID:        id,
		TaskID:    r.TaskID,
		Round:     r.Number,
		Members:   cloneStrings(r.FrozenMembers),
		Reports:   reports,
		CreatedAt: atMillis,
	}
	c.snap.Manifests[id] = manifest
	r.ManifestID = id
	c.emitNotificationLocked(r, id)
}

// failRoundLocked moves a pending round to a failed terminal state (aborted or
// timed out) and emits its single notification. No manifest is created.
func (c *Coordinator) failRoundLocked(r *Round, status RoundStatus, reason string) {
	r.Status = status
	r.Reason = reason
	r.CompletedAt = c.now().UnixMilli()
	c.emitNotificationLocked(r, "")
}

func (c *Coordinator) emitNotificationLocked(r *Round, manifestID string) {
	id, err := randomID()
	if err != nil {
		panic("cannot generate notification id: " + err.Error())
	}
	n := &Notification{
		ID:         id,
		TaskID:     r.TaskID,
		Round:      r.Number,
		Status:     r.Status,
		ManifestID: manifestID,
		CreatedAt:  r.CompletedAt,
	}
	r.NotificationID = id
	c.snap.Notifications[r.TaskID] = append(c.snap.Notifications[r.TaskID], n)
}

func (c *Coordinator) notificationForLocked(taskID string, round int64) *Notification {
	for _, n := range c.snap.Notifications[taskID] {
		if n.Round == round {
			return n
		}
	}
	return nil
}

func (c *Coordinator) buildReportResultLocked(r *Round, duplicate bool) *ReportResult {
	res := &ReportResult{
		AlreadyReported: duplicate,
		Round:           *cloneRound(r),
	}
	if r.Status.Terminal() {
		res.Completed = r.Status == RoundCompleted
		res.Notification = cloneNotification(c.notificationForLocked(r.TaskID, r.Number))
		if r.ManifestID != "" {
			res.Manifest = cloneManifest(c.snap.Manifests[r.ManifestID])
		}
	}
	return res
}

// persistLocked saves the current snapshot. On error the in-memory state may
// have diverged from disk, so callers must roll back (reload) before
// releasing the lock.
func (c *Coordinator) persistLocked() error {
	return c.store.save(c.snap)
}

// reloadAfterPersistErrorLocked restores in-memory state from the store after a
// failed save, so the coordinator never serves a state that was not durably
// committed. If the reload itself fails the process state is unreliable; the
// error from the original save is returned to the caller.
func (c *Coordinator) reloadAfterPersistErrorLocked(saveErr error) {
	snap, err := c.store.load()
	if err != nil {
		panic("checkpoint store failed to save and reload: " + saveErr.Error() + "; reload error: " + err.Error())
	}
	c.snap = snap
	c.currentByTask = map[string]*Round{}
	for _, r := range snap.Rounds {
		c.currentByTask[r.TaskID] = r
		if r.Reports == nil {
			r.Reports = map[string]Report{}
		}
	}
}

// rewindCurrentLocked recomputes the current round for a task, used when
// rolling back a failed StartCheckpoint append.
func (c *Coordinator) rewindCurrentLocked(taskID string) {
	var last *Round
	for _, r := range c.snap.Rounds {
		if r.TaskID == taskID {
			last = r
		}
	}
	if last == nil {
		delete(c.currentByTask, taskID)
	} else {
		c.currentByTask[taskID] = last
	}
}

func allReported(r *Round) bool {
	for _, m := range r.FrozenMembers {
		if _, ok := r.Reports[m]; !ok {
			return false
		}
	}
	return true
}

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
