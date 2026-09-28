package gocheckpointcoordinator

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestNewInMemoryBasics(t *testing.T) {
	co := NewInMemory()
	if _, err := co.GetTask("nope"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("want ErrTaskNotFound, got %v", err)
	}
	if _, err := co.RegisterTask("t1", []string{"a"}); err != nil {
		t.Fatal(err)
	}
	if r, err := co.StartCheckpoint("t1", 0); err != nil || r.Number != 1 {
		t.Fatalf("start: %+v %v", r, err)
	}
}

func TestLookupErrorBranches(t *testing.T) {
	co := NewInMemory()

	if _, err := co.GetTask("x"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("GetTask: %v", err)
	}
	if _, err := co.GetRound("x"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("GetRound unknown task: %v", err)
	}
	if _, err := co.GetLatestRecoverableCheckpoint("x"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("GetLatestRecoverableCheckpoint: %v", err)
	}
	if _, err := co.GetCheckpointHistory("x"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("GetCheckpointHistory: %v", err)
	}
	if _, err := co.Notifications("x"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("Notifications: %v", err)
	}
	if _, err := co.GetManifest("nope"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("GetManifest: %v", err)
	}
	if _, _, err := co.AbortCheckpoint("x", "r"); !errors.Is(err, ErrNoRound) {
		t.Fatalf("abort without rounds: %v", err)
	}

	mustRegister(t, co, "t1", "a")
	if r, err := co.GetRound("t1"); err != ErrNoRound || r != nil {
		t.Fatalf("GetRound before first round: %v %v", r, err)
	}
	if n, err := co.Notifications("t1"); err != nil || len(n) != 0 {
		t.Fatalf("notifications before any round: %v %v", n, err)
	}

	mustStart(t, co, "t1", 0)
	if _, err := co.Report("ghost", 1, "a", 1, "d"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("report unknown task: %v", err)
	}
	if _, err := co.Report("t1", 2, "a", 1, "d"); !errors.Is(err, ErrStaleRound) {
		t.Fatalf("report future round: %v", err)
	}
}

// failingStore fails every save but loads successfully, exercising the
// rollback/reload paths after a persistence error.
type failingStore struct {
	mu      sync.Mutex
	snap    *snapshot
	saveErr error
	saveCnt int
}

func (f *failingStore) load() (*snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.snap == nil {
		return newSnapshot(), nil
	}
	return cloneSnapshot(f.snap), nil
}

func (f *failingStore) save(snap *snapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saveCnt++
	return f.saveErr
}

func TestPersistenceFailureRollsBack(t *testing.T) {
	st := &failingStore{saveErr: errors.New("disk on fire")}
	co, err := NewWithStore(st, nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := co.RegisterTask("t1", []string{"a"}); err == nil {
		t.Fatal("register must surface the persistence error")
	}
	if _, err := co.GetTask("t1"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("failed registration must roll back, got %v", err)
	}

	// Seed durable state directly, then fail every subsequent save.
	st.snap = &snapshot{
		Tasks: map[string]*Task{
			"t1": {ID: "t1", Members: []string{"a"}},
		},
		Manifests:     map[string]*Manifest{},
		Notifications: map[string][]*Notification{},
	}
	st.saveErr = errors.New("disk on fire")
	co2, err := NewWithStore(st, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := co2.StartCheckpoint("t1", 0); err == nil {
		t.Fatal("start must surface the persistence error")
	}
	if r, err := co2.GetRound("t1"); !errors.Is(err, ErrNoRound) {
		t.Fatalf("failed start must roll back: r=%v err=%v", r, err)
	}

	// Manually open a round in durable state so later operations fail too.
	st.mu.Lock()
	st.snap.Rounds = append(st.snap.Rounds, &Round{
		TaskID: "t1", Number: 1, Status: RoundPending,
		FrozenMembers: []string{"a"},
		Reports:       map[string]Report{},
	})
	st.mu.Unlock()
	co3, err := NewWithStore(st, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := co3.Report("t1", 1, "a", 1, "d"); err == nil {
		t.Fatal("report must surface the persistence error")
	}
	r, err := co3.GetRound("t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Reports) != 0 {
		t.Fatalf("failed report must roll back: %d reports persisted in memory", len(r.Reports))
	}
	if _, _, err := co3.AbortCheckpoint("t1", "x"); err == nil {
		t.Fatal("abort must surface the persistence error")
	}
	r, _ = co3.GetRound("t1")
	if r.Status != RoundPending {
		t.Fatalf("failed abort must roll back, status=%s", r.Status)
	}
}

func TestFileStoreCorruptAndUnwritable(t *testing.T) {
	dir := t.TempDir()

	// Corrupt JSON must produce a decode error rather than a panic.
	path := filepath.Join(dir, "corrupt.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(path); err == nil {
		t.Fatal("corrupt state file must fail to open")
	}

	// A state path beneath a regular file cannot be read or written.
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(filepath.Join(blocker, "state.json")); err == nil {
		t.Fatal("opening a state path beneath a regular file must fail")
	}

	// A state path that is itself a directory also fails to load.
	stateAsDir := filepath.Join(dir, "statedir")
	if err := os.Mkdir(stateAsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := New(stateAsDir); err == nil {
		t.Fatal("opening a state path that is a directory must fail")
	}
}
