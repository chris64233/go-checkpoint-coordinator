package gocheckpointcoordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// store persists and restores coordinator snapshots.
type store interface {
	// load returns the most recently saved snapshot, or an empty snapshot if
	// nothing has been persisted yet.
	load() (*snapshot, error)
	// save atomically replaces the persisted state with snap.
	save(snap *snapshot) error
}

// fileStore persists state as a single JSON file. Writes go to a temporary
// file in the same directory which is fsynced and then renamed over the target,
// so the file is never observed half written.
type fileStore struct {
	path string
}

func newFileStore(path string) *fileStore {
	return &fileStore{path: path}
}

// persistedSnapshot is the JSON encoding of a snapshot.
type persistedSnapshot struct {
	Version       int                        `json:"version"`
	Tasks         map[string]*Task           `json:"tasks"`
	Rounds        []*Round                   `json:"rounds"`
	Manifests     map[string]*Manifest       `json:"manifests"`
	Notifications map[string][]*Notification `json:"notifications"`
}

func (s *fileStore) load() (*snapshot, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return newSnapshot(), nil
		}
		return nil, fmt.Errorf("read checkpoint state: %w", err)
	}
	var p persistedSnapshot
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("decode checkpoint state: %w", err)
	}
	snap := newSnapshot()
	if p.Tasks != nil {
		snap.Tasks = p.Tasks
	}
	if p.Manifests != nil {
		snap.Manifests = p.Manifests
	}
	if p.Notifications != nil {
		snap.Notifications = p.Notifications
	}
	snap.Rounds = p.Rounds
	return snap, nil
}

func (s *fileStore) save(snap *snapshot) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}
	p := persistedSnapshot{
		Version:       1,
		Tasks:         snap.Tasks,
		Rounds:        snap.Rounds,
		Manifests:     snap.Manifests,
		Notifications: snap.Notifications,
	}
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return fmt.Errorf("encode checkpoint state: %w", err)
	}

	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".checkpoint-state-*")
	if err != nil {
		return fmt.Errorf("create temp state file: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("write checkpoint state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("flush checkpoint state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close checkpoint state: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		cleanup()
		return fmt.Errorf("commit checkpoint state: %w", err)
	}
	// Sync the directory so the rename itself is durable.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// memoryStore keeps state in memory and is mainly useful for tests that
// exercise concurrency without touching disk.
type memoryStore struct {
	mu   sync.Mutex
	snap *snapshot
}

func newMemoryStore() *memoryStore {
	return &memoryStore{snap: newSnapshot()}
}

func (m *memoryStore) load() (*snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.snap == nil {
		return newSnapshot(), nil
	}
	return cloneSnapshot(m.snap), nil
}

func (m *memoryStore) save(snap *snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snap = cloneSnapshot(snap)
	return nil
}
