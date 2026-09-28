package gocheckpointcoordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// reportState 记录单个成员在某轮的首次上报内容。
type reportState struct {
	Position   int64     `json:"position"`
	Digest     string    `json:"digest"`
	ReportedAt time.Time `json:"reported_at"`
}

// roundState 是一轮检查点的内部持久化状态。
type roundState struct {
	Round     int64                   `json:"round"`
	Status    RoundStatus             `json:"status"`
	Waiting   []string                `json:"waiting"` // 已冻结的等待集合
	Reports   map[string]*reportState `json:"reports"` // 仅记录冻结集合内成员的首次上报
	Deadline  time.Time               `json:"deadline"`
	CreatedAt time.Time               `json:"created_at"`
	EndedAt   time.Time               `json:"ended_at"`
	Manifest  *Manifest               `json:"manifest,omitempty"`
	Reason    string                  `json:"reason,omitempty"`
}

// taskState 是任务的内部持久化状态。
type taskState struct {
	ID        string                `json:"id"`
	Name      string                `json:"name"`
	Members   []string              `json:"members"`
	CreatedAt time.Time             `json:"created_at"`
	NextRound int64                 `json:"next_round"`
	Rounds    map[int64]*roundState `json:"rounds"`
}

// snapshot 是持久化到磁盘的完整状态快照。
type snapshot struct {
	Version int                   `json:"version"`
	SavedAt time.Time             `json:"saved_at"`
	Tasks   map[string]*taskState `json:"tasks"`
}

const snapshotVersion = 1

// store 抽象状态持久化。Coordinator 在互斥保护下把同一快照交给 Save，
// 保证“状态变更”和“落盘”在临界区内顺序完成。
type store interface {
	load() (*snapshot, error)
	save(snap *snapshot) error
}

// memoryStore 用于不需要落盘的场景（例如测试）。
type memoryStore struct{}

func (memoryStore) load() (*snapshot, error) { return nil, nil }
func (memoryStore) save(*snapshot) error     { return nil }

// fileStore 以 JSON 快照文件持久化全部状态。
// 写入采用“临时文件 + fsync + rename”的原子替换方式，
// 崩溃后要么看到旧快照、要么看到新快照，绝不会出现半份文件。
type fileStore struct {
	mu   sync.Mutex
	path string
}

func newFileStore(path string) *fileStore {
	return &fileStore{path: path}
}

func (f *fileStore) load() (*snapshot, error) {
	data, err := os.ReadFile(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read snapshot %q: %w", f.path, err)
	}
	if len(data) == 0 {
		return nil, nil
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("decode snapshot %q: %w", f.path, err)
	}
	return &snap, nil
}

func (f *fileStore) save(snap *snapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if dir := filepath.Dir(f.path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create snapshot dir: %w", err)
		}
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}

	tmp := f.path + ".tmp"
	fh, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create temp snapshot: %w", err)
	}
	if _, err := fh.Write(data); err != nil {
		fh.Close()
		os.Remove(tmp)
		return fmt.Errorf("write temp snapshot: %w", err)
	}
	if err := fh.Sync(); err != nil {
		fh.Close()
		os.Remove(tmp)
		return fmt.Errorf("fsync temp snapshot: %w", err)
	}
	if err := fh.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close temp snapshot: %w", err)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename snapshot: %w", err)
	}
	return nil
}
