package gocheckpointcoordinator

import "sort"

// backfillMainlineLocked rebuilds the mainline history, generation counters
// and branch counters from older snapshots that predate branch support. It is
// idempotent: snapshots already carrying mainline state are left untouched.
func (s *snapshot) backfillMainlineLocked() {
	if len(s.Mainline) > 0 {
		return
	}
	roundsByTask := map[string][]*Round{}
	for _, r := range s.Rounds {
		roundsByTask[r.TaskID] = append(roundsByTask[r.TaskID], r)
	}
	taskIDs := make([]string, 0, len(roundsByTask))
	for id := range roundsByTask {
		taskIDs = append(taskIDs, id)
	}
	sort.Strings(taskIDs)
	for _, taskID := range taskIDs {
		var gen int64
		var parentID string
		for _, r := range roundsByTask[taskID] {
			if r.Status != RoundCompleted || r.ManifestID == "" {
				continue
			}
			manifest := s.Manifests[r.ManifestID]
			if manifest == nil {
				continue
			}
			gen++
			reports := make([]Report, len(manifest.Reports))
			copy(reports, manifest.Reports)
			cp := &MainlineCheckpoint{
				ID:         manifest.ID,
				TaskID:     taskID,
				Generation: gen,
				ParentID:   parentID,
				ManifestID: manifest.ID,
				Round:      r.Number,
				Source:     "round",
				Reports:    reports,
				CreatedAt:  r.CompletedAt,
			}
			s.Mainline[taskID] = append(s.Mainline[taskID], cp)
			parentID = cp.ID
		}
		s.MainlineGen[taskID] = gen
		var nextNum int64
		for _, b := range s.Branches[taskID] {
			if b.Number > nextNum {
				nextNum = b.Number
			}
		}
		s.NextBranchNum[taskID] = nextNum
	}
}
