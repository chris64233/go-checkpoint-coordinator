package gocheckpointcoordinator

import "sort"

// All values returned to callers are deep copies: callers can mutate them
// without affecting coordinator state, and coordinator internals can evolve
// after a value has been handed out without aliasing.

func cloneStrings(in []string) []string {
	if in == nil {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func cloneTask(t *Task) *Task {
	if t == nil {
		return nil
	}
	cp := *t
	cp.Members = cloneStrings(t.Members)
	return &cp
}

func cloneReport(r Report) Report {
	return r // Report contains only value types
}

func cloneRound(r *Round) *Round {
	if r == nil {
		return nil
	}
	cp := *r
	cp.FrozenMembers = cloneStrings(r.FrozenMembers)
	if r.Reports != nil {
		cp.Reports = make(map[string]Report, len(r.Reports))
		for k, v := range r.Reports {
			cp.Reports[k] = cloneReport(v)
		}
	}
	return &cp
}

func cloneManifest(m *Manifest) *Manifest {
	if m == nil {
		return nil
	}
	cp := *m
	cp.Members = cloneStrings(m.Members)
	if m.Reports != nil {
		cp.Reports = make([]Report, len(m.Reports))
		copy(cp.Reports, m.Reports)
	}
	return &cp
}

func cloneNotification(n *Notification) *Notification {
	if n == nil {
		return nil
	}
	cp := *n
	return &cp
}

func cloneBranch(b *Branch) *Branch {
	if b == nil {
		return nil
	}
	cp := *b
	cp.FrozenMembers = cloneStrings(b.FrozenMembers)
	if b.Reports != nil {
		cp.Reports = make(map[string]Report, len(b.Reports))
		for k, v := range b.Reports {
			cp.Reports[k] = cloneReport(v)
		}
	}
	return &cp
}

func cloneMainline(cp *MainlineCheckpoint) *MainlineCheckpoint {
	if cp == nil {
		return nil
	}
	out := *cp
	out.Reports = make([]Report, len(cp.Reports))
	copy(out.Reports, cp.Reports)
	out.Branches = cloneInt64s(cp.Branches)
	return &out
}

func cloneInt64s(in []int64) []int64 {
	if in == nil {
		return nil
	}
	out := make([]int64, len(in))
	copy(out, in)
	return out
}

func cloneConflicts(in []MergeConflict) []MergeConflict {
	if in == nil {
		return nil
	}
	out := make([]MergeConflict, len(in))
	copy(out, in)
	return out
}

func cloneMerge(m *MergeRecord) *MergeRecord {
	if m == nil {
		return nil
	}
	cp := *m
	cp.Conflicts = cloneConflicts(m.Conflicts)
	return &cp
}

// cloneSnapshot deep-copies every reachable part of snap.
func cloneSnapshot(snap *snapshot) *snapshot {
	cp := newSnapshot()
	for id, t := range snap.Tasks {
		cp.Tasks[id] = cloneTask(t)
	}
	for _, r := range snap.Rounds {
		cp.Rounds = append(cp.Rounds, cloneRound(r))
	}
	for id, m := range snap.Manifests {
		cp.Manifests[id] = cloneManifest(m)
	}
	for taskID, ns := range snap.Notifications {
		for _, n := range ns {
			cp.Notifications[taskID] = append(cp.Notifications[taskID], cloneNotification(n))
		}
		sort.Slice(cp.Notifications[taskID], func(i, j int) bool {
			return cp.Notifications[taskID][i].Round < cp.Notifications[taskID][j].Round
		})
	}
	for taskID, bs := range snap.Branches {
		for _, b := range bs {
			cp.Branches[taskID] = append(cp.Branches[taskID], cloneBranch(b))
		}
		sort.Slice(cp.Branches[taskID], func(i, j int) bool {
			return cp.Branches[taskID][i].Number < cp.Branches[taskID][j].Number
		})
	}
	for taskID, ms := range snap.Merges {
		if cp.Merges[taskID] == nil {
			cp.Merges[taskID] = map[string]*MergeRecord{}
		}
		for id, m := range ms {
			cp.Merges[taskID][id] = cloneMerge(m)
		}
	}
	for taskID, cps := range snap.Mainline {
		for _, m := range cps {
			cp.Mainline[taskID] = append(cp.Mainline[taskID], cloneMainline(m))
		}
		sort.Slice(cp.Mainline[taskID], func(i, j int) bool {
			return cp.Mainline[taskID][i].Generation < cp.Mainline[taskID][j].Generation
		})
		cp.MainlineGen[taskID] = snap.MainlineGen[taskID]
		cp.NextBranchNum[taskID] = snap.NextBranchNum[taskID]
	}
	return cp
}

func containsString(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
