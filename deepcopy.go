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
