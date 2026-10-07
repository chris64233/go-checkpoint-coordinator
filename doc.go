// Package gocheckpointcoordinator implements a coordinator for consistent,
// durable checkpoints of distributed tasks.
//
// A task registers a fixed set of participating members. Each checkpoint
// attempt runs in its own round: the round number increases monotonically and,
// at round creation, the set of members the round waits for is frozen. Members
// report their processing position together with a status digest. Once every
// frozen member has reported successfully, the coordinator materialises a
// single immutable checkpoint manifest and a unique completion notification.
// A round may instead finish aborted or timed out; in every case a round has
// exactly one terminal state, and only completed rounds can be used as recovery
// points.
//
// All state is persisted atomically to disk after every mutation, so a
// coordinator can be reopened at the same path and resume.
//
// A confirmed checkpoint can fork processing branches (CreateBranch): each
// branch records its parent checkpoint, a branch number, its processing range
// and a generation that advances on every branch report. Branches can only be
// created from completed checkpoints. RequestMerge folds two branches back into
// the main line after reconfirming a shared common prefix and disjoint
// ranges; every disagreement is reported as a concrete conflict rather than
// silently overwritten. A caller-supplied merge id makes submissions
// idempotent, a successful merge publishes exactly one mainline version, and
// merged branches reject further receipts so stale branch reports can never be
// written back to the main line.
package gocheckpointcoordinator
