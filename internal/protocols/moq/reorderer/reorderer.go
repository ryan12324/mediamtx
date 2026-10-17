// Package reorderer contains a subgroup reorderer.
package reorderer

import (
	"slices"
	"sync"
	"time"

	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/protocols/moq/subgroup"
)

const (
	maxPendingSubGroups = 50

	// maxPendingWait is how long a gap in the group sequence is waited for. A group can
	// be lost on the way, and groups can always arrive out of order, so a subscriber
	// cannot tell a late group from one that will never come (draft-ietf-moq-transport-19,
	// section 2.3.1). A live stream can't hold the later groups back for long.
	maxPendingWait = 500 * time.Millisecond
)

func subGroupPayloadSize(sg *subgroup.SubGroup) int {
	n := 0
	for _, obj := range sg.Objects {
		n += len(obj.Payload)
	}
	return n
}

// Reorderer is a subgroup reorderer.
type Reorderer struct {
	Parent *Orchestrator

	maxPendingSubGroups int
	maxPendingWait      time.Duration
	now                 func() time.Time

	initialized  bool
	pendingSince time.Time
	mu           sync.Mutex
	curGroupID   uint64
	pending      map[uint64]*subgroup.SubGroup
}

// Initialize initializes the reorderer.
func (r *Reorderer) Initialize() {
	if r.maxPendingSubGroups == 0 {
		r.maxPendingSubGroups = maxPendingSubGroups
	}
	if r.maxPendingWait == 0 {
		r.maxPendingWait = maxPendingWait
	}
	if r.now == nil {
		r.now = time.Now
	}

	r.pending = make(map[uint64]*subgroup.SubGroup)
}

// Push pushes a subgroup and returns a list of subgroups that are now in order.
// Groups are put in order; subgroups of the current group are passed on as they
// come, since a group can be split into several objects and read one object at a time.
func (r *Reorderer) Push(sg *subgroup.SubGroup) ([]*subgroup.SubGroup, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.initialized {
		r.initialized = true
		r.curGroupID = sg.Header.GroupID
		return []*subgroup.SubGroup{sg}, nil
	}

	switch {
	case sg.Header.GroupID == r.curGroupID:
		return []*subgroup.SubGroup{sg}, nil

	case sg.Header.GroupID < r.curGroupID:
		r.Parent.Parent.Log(logger.Warn, "skipping out-of-order subgroup")

	case sg.Header.GroupID == r.curGroupID+1 && len(r.pending) == 0:
		r.curGroupID = sg.Header.GroupID
		return []*subgroup.SubGroup{sg}, nil

	default:
		if prev, ok := r.pending[sg.Header.GroupID]; ok {
			prev.Objects = append(prev.Objects, sg.Objects...)
		} else {
			r.pending[sg.Header.GroupID] = sg
		}
		if r.pendingSince.IsZero() {
			r.pendingSince = r.now()
		}
		withinLimit := r.Parent.addPendingBytes(subGroupPayloadSize(sg))

		diff := sg.Header.GroupID - r.curGroupID

		var countInRange uint64
		for id := range r.pending {
			if id <= sg.Header.GroupID {
				countInRange++
			}
		}

		switch {
		case countInRange == diff:
			return r.flushUpTo(sg.Header.GroupID), nil

		case len(r.pending) > r.maxPendingSubGroups:
			r.Parent.Parent.Log(logger.Warn, "too many reordered subgroups, flushing")
			return r.flushUpTo(sg.Header.GroupID), nil

		case !withinLimit:
			r.Parent.Parent.Log(logger.Warn, "too many reordered bytes, flushing")
			return r.flushUpTo(sg.Header.GroupID), nil

		case r.now().Sub(r.pendingSince) >= r.maxPendingWait:
			r.Parent.Parent.Log(logger.Warn, "groups %d to %d did not arrive in time, skipping them",
				r.curGroupID+1, r.firstPending()-1)
			return r.flushUpTo(sg.Header.GroupID), nil
		}
	}

	return nil, nil
}

func (r *Reorderer) firstPending() uint64 {
	first := uint64(0)
	for id := range r.pending {
		if first == 0 || id < first {
			first = id
		}
	}
	return first
}

func (r *Reorderer) flushUpTo(maxGroupID uint64) []*subgroup.SubGroup {
	ids := make([]uint64, 0, len(r.pending))
	for id := range r.pending {
		if id > r.curGroupID && id <= maxGroupID {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)

	out := make([]*subgroup.SubGroup, 0, len(ids))
	for _, id := range ids {
		out = append(out, r.pending[id])
		r.Parent.removePendingBytes(subGroupPayloadSize(r.pending[id]))
		delete(r.pending, id)
	}

	r.curGroupID = maxGroupID

	for {
		next, ok := r.pending[r.curGroupID+1]
		if !ok {
			break
		}
		out = append(out, next)
		r.Parent.removePendingBytes(subGroupPayloadSize(next))
		delete(r.pending, r.curGroupID+1)
		r.curGroupID++
	}

	// Whatever is still held waits for a gap of its own, from now.
	if len(r.pending) == 0 {
		r.pendingSince = time.Time{}
	} else {
		r.pendingSince = r.now()
	}

	return out
}
