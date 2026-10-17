package reorderer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/protocols/moq/subgroup"
)

type nopLogger struct{}

func (nopLogger) Log(logger.Level, string, ...any) {}

func newTestReorderer(maxPendingSubGroups int, maxPendingBytes int) *Reorderer {
	o := &Orchestrator{Parent: nopLogger{}, maxPendingBytes: maxPendingBytes}
	o.Initialize()

	r := &Reorderer{Parent: o, maxPendingSubGroups: maxPendingSubGroups}
	r.Initialize()

	return r
}

func makeSG(groupID uint64) *subgroup.SubGroup {
	return &subgroup.SubGroup{
		Header: subgroup.Header{GroupID: groupID},
	}
}

func makeSGWithPayload(groupID uint64, size int) *subgroup.SubGroup {
	return &subgroup.SubGroup{
		Header: subgroup.Header{GroupID: groupID},
		Objects: []subgroup.Object{{
			Payload: make([]byte, size),
		}},
	}
}

func TestReordererFirstPush(t *testing.T) {
	r := newTestReorderer(5, 100000)

	sg0 := makeSG(0)
	out, err := r.Push(sg0)
	require.NoError(t, err)
	require.Equal(t, []*subgroup.SubGroup{sg0}, out)
}

func TestReordererInOrder(t *testing.T) {
	r := newTestReorderer(5, 100000)

	sg0 := makeSG(0)
	out, err := r.Push(sg0)
	require.NoError(t, err)
	require.Equal(t, []*subgroup.SubGroup{sg0}, out)

	sg1 := makeSG(1)
	out, err = r.Push(sg1)
	require.NoError(t, err)
	require.Equal(t, []*subgroup.SubGroup{sg1}, out)

	sg2 := makeSG(2)
	out, err = r.Push(sg2)
	require.NoError(t, err)
	require.Equal(t, []*subgroup.SubGroup{sg2}, out)
}

func TestReordererStale(t *testing.T) {
	r := newTestReorderer(5, 100000)

	sg5 := makeSG(5)
	out, err := r.Push(sg5)
	require.NoError(t, err)
	require.Equal(t, []*subgroup.SubGroup{sg5}, out)

	// older GroupID
	out, err = r.Push(makeSG(3))
	require.NoError(t, err)
	require.Nil(t, out)
}

func TestReordererSameGroup(t *testing.T) {
	r := newTestReorderer(5, 100000)

	sg5 := makeSG(5)
	out, err := r.Push(sg5)
	require.NoError(t, err)
	require.Equal(t, []*subgroup.SubGroup{sg5}, out)

	// next object of the current group
	sg5b := makeSG(5)
	out, err = r.Push(sg5b)
	require.NoError(t, err)
	require.Equal(t, []*subgroup.SubGroup{sg5b}, out)
}

func TestReordererPendingGroupObjects(t *testing.T) {
	r := newTestReorderer(5, 100000)

	out, err := r.Push(makeSGWithPayload(0, 1))
	require.NoError(t, err)
	require.Len(t, out, 1)

	// two objects of group 2 arrive before group 1; both are kept
	out, err = r.Push(makeSGWithPayload(2, 2))
	require.NoError(t, err)
	require.Nil(t, out)

	out, err = r.Push(makeSGWithPayload(2, 3))
	require.NoError(t, err)
	require.Nil(t, out)

	out, err = r.Push(makeSGWithPayload(1, 4))
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Equal(t, uint64(1), out[0].Header.GroupID)
	require.Equal(t, uint64(2), out[1].Header.GroupID)
	require.Len(t, out[1].Objects, 2)
	require.Equal(t, 0, r.Parent.pendingBytes)

	// later objects of group 2 are passed on directly
	sg2 := makeSGWithPayload(2, 5)
	out, err = r.Push(sg2)
	require.NoError(t, err)
	require.Equal(t, []*subgroup.SubGroup{sg2}, out)
}

func TestReordererOutOfOrderPending(t *testing.T) {
	r := newTestReorderer(5, 100000)

	sg0 := makeSG(0)
	out, err := r.Push(sg0)
	require.NoError(t, err)
	require.Equal(t, []*subgroup.SubGroup{sg0}, out)

	// sg2 arrives before sg1; held in pending
	out, err = r.Push(makeSG(2))
	require.NoError(t, err)
	require.Nil(t, out)
}

func TestReordererOutOfOrderFilled(t *testing.T) {
	r := newTestReorderer(5, 100000)

	sg0 := makeSG(0)
	out, err := r.Push(sg0)
	require.NoError(t, err)
	require.Equal(t, []*subgroup.SubGroup{sg0}, out)

	// sg2 arrives out of order, held in pending
	sg2 := makeSG(2)
	out, err = r.Push(sg2)
	require.NoError(t, err)
	require.Nil(t, out)

	// sg1 (the missing piece) arrives; gap is now filled; sg1 and the
	// already-pending sg2 are both returned together
	sg1 := makeSG(1)
	out, err = r.Push(sg1)
	require.NoError(t, err)
	require.Equal(t, []*subgroup.SubGroup{sg1, sg2}, out)
}

func TestReordererDrainAfterFill(t *testing.T) { //nolint:dupl
	r := newTestReorderer(5, 100000)

	sg0 := makeSG(0)
	out, err := r.Push(sg0)
	require.NoError(t, err)
	require.Equal(t, []*subgroup.SubGroup{sg0}, out)

	// sg2 and sg3 arrive before sg1
	sg2 := makeSG(2)
	out, err = r.Push(sg2)
	require.NoError(t, err)
	require.Nil(t, out)

	sg3 := makeSG(3)
	out, err = r.Push(sg3)
	require.NoError(t, err)
	require.Nil(t, out)

	// sg1 arrives; fills the gap; sg1, sg2, and sg3 are all returned together
	sg1 := makeSG(1)
	out, err = r.Push(sg1)
	require.NoError(t, err)
	require.Equal(t, []*subgroup.SubGroup{sg1, sg2, sg3}, out)
}

func TestReorderermaxPendingSubGroups(t *testing.T) { //nolint:dupl
	r := newTestReorderer(2, 100000)

	sg0 := makeSG(0)
	out, err := r.Push(sg0)
	require.NoError(t, err)
	require.Equal(t, []*subgroup.SubGroup{sg0}, out)

	// first out-of-order item (pending=1, not over limit)
	sg2 := makeSG(2)
	out, err = r.Push(sg2)
	require.NoError(t, err)
	require.Nil(t, out)

	// second out-of-order item (pending=2, not over limit)
	sg4 := makeSG(4)
	out, err = r.Push(sg4)
	require.NoError(t, err)
	require.Nil(t, out)

	// third out-of-order item pushes pending to 3, which exceeds maxPendingSubGroups=2;
	// all available items are flushed in order, skipping the missing ones (1, 3)
	sg5 := makeSG(5)
	out, err = r.Push(sg5)
	require.NoError(t, err)
	require.Equal(t, []*subgroup.SubGroup{sg2, sg4, sg5}, out)
}

func TestReorderermaxPendingBytes(t *testing.T) {
	r := newTestReorderer(10, 100)

	sg0 := makeSG(0)
	out, err := r.Push(sg0)
	require.NoError(t, err)
	require.Equal(t, []*subgroup.SubGroup{sg0}, out)

	sg2 := makeSGWithPayload(2, 60)
	out, err = r.Push(sg2)
	require.NoError(t, err)
	require.Nil(t, out)

	sg4 := makeSGWithPayload(4, 60)
	out, err = r.Push(sg4)
	require.NoError(t, err)
	require.Equal(t, []*subgroup.SubGroup{sg2, sg4}, out)
}

type testClock struct{ t time.Time }

func (c *testClock) now() time.Time { return c.t }

func newClockedReorderer() (*Reorderer, *testClock) {
	c := &testClock{t: time.Unix(1000, 0)}
	r := newTestReorderer(50, 100000)
	r.now = c.now
	return r, c
}

// A group that never comes (lost on every path) holds the later ones back only for
// maxPendingWait; then they go on, in order.
func TestReordererMissingGroupIsSkippedAfterTheWait(t *testing.T) {
	r, c := newClockedReorderer()

	_, err := r.Push(makeSG(9))
	require.NoError(t, err)

	sg11 := makeSGWithPayload(11, 10)
	out, err := r.Push(sg11)
	require.NoError(t, err)
	require.Nil(t, out)

	c.t = c.t.Add(100 * time.Millisecond)
	sg12 := makeSGWithPayload(12, 10)
	out, err = r.Push(sg12)
	require.NoError(t, err)
	require.Nil(t, out)

	c.t = c.t.Add(maxPendingWait)
	sg13 := makeSGWithPayload(13, 10)
	out, err = r.Push(sg13)
	require.NoError(t, err)
	require.Equal(t, []*subgroup.SubGroup{sg11, sg12, sg13}, out)

	// Back in order: the next group passes at once.
	sg14 := makeSG(14)
	out, err = r.Push(sg14)
	require.NoError(t, err)
	require.Equal(t, []*subgroup.SubGroup{sg14}, out)
}

// A group that is only late, within maxPendingWait, is waited for.
func TestReordererLateGroupWithinTheWait(t *testing.T) {
	r, c := newClockedReorderer()

	_, err := r.Push(makeSG(0))
	require.NoError(t, err)

	sg2 := makeSG(2)
	out, err := r.Push(sg2)
	require.NoError(t, err)
	require.Nil(t, out)

	c.t = c.t.Add(maxPendingWait / 2)
	sg1 := makeSG(1)
	out, err = r.Push(sg1)
	require.NoError(t, err)
	require.Equal(t, []*subgroup.SubGroup{sg1, sg2}, out)
}
