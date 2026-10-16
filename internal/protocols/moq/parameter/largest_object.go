package parameter

import (
	"github.com/bluenviron/mediamtx/internal/protocols/moq/varint"
)

const typeLargestObject = 0x09

// LargestObject is the LARGEST_OBJECT parameter.
// Its value is a Location: two consecutive varints, Group and Object.
// spec:
// * draft-18/19, section 10.2.16
type LargestObject struct {
	Group  uint64
	Object uint64
}

func (*LargestObject) isParameter() {}

func (*LargestObject) paramType() uint64 {
	return typeLargestObject
}

func (l *LargestObject) unmarshal(buf []byte) (int, error) {
	var group varint.Varint
	n1, err := group.Unmarshal(buf)
	if err != nil {
		return 0, err
	}

	var object varint.Varint
	n2, err := object.Unmarshal(buf[n1:])
	if err != nil {
		return 0, err
	}

	l.Group = uint64(group)
	l.Object = uint64(object)

	return n1 + n2, nil
}

func (l LargestObject) marshalSize() int {
	return varint.Varint(l.Group).MarshalSize() +
		varint.Varint(l.Object).MarshalSize()
}

func (l LargestObject) marshalTo(buf []byte) int {
	n := varint.Varint(l.Group).MarshalTo(buf)
	n += varint.Varint(l.Object).MarshalTo(buf[n:])
	return n
}
