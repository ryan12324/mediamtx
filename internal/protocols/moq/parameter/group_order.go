package parameter

import (
	"fmt"
)

const typeGroupOrder = 0x22

// GroupOrder is the value of the GROUP_ORDER parameter.
// spec:
// * draft-18/19, section 10.2.8
type GroupOrder uint8

// spec:
// * draft-18/19, section 10.2.8
const (
	GroupOrderAscending  GroupOrder = 0x01
	GroupOrderDescending GroupOrder = 0x02
)

func (*GroupOrder) isParameter() {}

func (*GroupOrder) paramType() uint64 {
	return typeGroupOrder
}

func (o *GroupOrder) unmarshal(buf []byte) (int, error) {
	if len(buf) < 1 {
		return 0, fmt.Errorf("not enough bytes for parameter value")
	}

	v := GroupOrder(buf[0])
	if v != GroupOrderAscending && v != GroupOrderDescending {
		return 0, fmt.Errorf("invalid group order: %d", buf[0])
	}

	*o = v
	return 1, nil
}

func (GroupOrder) marshalSize() int {
	return 1
}

func (o GroupOrder) marshalTo(buf []byte) int {
	buf[0] = byte(o)
	return 1
}
