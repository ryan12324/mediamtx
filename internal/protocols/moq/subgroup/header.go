// Package subgroup implements the subgroup stream.
package subgroup

import (
	"fmt"
	"io"

	"github.com/bluenviron/mediamtx/internal/protocols/moq/varint"
)

// IDMode is the SUBGROUP_ID_MODE field of the header type.
// spec:
// * draft-18/19, section 11.4.2
type IDMode uint8

// spec:
// * draft-18/19, section 11.4.2
const (
	// IDModeZero means the Subgroup ID field is absent and the Subgroup ID is 0.
	IDModeZero IDMode = 0b00

	// IDModeFirstObject means the Subgroup ID field is absent and the Subgroup ID
	// is the Object ID of the first object in the stream.
	IDModeFirstObject IDMode = 0b01

	// IDModePresent means the Subgroup ID field is present.
	IDModePresent IDMode = 0b10
)

// Header is the SUBGROUP_HEADER structure.
// spec:
// * draft-17, section 10.4.2
// * draft-18/19, section 11.4.2
type Header struct {
	HasProperties  bool
	SubgroupIDMode IDMode
	IsFirstObject  bool
	TrackAlias     uint64
	GroupID        uint64

	// present when SubgroupIDMode is IDModePresent.
	SubgroupID uint64

	// when false, the DEFAULT_PRIORITY bit is set and the subgroup
	// inherits the priority of the subscription.
	HasPriority bool
	Priority    uint8
}

func (h *Header) read(r io.Reader) error {
	var b [1]byte
	_, err := io.ReadFull(r, b[:])
	if err != nil {
		return err
	}

	// type values have the form 0b0XX1XXXX.
	if (b[0] & 0x90) != 0x10 {
		return fmt.Errorf("invalid subgroup header type: 0x%x", b[0])
	}

	h.HasProperties = (b[0] & 0x01) != 0
	h.SubgroupIDMode = IDMode((b[0] & 0x06) >> 1)
	h.HasPriority = (b[0] & 0x20) == 0
	h.IsFirstObject = (b[0] & 0x40) != 0

	if h.SubgroupIDMode == 0b11 {
		return fmt.Errorf("invalid subgroup header type: 0x%x", b[0])
	}

	var trackAlias varint.Varint
	err = trackAlias.Read(r)
	if err != nil {
		return err
	}
	h.TrackAlias = uint64(trackAlias)

	var groupID varint.Varint
	err = groupID.Read(r)
	if err != nil {
		return err
	}
	h.GroupID = uint64(groupID)

	if h.SubgroupIDMode == IDModePresent {
		var subgroupID varint.Varint
		err = subgroupID.Read(r)
		if err != nil {
			return err
		}
		h.SubgroupID = uint64(subgroupID)
	}

	if h.HasPriority {
		_, err = io.ReadFull(r, b[:])
		if err != nil {
			return err
		}
		h.Priority = b[0]
	}

	return nil
}

func (h Header) streamType() varint.Varint {
	st := varint.Varint(0x10)
	if h.HasProperties {
		st |= 0x01
	}
	st |= varint.Varint(h.SubgroupIDMode) << 1
	if !h.HasPriority {
		st |= 0x20
	}
	if h.IsFirstObject {
		st |= 0x40
	}
	return st
}

func (h Header) marshalSize() int {
	n := h.streamType().MarshalSize() +
		varint.Varint(h.TrackAlias).MarshalSize() +
		varint.Varint(h.GroupID).MarshalSize()
	if h.SubgroupIDMode == IDModePresent {
		n += varint.Varint(h.SubgroupID).MarshalSize()
	}
	if h.HasPriority {
		n++
	}
	return n
}

func (h Header) marshalTo(buf []byte) int {
	pos := h.streamType().MarshalTo(buf)
	pos += varint.Varint(h.TrackAlias).MarshalTo(buf[pos:])
	pos += varint.Varint(h.GroupID).MarshalTo(buf[pos:])
	if h.SubgroupIDMode == IDModePresent {
		pos += varint.Varint(h.SubgroupID).MarshalTo(buf[pos:])
	}
	if h.HasPriority {
		buf[pos] = h.Priority
		pos++
	}
	return pos
}
