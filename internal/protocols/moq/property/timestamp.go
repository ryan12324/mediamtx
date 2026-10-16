package property

import "github.com/bluenviron/mediamtx/internal/protocols/moq/varint"

const (
	// IANA table of draft-ietf-moq-loc-03 (its section 2.3.1.1 says 0x0A).
	timestampPropertyTypeLOC03 = 0x06
	// draft-ietf-moq-loc-04, section 2.3.1.1 and table 1.
	timestampPropertyTypeLOC04 = 0x10
)

// Timestamp is the timestamp property.
// Both type IDs are accepted when reading; it is written with the LOC-03 one.
// spec: draft-ietf-moq-loc, section 2.3.1.1
type Timestamp int64

func (t *Timestamp) isProperty() {}

func (Timestamp) propType() varint.Varint {
	return timestampPropertyTypeLOC03
}

func (t *Timestamp) unmarshal(buf []byte) (int, error) {
	var val varint.Varint
	n, err := val.Unmarshal(buf)
	if err != nil {
		return 0, err
	}
	*t = Timestamp(val)
	return n, nil
}

func (t Timestamp) marshalSize() int {
	return varint.Varint(t).MarshalSize()
}

func (t Timestamp) marshalTo(buf []byte) int {
	return varint.Varint(t).MarshalTo(buf)
}
