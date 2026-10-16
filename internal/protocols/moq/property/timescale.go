package property

import "github.com/bluenviron/mediamtx/internal/protocols/moq/varint"

const timescalePropertyType = 0x08

// Timescale is the timescale property, the number of Timestamp units per second.
// It can be a track property or an object property.
// spec: draft-ietf-moq-loc-04, section 2.3.1.2
type Timescale uint64

func (t *Timescale) isProperty() {}

func (Timescale) propType() varint.Varint {
	return timescalePropertyType
}

func (t *Timescale) unmarshal(buf []byte) (int, error) {
	var val varint.Varint
	n, err := val.Unmarshal(buf)
	if err != nil {
		return 0, err
	}
	*t = Timescale(val)
	return n, nil
}

func (t Timescale) marshalSize() int {
	return varint.Varint(t).MarshalSize()
}

func (t Timescale) marshalTo(buf []byte) int {
	return varint.Varint(t).MarshalTo(buf)
}
