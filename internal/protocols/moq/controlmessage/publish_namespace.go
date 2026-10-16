package controlmessage

import (
	"github.com/bluenviron/mediamtx/internal/protocols/moq/namespace"
	"github.com/bluenviron/mediamtx/internal/protocols/moq/parameter"
	"github.com/bluenviron/mediamtx/internal/protocols/moq/varint"
)

const typePublishNamespace varint.Varint = 0x06

// PublishNamespace is the PUBLISH_NAMESPACE control message.
// spec:
// * draft-18/19, section 10.15
type PublishNamespace struct {
	RequestID  uint64
	Namespace  namespace.Namespace
	Parameters parameter.Parameters
}

func (*PublishNamespace) isMessage() {}

func (m *PublishNamespace) unmarshal(buf []byte) error {
	var requestID varint.Varint
	n, err := requestID.Unmarshal(buf)
	if err != nil {
		return err
	}
	buf = buf[n:]

	m.RequestID = uint64(requestID)

	n, err = m.Namespace.Unmarshal(buf)
	if err != nil {
		return err
	}
	buf = buf[n:]

	var paramCount varint.Varint
	n, err = paramCount.Unmarshal(buf)
	if err != nil {
		return err
	}
	buf = buf[n:]

	_, err = m.Parameters.Unmarshal(int(paramCount), buf)
	return err
}

func (m PublishNamespace) payloadSize() int {
	return varint.Varint(m.RequestID).MarshalSize() +
		m.Namespace.MarshalSize() +
		varint.Varint(len(m.Parameters)).MarshalSize() +
		m.Parameters.MarshalSize()
}

func (m PublishNamespace) marshalSize() int {
	return typePublishNamespace.MarshalSize() + 2 + m.payloadSize()
}

func (m PublishNamespace) marshalTo(buf []byte) int {
	payloadSize := m.payloadSize()

	n := typePublishNamespace.MarshalTo(buf)
	buf[n] = byte(payloadSize >> 8)
	buf[n+1] = byte(payloadSize)
	n += 2
	n += varint.Varint(m.RequestID).MarshalTo(buf[n:])
	n += m.Namespace.MarshalTo(buf[n:])
	n += varint.Varint(len(m.Parameters)).MarshalTo(buf[n:])
	n += m.Parameters.MarshalTo(buf[n:])

	return n
}

// Marshal implements Message.
func (m PublishNamespace) Marshal() []byte {
	buf := make([]byte, m.marshalSize())
	m.marshalTo(buf)
	return buf
}
