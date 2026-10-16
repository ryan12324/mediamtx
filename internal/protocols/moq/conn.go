package moq

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/webtransport-go"
)

// IsStreamReset tells whether err comes from a stream that was reset, over native QUIC or
// WebTransport. A sender that closes a subgroup stream before delivering all its objects
// resets it, for instance when a delivery timeout expires: that ends the subgroup, not
// the session.
// spec: draft-18/19, section 11.4.3
func IsStreamReset(err error) bool {
	if _, ok := errors.AsType[*quic.StreamError](err); ok {
		return true
	}
	_, ok := errors.AsType[*webtransport.StreamError](err)
	return ok
}

// Conn is a connection built on top of either QUIC or WebTransport.
type Conn interface {
	CloseWithError(code uint64, msg string)
	RemoteAddr() net.Addr
	OpenUniStreamSync(ctx context.Context) (io.WriteCloser, error)
	AcceptUniStream(ctx context.Context) (io.Reader, error)
	OpenStreamSync(ctx context.Context) (io.ReadWriteCloser, error)
	AcceptStream(ctx context.Context) (io.ReadWriteCloser, error)
}
