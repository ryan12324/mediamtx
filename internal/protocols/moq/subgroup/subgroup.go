package subgroup

import (
	"errors"
	"fmt"
	"io"
)

// Reader reads a subgroup stream one object at a time, so that objects can
// be used as soon as they arrive. A subgroup can hold many objects, for
// instance a whole group of pictures (draft-ietf-moq-msf-01, section 4.1).
type Reader struct {
	R io.Reader

	Header Header

	ended bool
}

// ReadHeader reads the subgroup header.
func (r *Reader) ReadHeader() error {
	return r.Header.read(r.R)
}

// ReadObject reads the next object that has a payload.
// It returns io.EOF when the subgroup ends, that is when the stream is
// closed (draft-18/19, section 11.4.3) or when an object has the
// End of Group or End of Track status (draft-18/19, section 11.2.1.1).
func (r *Reader) ReadObject() (*Object, error) {
	for !r.ended {
		var o Object
		err := o.read(r.R, &r.Header)
		if err != nil {
			return nil, err
		}

		if len(o.Payload) != 0 {
			return &o, nil
		}

		// a Normal object can be empty: there is nothing to pass on.
		if o.status != objectStatusNormal {
			r.ended = true
		}
	}

	return nil, io.EOF
}

// SubGroup is a subgroup stream.
type SubGroup struct {
	Header  Header
	Objects []Object
}

// Read reads a SubGroup and all its objects.
func (s *SubGroup) Read(r io.Reader) error {
	sr := Reader{R: r}

	err := sr.ReadHeader()
	if err != nil {
		return err
	}
	s.Header = sr.Header

	s.Objects = nil

	for {
		var o *Object
		o, err = sr.ReadObject()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		s.Objects = append(s.Objects, *o)
	}

	if len(s.Objects) == 0 {
		return fmt.Errorf("unexpected empty subgroup")
	}

	return nil
}

func (s SubGroup) marshalSize() int {
	n := s.Header.marshalSize()

	for _, o := range s.Objects {
		n += o.marshalSize(&s.Header)
	}

	n += Object{Payload: nil}.marshalSize(&s.Header)

	return n
}

func (s SubGroup) marshalTo(buf []byte) int {
	n := s.Header.marshalTo(buf)

	for _, o := range s.Objects {
		n += o.marshalTo(buf[n:], &s.Header)
	}

	n += Object{Payload: nil}.marshalTo(buf[n:], &s.Header)

	return n
}

// Marshal encodes a SubGroup.
func (s *SubGroup) Marshal() []byte {
	buf := make([]byte, s.marshalSize())
	s.marshalTo(buf)
	return buf
}
