package subgroup_test

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/mediamtx/internal/protocols/moq/property"
	"github.com/bluenviron/mediamtx/internal/protocols/moq/subgroup"
)

var cases = []struct {
	name string
	enc  []byte
	dec  subgroup.SubGroup
}{
	{
		name: "stream without properties",
		enc: []byte{
			0x30,                         // type (Properties=false, FirstObject=false)
			0x01,                         // TrackAlias = 1
			0x00,                         // GroupID = 0
			0x00,                         // object IDDelta = 0
			0x05,                         // payload length = 5
			0x68, 0x65, 0x6c, 0x6c, 0x6f, // payload = "hello"
			0x00, // end-of-stream IDDelta = 0
			0x00, // payload length = 0 (end-of-stream)
			0x03, // status = EndOfGroup
		},
		dec: subgroup.SubGroup{
			Header: subgroup.Header{
				HasProperties: false,
				IsFirstObject: false,
				TrackAlias:    1,
				GroupID:       0,
			},
			Objects: []subgroup.Object{{
				Payload: []byte("hello"),
			}},
		},
	},
	{
		name: "stream with properties",
		enc: []byte{
			0x31,       // type (Properties=true)
			0x01,       // TrackAlias = 1
			0x00,       // GroupID = 0
			0x00,       // object IDDelta = 0
			0x03,       // properties length = 3
			0x06,       // property type delta = 6 (Timestamp)
			0x83, 0xe8, // Timestamp value = 1000
			0x05,                         // payload length = 5
			0x68, 0x65, 0x6c, 0x6c, 0x6f, // payload = "hello"
			0x00, // end-of-stream IDDelta = 0
			0x00, // properties length = 0
			0x00, // payload length = 0 (end-of-stream)
			0x03, // status = EndOfGroup
		},
		dec: subgroup.SubGroup{
			Header: subgroup.Header{
				HasProperties: true,
				IsFirstObject: false,
				TrackAlias:    1,
				GroupID:       0,
			},
			Objects: []subgroup.Object{{
				Properties: property.Properties{
					new(property.Timestamp(1000)),
				},
				Payload: []byte("hello"),
			}},
		},
	},
	{
		name: "stream with priority",
		enc: []byte{
			0x50,                         // type (DefaultPriority=false, FirstObject=true)
			0x01,                         // TrackAlias = 1
			0x00,                         // GroupID = 0
			0x80,                         // Publisher Priority = 128
			0x00,                         // object IDDelta = 0
			0x05,                         // payload length = 5
			0x68, 0x65, 0x6c, 0x6c, 0x6f, // payload = "hello"
			0x00, // end-of-stream IDDelta = 0
			0x00, // payload length = 0 (end-of-stream)
			0x03, // status = EndOfGroup
		},
		dec: subgroup.SubGroup{
			Header: subgroup.Header{
				IsFirstObject: true,
				TrackAlias:    1,
				GroupID:       0,
				HasPriority:   true,
				Priority:      128,
			},
			Objects: []subgroup.Object{{
				Payload: []byte("hello"),
			}},
		},
	},
	{
		name: "stream with subgroup id and priority",
		enc: []byte{
			0x15,                         // type (Properties=true, SubgroupIDMode=present, DefaultPriority=false)
			0x02,                         // TrackAlias = 2
			0x07,                         // GroupID = 7
			0x03,                         // SubgroupID = 3
			0x40,                         // Publisher Priority = 64
			0x00,                         // object IDDelta = 0
			0x00,                         // properties length = 0
			0x05,                         // payload length = 5
			0x68, 0x65, 0x6c, 0x6c, 0x6f, // payload = "hello"
			0x00, // end-of-stream IDDelta = 0
			0x00, // properties length = 0
			0x00, // payload length = 0 (end-of-stream)
			0x03, // status = EndOfGroup
		},
		dec: subgroup.SubGroup{
			Header: subgroup.Header{
				HasProperties:  true,
				SubgroupIDMode: subgroup.IDModePresent,
				TrackAlias:     2,
				GroupID:        7,
				SubgroupID:     3,
				HasPriority:    true,
				Priority:       64,
			},
			Objects: []subgroup.Object{{
				Payload: []byte("hello"),
			}},
		},
	},
}

func TestUnmarshal(t *testing.T) {
	for _, ca := range cases {
		t.Run(ca.name, func(t *testing.T) {
			var s subgroup.SubGroup
			err := s.Read(bytes.NewReader(ca.enc))
			require.NoError(t, err)
			require.Equal(t, ca.dec, s)
		})
	}
}

func TestMarshal(t *testing.T) {
	for _, ca := range cases {
		t.Run(ca.name, func(t *testing.T) {
			require.Equal(t, ca.enc, ca.dec.Marshal())
		})
	}
}

// moq-net (moq-dev/moq) sends this header: type 0x58 has END_OF_GROUP and
// FIRST_OBJECT set and DEFAULT_PRIORITY clear, so a Priority byte follows.
func TestReadHeaderPriority(t *testing.T) {
	var s subgroup.SubGroup
	err := s.Read(bytes.NewReader([]byte{
		0x58,             // type
		0x01,             // TrackAlias = 1
		0x00,             // GroupID = 0
		0x80,             // Publisher Priority = 128
		0x00,             // object IDDelta = 0
		0x02, 0x7b, 0x7d, // payload = "{}"
		0x00, // end-of-stream IDDelta = 0
		0x00, // payload length = 0 (end-of-stream)
		0x03, // status = EndOfGroup
	}))
	require.NoError(t, err)
	require.Equal(t, subgroup.Header{
		IsFirstObject: true,
		TrackAlias:    1,
		HasPriority:   true,
		Priority:      128,
	}, s.Header)
	require.Equal(t, []byte("{}"), s.Objects[0].Payload)
}

func TestReadHeaderInvalidType(t *testing.T) {
	for _, typ := range []byte{
		0x00, // bit 4 not set
		0x90, // bit 7 set
		0x16, // IDMode = 0b11 (reserved)
		0x7f, // IDMode = 0b11 (reserved)
	} {
		var s subgroup.SubGroup
		err := s.Read(bytes.NewReader([]byte{typ, 0x01, 0x00, 0x00, 0x00}))
		require.EqualError(t, err, fmt.Sprintf("invalid subgroup header type: 0x%x", typ))
	}
}

// moq-net (moq-dev/moq) puts a whole group of pictures in one subgroup,
// one object per frame (draft-ietf-moq-msf-01, section 4.1), and ends the
// stream with a FIN instead of an End of Group object.
func TestReadSeveralObjects(t *testing.T) {
	buf := []byte{
		0x31,       // type (Properties=true)
		0x03,       // TrackAlias = 3
		0x00,       // GroupID = 0
		0x00,       // object IDDelta = 0
		0x00,       // properties length = 0
		0x02,       // payload length = 2
		0x01, 0x02, // payload
		0x00,       // object IDDelta = 0 (object 1)
		0x00,       // properties length = 0
		0x00,       // payload length = 0
		0x00,       // status = Normal
		0x00,       // object IDDelta = 0 (object 2)
		0x00,       // properties length = 0
		0x02,       // payload length = 2
		0x03, 0x04, // payload
	}

	sr := subgroup.Reader{R: bytes.NewReader(buf)}
	err := sr.ReadHeader()
	require.NoError(t, err)
	require.Equal(t, uint64(3), sr.Header.TrackAlias)

	obj, err := sr.ReadObject()
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2}, obj.Payload)

	// the empty Normal object is skipped
	obj, err = sr.ReadObject()
	require.NoError(t, err)
	require.Equal(t, []byte{3, 4}, obj.Payload)

	_, err = sr.ReadObject()
	require.Equal(t, io.EOF, err)

	var s subgroup.SubGroup
	err = s.Read(bytes.NewReader(buf))
	require.NoError(t, err)
	require.Len(t, s.Objects, 2)
}

func TestReadEndOfGroup(t *testing.T) {
	sr := subgroup.Reader{R: bytes.NewReader([]byte{
		0x30,       // type
		0x01,       // TrackAlias = 1
		0x00,       // GroupID = 0
		0x00,       // object IDDelta = 0
		0x02,       // payload length = 2
		0x01, 0x02, // payload
		0x00, // object IDDelta = 0
		0x00, // payload length = 0
		0x03, // status = EndOfGroup
		0xff, // not read
	})}
	err := sr.ReadHeader()
	require.NoError(t, err)

	_, err = sr.ReadObject()
	require.NoError(t, err)

	_, err = sr.ReadObject()
	require.Equal(t, io.EOF, err)
}

func TestReadTruncated(t *testing.T) {
	sr := subgroup.Reader{R: bytes.NewReader([]byte{
		0x30,       // type
		0x01,       // TrackAlias = 1
		0x00,       // GroupID = 0
		0x00,       // object IDDelta = 0
		0x05,       // payload length = 5
		0x01, 0x02, // payload, cut short
	})}
	err := sr.ReadHeader()
	require.NoError(t, err)

	_, err = sr.ReadObject()
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	sr = subgroup.Reader{R: bytes.NewReader([]byte{
		0x30, // type
		0x01, // TrackAlias = 1
		0x00, // GroupID = 0
		0x00, // object IDDelta = 0, then nothing
	})}
	err = sr.ReadHeader()
	require.NoError(t, err)

	_, err = sr.ReadObject()
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func FuzzUnmarshal(f *testing.F) {
	for _, ca := range cases {
		f.Add(ca.enc)
	}

	f.Fuzz(func(t *testing.T, buf []byte) {
		var s subgroup.SubGroup
		err := s.Read(bytes.NewReader(buf))
		if err != nil {
			return
		}

		require.NotEmpty(t, s.Objects)

		for _, obj := range s.Objects {
			require.NotEmpty(t, obj.Payload)
		}

		s.Marshal()
	})
}
