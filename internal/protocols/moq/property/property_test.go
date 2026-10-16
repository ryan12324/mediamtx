package property_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/mediamtx/internal/protocols/moq/property"
)

var cases = []struct {
	name string
	enc  []byte
	dec  property.Properties
}{
	{
		name: "no properties",
		enc:  []byte{},
		dec:  nil,
	},
	{
		name: "timestamp",
		enc: []byte{
			0x06,       // type delta = 6 (Timestamp)
			0x83, 0xe8, // value = 1000
		},
		dec: property.Properties{
			new(property.Timestamp(1000)),
		},
	},
	{
		name: "timescale",
		enc: []byte{
			0x08,       // type delta = 8 (Timescale)
			0x83, 0xe8, // value = 1000
		},
		dec: property.Properties{
			new(property.Timescale(1000)),
		},
	},
}

func TestUnmarshalLOC04Timestamp(t *testing.T) {
	// As sent by moq-net: Timescale track property, then a LOC-04 Timestamp object property.
	var props property.Properties
	err := props.Unmarshal([]byte{
		0x10,       // type delta = 0x10 (Timestamp, draft-ietf-moq-loc-04)
		0x83, 0xe8, // value = 1000
	})
	require.NoError(t, err)
	require.Equal(t, property.Properties{new(property.Timestamp(1000))}, props)

	props = nil
	err = props.Unmarshal([]byte{
		0x08,       // type delta = 8 (Timescale)
		0x83, 0xe8, // value = 1000
		0x08, // type delta = 8 -> 0x10 (Timestamp)
		0x05, // value = 5
	})
	require.NoError(t, err)
	require.Equal(t, property.Properties{new(property.Timescale(1000)), new(property.Timestamp(5))}, props)
}

func TestUnmarshal(t *testing.T) {
	for _, ca := range cases {
		t.Run(ca.name, func(t *testing.T) {
			var props property.Properties
			err := props.Unmarshal(ca.enc)
			require.NoError(t, err)
			require.Equal(t, ca.dec, props)
		})
	}
}

func TestMarshal(t *testing.T) {
	for _, ca := range cases {
		t.Run(ca.name, func(t *testing.T) {
			buf := make([]byte, ca.dec.MarshalSize())
			ca.dec.MarshalTo(buf)
			require.Equal(t, ca.enc, buf)
		})
	}
}

func FuzzUnmarshal(f *testing.F) {
	for _, ca := range cases {
		f.Add(ca.enc)
	}

	f.Fuzz(func(_ *testing.T, buf []byte) {
		var props property.Properties
		err := props.Unmarshal(buf)
		if err != nil {
			return
		}

		buf = make([]byte, props.MarshalSize())
		props.MarshalTo(buf)
	})
}
