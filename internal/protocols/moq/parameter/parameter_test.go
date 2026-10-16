package parameter_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/mediamtx/internal/protocols/moq/parameter"
)

var cases = []struct {
	name  string
	count int
	enc   []byte
	dec   parameter.Parameters
}{
	{
		name:  "no parameters",
		count: 0,
		enc:   []byte{},
		dec:   nil,
	},
	{
		name:  "authorization token",
		count: 1,
		enc: []byte{
			0x03,                               // type delta = 3 (AuthorizationToken)
			0x08,                               // inner length = 8
			0x03,                               // alias type = UseValue
			0x01,                               // token type = 1
			0x73, 0x65, 0x63, 0x72, 0x65, 0x74, // token value = "secret"
		},
		dec: parameter.Parameters{
			&parameter.AuthorizationToken{
				AliasType:  parameter.AuthorizationTokenAliasTypeUseValue,
				TokenType:  1,
				TokenValue: []byte("secret"),
			},
		},
	},
	{
		name:  "largest object",
		count: 1,
		enc: []byte{
			0x09, // type delta = 9 (LargestObject)
			0x00, // group = 0
			0x00, // object = 0
		},
		dec: parameter.Parameters{
			&parameter.LargestObject{},
		},
	},
	{
		name:  "authorization token and largest object",
		count: 2,
		enc: []byte{
			0x03,                               // type delta = 3 (AuthorizationToken)
			0x08,                               // inner length = 8
			0x03,                               // alias type = UseValue
			0x01,                               // token type = 1
			0x73, 0x65, 0x63, 0x72, 0x65, 0x74, // token value = "secret"
			0x06,       // type delta = 6 (LargestObject)
			0x81, 0x2c, // group = 300
			0x05, // object = 5
		},
		dec: parameter.Parameters{
			&parameter.AuthorizationToken{
				AliasType:  parameter.AuthorizationTokenAliasTypeUseValue,
				TokenType:  1,
				TokenValue: []byte("secret"),
			},
			&parameter.LargestObject{Group: 300, Object: 5},
		},
	},
}

func TestUnmarshal(t *testing.T) {
	for _, ca := range cases {
		t.Run(ca.name, func(t *testing.T) {
			var params parameter.Parameters
			_, err := params.Unmarshal(ca.count, ca.enc)
			require.NoError(t, err)
			require.Equal(t, ca.dec, params)
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
		f.Add(ca.count, ca.enc)
	}

	f.Fuzz(func(_ *testing.T, count int, buf []byte) {
		var params parameter.Parameters
		_, err := params.Unmarshal(count, buf)
		if err != nil {
			return
		}

		buf = make([]byte, params.MarshalSize())
		params.MarshalTo(buf)
	})
}
