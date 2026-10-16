package catalog_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/mediamtx/internal/protocols/moq/catalog"
)

func TestVersionUnmarshal(t *testing.T) {
	for _, ca := range []struct {
		name string
		enc  string
		dec  catalog.Version
	}{
		{"msf-00 number", `{"version":1,"tracks":[]}`, "1"},
		{"msf-01 string", `{"version":"draft-01","tracks":[]}`, "draft-01"},
		{"msf-01 example", `{"version":"1","tracks":[]}`, "1"},
	} {
		t.Run(ca.name, func(t *testing.T) {
			var c catalog.Catalog
			err := json.Unmarshal([]byte(ca.enc), &c)
			require.NoError(t, err)
			require.Equal(t, ca.dec, c.Version)
		})
	}
}

func TestVersionUnmarshalInvalid(t *testing.T) {
	var c catalog.Catalog
	err := json.Unmarshal([]byte(`{"version":true,"tracks":[]}`), &c)
	require.Error(t, err)
}

func TestVersionMarshal(t *testing.T) {
	buf, err := json.Marshal(catalog.Catalog{Version: "1", Tracks: []catalog.Track{}})
	require.NoError(t, err)
	require.Equal(t, `{"version":1,"tracks":[]}`, string(buf))

	buf, err = json.Marshal(catalog.Catalog{Version: "draft-01", Tracks: []catalog.Track{}})
	require.NoError(t, err)
	require.Equal(t, `{"version":"draft-01","tracks":[]}`, string(buf))
}
