// Package catalog contains a media catalog.
package catalog

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// Version is the MSF version of a catalog.
// It is a number in draft-ietf-moq-msf-00 and a string ("draft-01") in
// draft-ietf-moq-msf-01. Numeric versions are written back as numbers.
// spec:
// * draft-ietf-moq-msf-00, section 5.1.1
// * draft-ietf-moq-msf-01, section 5.1.1
type Version string

// MarshalJSON implements json.Marshaler.
func (v Version) MarshalJSON() ([]byte, error) {
	if n, err := strconv.ParseUint(string(v), 10, 64); err == nil {
		return json.Marshal(n)
	}
	return json.Marshal(string(v))
}

// UnmarshalJSON implements json.Unmarshaler.
func (v *Version) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		*v = Version(s)
		return nil
	}

	var n uint64
	if err := json.Unmarshal(b, &n); err != nil {
		return fmt.Errorf("invalid catalog version: %s", b)
	}
	*v = Version(strconv.FormatUint(n, 10))
	return nil
}

// Catalog is a media catalog.
// spec:
// * draft-ietf-moq-msf-00
// * draft-ietf-moq-msf-01
type Catalog struct {
	Version Version `json:"version"`
	Tracks  []Track `json:"tracks"`
}

// Track is a track in a media catalog.
type Track struct {
	Name       string  `json:"name"`
	Packaging  string  `json:"packaging"`
	IsLive     bool    `json:"isLive"`
	Namespace  string  `json:"namespace,omitempty"`
	Codec      string  `json:"codec,omitempty"`
	Bitrate    int     `json:"bitrate,omitempty"`
	Width      int     `json:"width,omitempty"`
	Height     int     `json:"height,omitempty"`
	Framerate  float64 `json:"framerate,omitempty"`
	Samplerate int     `json:"samplerate,omitempty"`
	Channels   int     `json:"channels,omitempty"`
	ClockRate  int     `json:"clockrate,omitempty"`
	InitData   string  `json:"initData,omitempty"`
}
