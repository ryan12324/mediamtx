package moq_test

import (
	"testing"

	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"
	"github.com/stretchr/testify/require"

	"github.com/bluenviron/mediamtx/internal/protocols/moq"
	"github.com/bluenviron/mediamtx/internal/protocols/moq/catalog"
	"github.com/bluenviron/mediamtx/internal/protocols/moq/property"
	"github.com/bluenviron/mediamtx/internal/protocols/moq/subgroup"
	"github.com/bluenviron/mediamtx/internal/stream"
	"github.com/bluenviron/mediamtx/internal/test"
	"github.com/bluenviron/mediamtx/internal/unit"
)

func TestToStream(t *testing.T) {
	cat := &catalog.Catalog{
		Tracks: []catalog.Track{
			{Codec: "av01.0.04M.08"},
			{Codec: "vp09.00.10.08"},
			{Codec: "vp8"},
			{Codec: "hev1.1.6.L93.B0"},
			{Codec: "avc3.640028"},
			{Codec: "opus"},
			{Codec: "mp4a.40.2", Samplerate: 44100, Channels: 2},
		},
	}

	medias, _, err := moq.ToStream(cat, nil, nil)
	require.NoError(t, err)

	require.Equal(t, []*description.Media{
		{
			Type:    description.MediaTypeVideo,
			Formats: []format.Format{&format.AV1{PayloadTyp: 96}},
		},
		{
			Type:    description.MediaTypeVideo,
			Formats: []format.Format{&format.VP9{PayloadTyp: 96}},
		},
		{
			Type:    description.MediaTypeVideo,
			Formats: []format.Format{&format.VP8{PayloadTyp: 96}},
		},
		{
			Type:    description.MediaTypeVideo,
			Formats: []format.Format{&format.H265{PayloadTyp: 96}},
		},
		{
			Type:    description.MediaTypeVideo,
			Formats: []format.Format{&format.H264{PayloadTyp: 96, PacketizationMode: 1}},
		},
		{
			Type:    description.MediaTypeAudio,
			Formats: []format.Format{&format.Opus{PayloadTyp: 96, ChannelCount: 2}},
		},
		{
			Type: description.MediaTypeAudio,
			Formats: []format.Format{&format.MPEG4Audio{
				PayloadTyp: 96,
				Config: &mpeg4audio.AudioSpecificConfig{
					Type:          mpeg4audio.ObjectTypeAACLC,
					SampleRate:    44100,
					ChannelConfig: 2,
				},
				SizeLength:       13,
				IndexLength:      3,
				IndexDeltaLength: 3,
			}},
		},
	}, medias)
}

func TestToStreamTimescale(t *testing.T) {
	for _, ca := range []struct {
		name           string
		trackTimescale uint64
		props          property.Properties
		pts            int64
	}{
		{
			"no timescale",
			0,
			property.Properties{new(property.Timestamp(180000))},
			180000,
		},
		{
			"track timescale",
			1000,
			property.Properties{new(property.Timestamp(2000))},
			180000,
		},
		{
			"object timescale",
			1000,
			property.Properties{new(property.Timescale(1000000)), new(property.Timestamp(2000000))},
			180000,
		},
	} {
		t.Run(ca.name, func(t *testing.T) {
			cat := &catalog.Catalog{Tracks: []catalog.Track{{Codec: "hev1.1.6.L93.B0"}}}

			var subStream *stream.SubStream
			medias, writeFuncs, err := moq.ToStream(cat, &subStream, func(trackAlias uint64) uint64 {
				require.Equal(t, uint64(1), trackAlias)
				return ca.trackTimescale
			})
			require.NoError(t, err)

			strm := &stream.Stream{
				OrigDesc:          &description.Session{Medias: medias},
				WriteQueueSize:    512,
				RTPMaxPayloadSize: 1450,
				Parent:            test.NilLogger,
			}
			err = strm.Initialize()
			require.NoError(t, err)
			defer strm.Close()

			subStream = &stream.SubStream{Stream: strm, UseRTPPackets: false}
			err = subStream.Initialize()
			require.NoError(t, err)

			r := &stream.Reader{Parent: test.NilLogger}
			recv := make(chan int64, 1)
			r.OnData(medias[0], medias[0].Formats[0], func(u *unit.Unit) error {
				recv <- u.PTS
				return nil
			})
			strm.AddReader(r)
			defer strm.RemoveReader(r)

			payload, err := h264.AVCC{{0x26, 0x01, 0xaf}}.Marshal()
			require.NoError(t, err)

			err = writeFuncs[1](&subgroup.SubGroup{
				Objects: []subgroup.Object{{Properties: ca.props, Payload: payload}},
			})
			require.NoError(t, err)

			require.Equal(t, ca.pts, <-recv)
		})
	}
}
