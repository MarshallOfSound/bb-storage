package zstd_test

import (
	"bytes"
	"testing"

	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/stretchr/testify/require"
)

func TestGetFrameContentSize(t *testing.T) {
	t.Run("Announced", func(t *testing.T) {
		data := bytes.Repeat([]byte("BuildBarn "), 10000)
		size, ok := bb_zstd.GetFrameContentSize(bb_zstd.Encode(data))
		require.True(t, ok)
		require.Equal(t, uint64(len(data)), size)
	})

	t.Run("NotAnnounced", func(t *testing.T) {
		// Small objects are stored in a frame that does not
		// announce its size.
		_, ok := bb_zstd.GetFrameContentSize(bb_zstd.Encode([]byte("Hello")))
		require.False(t, ok)
	})

	t.Run("SkippableFrame", func(t *testing.T) {
		_, ok := bb_zstd.GetFrameContentSize([]byte{0x50, 0x2a, 0x4d, 0x18, 0x02, 0x00, 0x00, 0x00, 'H', 'i'})
		require.False(t, ok)
	})

	t.Run("Malformed", func(t *testing.T) {
		_, ok := bb_zstd.GetFrameContentSize(nil)
		require.False(t, ok)
		_, ok = bb_zstd.GetFrameContentSize([]byte("This is not a ZSTD frame"))
		require.False(t, ok)
		_, ok = bb_zstd.GetFrameContentSize([]byte{0x28, 0xb5, 0x2f, 0xfd, 0xe0})
		require.False(t, ok)
	})
}
