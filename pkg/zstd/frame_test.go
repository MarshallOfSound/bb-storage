package zstd_test

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"

	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// rawFrame creates a ZSTD frame consisting of a single uncompressed
// block, using a caller provided frame header.
func rawFrame(header, data []byte) []byte {
	frame := append([]byte{0x28, 0xb5, 0x2f, 0xfd}, header...)
	blockHeader := uint32(len(data))<<3 | 1
	frame = append(frame, byte(blockHeader), byte(blockHeader>>8), byte(blockHeader>>16))
	return append(frame, data...)
}

func skippableFrame(payload []byte) []byte {
	frame := []byte{0x50, 0x2a, 0x4d, 0x18}
	frame = binary.LittleEndian.AppendUint32(frame, uint32(len(payload)))
	return append(frame, payload...)
}

func TestIsReusableFrame(t *testing.T) {
	compressible := bytes.Repeat([]byte("BuildBarn "), 100000)
	incompressible := pseudoRandomData(t, 1<<20)
	hello := []byte("Hello")

	t.Run("Accepted", func(t *testing.T) {
		// Frames created by our own encoder, regardless of
		// whether the data is compressible.
		for name, data := range map[string][]byte{
			"Compressible":   compressible,
			"Incompressible": incompressible,
			"Small":          hello,
			"SingleByte":     {0x42},
		} {
			t.Run("Encode/"+name, func(t *testing.T) {
				require.True(t, bb_zstd.IsReusableFrame(bb_zstd.Encode(data), len(data)))
			})
		}

		// Frames created by clients using different settings.
		for name, options := range map[string][]zstd.EOption{
			"Default":           nil,
			"Fastest":           {zstd.WithEncoderLevel(zstd.SpeedFastest)},
			"BetterCompression": {zstd.WithEncoderLevel(zstd.SpeedBetterCompression)},
			"BestCompression":   {zstd.WithEncoderLevel(zstd.SpeedBestCompression)},
			"NoChecksum":        {zstd.WithEncoderCRC(false)},
			"NoSingleSegment":   {zstd.WithSingleSegment(false), zstd.WithWindowSize(1 << 20)},
		} {
			for dataName, data := range map[string][]byte{
				"Compressible":   compressible,
				"Incompressible": incompressible,
				"Small":          hello,
			} {
				t.Run(name+"/"+dataName, func(t *testing.T) {
					encoder, err := zstd.NewWriter(nil, options...)
					require.NoError(t, err)
					frame := encoder.EncodeAll(data, nil)
					require.True(t, bb_zstd.IsReusableFrame(frame, len(data)))
				})
			}
		}

		t.Run("RLEBlock", func(t *testing.T) {
			// Window descriptor, followed by a last RLE block
			// of 1000 bytes.
			frame := []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x00}
			blockHeader := uint32(1000)<<3 | 1<<1 | 1
			frame = append(frame, byte(blockHeader), byte(blockHeader>>8), byte(blockHeader>>16), 'A')
			decoded, err := bb_zstd.Decode(frame, 1000)
			require.NoError(t, err)
			require.Equal(t, bytes.Repeat([]byte("A"), 1000), decoded)
			require.True(t, bb_zstd.IsReusableFrame(frame, 1000))
		})

		t.Run("FrameContentSize", func(t *testing.T) {
			// Single segment with a one byte frame content size.
			require.True(t, bb_zstd.IsReusableFrame(rawFrame([]byte{0x20, 5}, hello), 5))
			// Two byte frame content size, which is offset by 256.
			data := pseudoRandomData(t, 1000)
			require.True(t, bb_zstd.IsReusableFrame(rawFrame([]byte{0x40, 0x00, (1000 - 256) & 0xff, (1000 - 256) >> 8}, data), 1000))
			// Four and eight byte frame content sizes.
			require.True(t, bb_zstd.IsReusableFrame(rawFrame([]byte{0x80, 0x00, 5, 0, 0, 0}, hello), 5))
			require.True(t, bb_zstd.IsReusableFrame(rawFrame([]byte{0xc0, 0x00, 5, 0, 0, 0, 0, 0, 0, 0}, hello), 5))
		})
	})

	valid := bb_zstd.Encode(compressible)
	require.True(t, bb_zstd.IsReusableFrame(valid, len(compressible)))

	t.Run("Empty", func(t *testing.T) {
		require.False(t, bb_zstd.IsReusableFrame(nil, 0))
		require.False(t, bb_zstd.IsReusableFrame([]byte{0x28, 0xb5, 0x2f, 0xfd}, 0))
	})

	t.Run("NegativeSize", func(t *testing.T) {
		require.False(t, bb_zstd.IsReusableFrame(valid, -1))
	})

	t.Run("Truncated", func(t *testing.T) {
		for length := range len(valid) {
			require.False(t, bb_zstd.IsReusableFrame(valid[:length], len(compressible)), "Length %d", length)
		}
	})

	t.Run("SkippableFrame", func(t *testing.T) {
		skippable := skippableFrame([]byte("Smuggled"))
		// On its own.
		require.False(t, bb_zstd.IsReusableFrame(skippable, 0))
		require.False(t, bb_zstd.IsReusableFrame(skippable, len(skippable)))
		// Before and after a regular frame. These decode to the
		// right data, as decoders ignore skippable frames.
		for _, frame := range [][]byte{
			append(append([]byte(nil), skippable...), valid...),
			append(append([]byte(nil), valid...), skippable...),
		} {
			decoded, err := bb_zstd.Decode(frame, len(compressible))
			require.NoError(t, err)
			require.Equal(t, compressible, decoded)
			require.False(t, bb_zstd.IsReusableFrame(frame, len(compressible)))
		}
	})

	t.Run("MultipleFrames", func(t *testing.T) {
		// Two frames that together decode to the right data.
		half := len(compressible) / 2
		frame := append(bb_zstd.Encode(compressible[:half]), bb_zstd.Encode(compressible[half:])...)
		decoded, err := bb_zstd.Decode(frame, len(compressible))
		require.NoError(t, err)
		require.Equal(t, compressible, decoded)
		require.False(t, bb_zstd.IsReusableFrame(frame, len(compressible)))

		// A valid frame followed by an empty frame.
		frame = append(append([]byte(nil), valid...), 0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x58, 0x01, 0x00, 0x00)
		decoded, err = bb_zstd.Decode(frame, len(compressible))
		require.NoError(t, err)
		require.Equal(t, compressible, decoded)
		require.False(t, bb_zstd.IsReusableFrame(frame, len(compressible)))
	})

	t.Run("TrailingData", func(t *testing.T) {
		require.False(t, bb_zstd.IsReusableFrame(append(append([]byte(nil), valid...), 0x00), len(compressible)))
		require.False(t, bb_zstd.IsReusableFrame(append(append([]byte(nil), valid...), "Smuggled"...), len(compressible)))
	})

	t.Run("Dictionary", func(t *testing.T) {
		// Frame header descriptors with one, two and four byte
		// dictionary IDs, even if the ID is zero.
		require.False(t, bb_zstd.IsReusableFrame(rawFrame([]byte{0x01, 0x00, 0x2a}, hello), 5))
		require.False(t, bb_zstd.IsReusableFrame(rawFrame([]byte{0x02, 0x00, 0x2a, 0x00}, hello), 5))
		require.False(t, bb_zstd.IsReusableFrame(rawFrame([]byte{0x03, 0x00, 0x2a, 0x00, 0x00, 0x00}, hello), 5))
		require.False(t, bb_zstd.IsReusableFrame(rawFrame([]byte{0x01, 0x00, 0x00}, hello), 5))
	})

	t.Run("ReservedHeaderBits", func(t *testing.T) {
		require.False(t, bb_zstd.IsReusableFrame(rawFrame([]byte{0x08, 0x00}, hello), 5))
		require.False(t, bb_zstd.IsReusableFrame(rawFrame([]byte{0x10, 0x00}, hello), 5))
	})

	t.Run("LargerThanUncompressed", func(t *testing.T) {
		// A frame that stores the data in many tiny uncompressed
		// blocks decodes properly, but is far larger than the
		// data itself.
		frame := []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x00}
		data := pseudoRandomData(t, 1000)
		for i, b := range data {
			blockHeader := uint32(1) << 3
			if i == len(data)-1 {
				blockHeader |= 1
			}
			frame = append(frame, byte(blockHeader), byte(blockHeader>>8), byte(blockHeader>>16), b)
		}
		decoded, err := bb_zstd.Decode(frame, len(data))
		require.NoError(t, err)
		require.Equal(t, data, decoded)
		require.False(t, bb_zstd.IsReusableFrame(frame, len(data)))

		// The size that is provided needs to be that of the
		// decoded data.
		require.False(t, bb_zstd.IsReusableFrame(bb_zstd.Encode(incompressible), len(incompressible)/2))
	})

	t.Run("FrameContentSizeMismatch", func(t *testing.T) {
		require.False(t, bb_zstd.IsReusableFrame(rawFrame([]byte{0x20, 6}, hello), 5))
		require.False(t, bb_zstd.IsReusableFrame(rawFrame([]byte{0x80, 0x00, 4, 0, 0, 0}, hello), 5))
		require.False(t, bb_zstd.IsReusableFrame(rawFrame([]byte{0xc0, 0x00, 5, 0, 0, 0, 1, 0, 0, 0}, hello), 5))
		require.False(t, bb_zstd.IsReusableFrame(valid, len(compressible)+1))
	})

	t.Run("WindowTooLarge", func(t *testing.T) {
		// 8 MiB is permitted, anything above is not.
		require.True(t, bb_zstd.IsReusableFrame(rawFrame([]byte{0x00, 13 << 3}, hello), 5))
		require.False(t, bb_zstd.IsReusableFrame(rawFrame([]byte{0x00, 13<<3 | 1}, hello), 5))
		require.False(t, bb_zstd.IsReusableFrame(rawFrame([]byte{0x00, 14 << 3}, hello), 5))
		require.False(t, bb_zstd.IsReusableFrame(rawFrame([]byte{0x00, 0xff}, hello), 5))
	})

	t.Run("ReservedBlockType", func(t *testing.T) {
		frame := rawFrame([]byte{0x00, 0x00}, hello)
		frame[6] |= 3 << 1
		require.False(t, bb_zstd.IsReusableFrame(frame, 5))
	})

	t.Run("BlockTooLarge", func(t *testing.T) {
		data := pseudoRandomData(t, 128<<10+1)
		require.False(t, bb_zstd.IsReusableFrame(rawFrame([]byte{0x00, 0x48}, data), len(data)))
		require.True(t, bb_zstd.IsReusableFrame(rawFrame([]byte{0x00, 0x48}, data[:128<<10]), 128<<10))
	})

	t.Run("MissingChecksum", func(t *testing.T) {
		// Frame header announces a checksum that is not there.
		require.False(t, bb_zstd.IsReusableFrame(rawFrame([]byte{0x04, 0x00}, hello), 5))
	})

	t.Run("Mutations", func(t *testing.T) {
		// Any reusable frame that decodes must decode to exactly
		// the announced size, and must be a single frame
		// according to an independent parser.
		random := rand.New(rand.NewSource(42))
		frames := [][]byte{
			bb_zstd.Encode(compressible[:20000]),
			bb_zstd.Encode(hello),
			bb_zstd.Encode(incompressible[:4096]),
			rawFrame([]byte{0x20, 5}, hello),
		}
		accepted := 0
		for range 50000 {
			frame := append([]byte(nil), frames[random.Intn(len(frames))]...)
			for range 1 + random.Intn(3) {
				switch random.Intn(3) {
				case 0:
					frame[random.Intn(min(len(frame), 24))] ^= 1 << random.Intn(8)
				case 1:
					frame = frame[:random.Intn(len(frame))+1]
				case 2:
					frame = append(frame, byte(random.Intn(256)))
				}
			}
			decoded, err := bb_zstd.Decode(frame, 1<<16)
			if err != nil || !bb_zstd.IsReusableFrame(frame, len(decoded)) {
				continue
			}
			accepted++
			require.LessOrEqual(t, len(frame), len(decoded)+len(decoded)/256+32)
			var header zstd.Header
			require.NoError(t, header.Decode(frame))
			require.False(t, header.Skippable)
			require.Zero(t, header.DictionaryID)
			if header.HasFCS {
				require.Equal(t, uint64(len(decoded)), header.FrameContentSize)
			}
			// Removing the final byte must break the frame,
			// as there may be no trailing data.
			_, err = bb_zstd.Decode(frame[:len(frame)-1], 1<<16)
			require.Error(t, err)
		}
		require.NotZero(t, accepted)
	})
}

func TestDecodeChecksumMismatch(t *testing.T) {
	// IsReusableFrame() relies on Decode() rejecting frames whose
	// checksum does not match, as other decoders would refuse them.
	data := bytes.Repeat([]byte("BuildBarn "), 1000)
	frame := bb_zstd.Encode(data)
	frame[len(frame)-1] ^= 0xff
	require.True(t, bb_zstd.IsReusableFrame(frame, len(data)))
	_, err := bb_zstd.Decode(frame, len(data))
	require.Error(t, err)
}
