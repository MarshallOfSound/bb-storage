package zstd

import (
	"github.com/klauspost/compress/zstd"
)

// GetFrameContentSize returns the size of the decompressed data, as
// announced by the header of the first ZSTD frame stored in the
// provided data. The size is announced by the creator of the frame, and
// is not validated against the actual contents of the frame. It should
// therefore only be relied upon if the creator of the frame is trusted,
// or if the data is decompressed and validated afterwards.
//
// False is returned if the data does not start with a valid ZSTD frame
// header, or if the header does not announce a size.
func GetFrameContentSize(data []byte) (uint64, bool) {
	var header zstd.Header
	if err := header.Decode(data); err != nil || header.Skippable || !header.HasFCS {
		return 0, false
	}
	return header.FrameContentSize, true
}
