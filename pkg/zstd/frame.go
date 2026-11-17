package zstd

import (
	"encoding/binary"
)

const (
	frameMagic = 0xfd2fb528

	// The largest window size a frame may declare for it to be
	// reusable. This matches the default window size of the encoder
	// used by Encode(), meaning that frames produced by ordinary
	// clients pass. Any conforming decoder is required to support
	// windows of this size.
	maximumReusableWindowSizeBytes = 8 << 20

	// The maximum size of a block, both in compressed and
	// decompressed form.
	maximumBlockSizeBytes = 128 << 10
)

// maximumReusableFrameSizeBytes returns the size of the largest frame
// that IsReusableFrame() accepts for data of a given size.
//
// Data that cannot be compressed ends up being stored in uncompressed
// blocks, which makes the frame slightly larger than the data itself.
// Encode() does the same for data that is too small to be worth
// compressing. Permit just enough overhead for a frame header, a
// checksum, and a three byte header for every 1 KiB of data, so that
// the frames that encoders emit for such data remain reusable.
func maximumReusableFrameSizeBytes(decodedSizeBytes int) int {
	return decodedSizeBytes + decodedSizeBytes/256 + 32
}

// IsReusableFrame reports whether ZSTD data provided by an untrusted
// party may be retained and handed out to others as the compressed
// representation of an object, as opposed to compressing the object
// again.
//
// This function only inspects the structure of the frame. The caller
// MUST have successfully decompressed the data using Decode(), and
// have validated that the resulting decodedSizeBytes bytes of data
// correspond to the object, e.g. by comparing its digest. Decode()
// also validates the checksum of the frame, if present.
//
// Data is only considered reusable if all of the following hold:
//
//   - It consists of exactly one ZSTD frame. No skippable frames,
//     additional frames or other trailing data are present, meaning the
//     data cannot be used to smuggle anything that is not covered by
//     the object's digest.
//   - It does not depend on a dictionary.
//   - The window size is bounded, so that it can be decompressed by any
//     decoder without requiring excessive amounts of memory.
//   - It is at most marginally larger than the object itself, as is
//     the case for incompressible data stored in uncompressed blocks.
//     Storing it is therefore never meaningfully worse than not
//     compressing the object at all.
//   - Its header does not use any reserved features and, if present,
//     declares a frame content size that matches the object.
func IsReusableFrame(frame []byte, decodedSizeBytes int) bool {
	if decodedSizeBytes < 0 || len(frame) > maximumReusableFrameSizeBytes(decodedSizeBytes) {
		return false
	}

	// Magic number and frame header descriptor. Skippable frames use
	// a different magic number, so those are rejected here as well.
	if len(frame) < 5 || binary.LittleEndian.Uint32(frame) != frameMagic {
		return false
	}
	descriptor := frame[4]
	frameContentSizeFlag := descriptor >> 6
	singleSegment := descriptor&0x20 != 0
	hasChecksum := descriptor&0x04 != 0
	if descriptor&0x18 != 0 {
		// Unused or reserved bits set.
		return false
	}
	if descriptor&0x03 != 0 {
		// Dictionary ID present.
		return false
	}
	offset := 5

	// Window descriptor.
	if !singleSegment {
		if len(frame) < offset+1 {
			return false
		}
		windowDescriptor := frame[offset]
		offset++
		windowBase := uint64(1) << (10 + windowDescriptor>>3)
		windowSize := windowBase + windowBase/8*uint64(windowDescriptor&0x07)
		if windowSize > maximumReusableWindowSizeBytes {
			return false
		}
	}

	// Frame content size. Frames using a single segment always
	// contain one, and use it as their window size.
	frameContentSizeFieldSize := 0
	switch frameContentSizeFlag {
	case 0:
		if singleSegment {
			frameContentSizeFieldSize = 1
		}
	case 1:
		frameContentSizeFieldSize = 2
	case 2:
		frameContentSizeFieldSize = 4
	case 3:
		frameContentSizeFieldSize = 8
	}
	if len(frame) < offset+frameContentSizeFieldSize {
		return false
	}
	if frameContentSizeFieldSize > 0 {
		var frameContentSize uint64
		switch frameContentSizeFieldSize {
		case 1:
			frameContentSize = uint64(frame[offset])
		case 2:
			frameContentSize = uint64(binary.LittleEndian.Uint16(frame[offset:])) + 256
		case 4:
			frameContentSize = uint64(binary.LittleEndian.Uint32(frame[offset:]))
		case 8:
			frameContentSize = binary.LittleEndian.Uint64(frame[offset:])
		}
		if frameContentSize != uint64(decodedSizeBytes) {
			return false
		}
		offset += frameContentSizeFieldSize
	}

	// Walk over all of the blocks, so that we can determine where the
	// frame ends.
	for {
		if len(frame) < offset+3 {
			return false
		}
		blockHeader := uint32(frame[offset]) | uint32(frame[offset+1])<<8 | uint32(frame[offset+2])<<16
		offset += 3
		blockSize := int(blockHeader >> 3)
		if blockSize > maximumBlockSizeBytes {
			return false
		}
		switch (blockHeader >> 1) & 0x03 {
		case 0, 2:
			// Raw and compressed blocks.
			offset += blockSize
		case 1:
			// RLE blocks consist of a single byte that is
			// repeated.
			offset++
		default:
			// Reserved block type.
			return false
		}
		if offset > len(frame) {
			return false
		}
		if blockHeader&0x01 != 0 {
			// Last block.
			break
		}
	}

	// Optional checksum. There may be no data past the end of the
	// frame.
	if hasChecksum {
		offset += 4
	}
	return offset == len(frame)
}
