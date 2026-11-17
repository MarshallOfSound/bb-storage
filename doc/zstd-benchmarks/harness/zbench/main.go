//go:build ignore

package main

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
)

func main() {
	var blobs [][]byte
	total := 0
	f, _ := os.Open(os.Args[1])
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		b, err := os.ReadFile(sc.Text())
		if err == nil && len(b) > 0 && len(b) < 2<<20 {
			blobs = append(blobs, b)
			total += len(b)
		}
	}
	gb := float64(total) / 1e9
	fmt.Printf("corpus %d blobs %.1f MB\n", len(blobs), float64(total)/1e6)
	dec, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecodeAllCapLimit(true), zstd.WithDecoderLowmem(true))
	decNoCRC, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecodeAllCapLimit(true), zstd.WithDecoderLowmem(true), zstd.IgnoreChecksum(true))
	for _, lv := range []struct {
		n   string
		l   zstd.EncoderLevel
		crc bool
	}{{"fastest", zstd.SpeedFastest, true}, {"default", zstd.SpeedDefault, true}, {"default-nocrc", zstd.SpeedDefault, false}, {"better", zstd.SpeedBetterCompression, true}} {
		enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(lv.l), zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(lv.crc))
		scratch := make([]byte, 0, 4<<20)
		var frames [][]byte
		out := 0
		t := time.Now()
		for _, b := range blobs {
			e := enc.EncodeAll(b, scratch[:0])
			out += len(e)
			frames = append(frames, append([]byte(nil), e...))
		}
		et := time.Since(t).Seconds()
		t = time.Now()
		for i, fr := range frames {
			if _, err := dec.DecodeAll(fr, make([]byte, 0, len(blobs[i]))); err != nil {
				panic(err)
			}
		}
		dt := time.Since(t).Seconds()
		t = time.Now()
		for i, fr := range frames {
			decNoCRC.DecodeAll(fr, make([]byte, 0, len(blobs[i])))
		}
		dt2 := time.Since(t).Seconds()
		fmt.Printf("%-14s ratio=%.3f encode=%.2f core-s/GB decode=%.2f core-s/GB decode(ignore crc)=%.2f\n", lv.n, float64(out)/float64(total), et/gb, dt/gb, dt2/gb)
	}
	t := time.Now()
	for _, b := range blobs {
		sha256.Sum256(b)
	}
	fmt.Printf("sha256 %.2f core-s/GB\n", time.Since(t).Seconds()/gb)
}
