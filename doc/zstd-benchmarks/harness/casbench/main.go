//go:build ignore

// casbench drives a REAPI CAS the way siso does: FindMissingBlobs followed by
// BatchUpdateBlobs (64 concurrent calls of up to 32 blobs) for small blobs and
// ByteStream for large ones, and reads everything back with BatchReadBlobs /
// ByteStream.Read, verifying digests. It reports per-phase wall time, RPC
// counts and gRPC payload bytes, and client/frontend/shard CPU from /proc.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	rpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/errgroup"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
)

type methodStats struct {
	Calls    int64 `json:"calls"`
	SentWire int64 `json:"sent"`
	RecvWire int64 `json:"recv"`
}
type rpcStats struct {
	mu sync.Mutex
	m  map[string]*methodStats
}
type methodKey struct{}

func (s *rpcStats) get(method string) *methodStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	ms, ok := s.m[method]
	if !ok {
		ms = &methodStats{}
		s.m[method] = ms
	}
	return ms
}

func (s *rpcStats) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	name := info.FullMethodName
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	return context.WithValue(ctx, methodKey{}, s.get(name))
}

func (s *rpcStats) HandleRPC(ctx context.Context, st stats.RPCStats) {
	ms, _ := ctx.Value(methodKey{}).(*methodStats)
	if ms == nil {
		return
	}
	switch v := st.(type) {
	case *stats.OutPayload:
		atomic.AddInt64(&ms.SentWire, int64(v.WireLength))
	case *stats.InPayload:
		atomic.AddInt64(&ms.RecvWire, int64(v.WireLength))
	case *stats.End:
		atomic.AddInt64(&ms.Calls, 1)
	}
}
func (s *rpcStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (s *rpcStats) HandleConn(context.Context, stats.ConnStats)                       {}
func (s *rpcStats) snapshot() map[string]methodStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]methodStats{}
	for k, v := range s.m {
		out[k] = methodStats{atomic.LoadInt64(&v.Calls), atomic.LoadInt64(&v.SentWire), atomic.LoadInt64(&v.RecvWire)}
	}
	return out
}

func procCPU(pid int) float64 {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return -1
	}
	s := string(b)
	f := strings.Fields(s[strings.LastIndex(s, ")")+2:])
	u, _ := strconv.ParseFloat(f[11], 64)
	k, _ := strconv.ParseFloat(f[12], 64)
	return (u + k) / 100
}

func pidsCPU(pids []int) float64 {
	t := 0.0
	for _, p := range pids {
		t += procCPU(p)
	}
	return t
}

func selfCPU() float64 {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return float64(ru.Utime.Sec+ru.Stime.Sec) + float64(ru.Utime.Usec+ru.Stime.Usec)/1e6
}

func proxyBytes(url string) (int64, int64) {
	if url == "" {
		return 0, 0
	}
	resp, err := http.Get(url)
	if err != nil {
		return -1, -1
	}
	defer resp.Body.Close()
	var m map[string]int64
	json.NewDecoder(resp.Body).Decode(&m)
	return m["c2s"], m["s2c"]
}

type phaseResult struct {
	Phase       string                 `json:"phase"`
	Blobs       int                    `json:"blobs"`
	Bytes       int64                  `json:"bytes"`
	WallSec     float64                `json:"wall_s"`
	ClientCPU   float64                `json:"client_cpu_s"`
	FrontendCPU float64                `json:"frontend_cpu_s"`
	ShardCPU    float64                `json:"shard_cpu_s"`
	TCPUp       int64                  `json:"tcp_c2s"`
	TCPDown     int64                  `json:"tcp_s2c"`
	RPC         map[string]methodStats `json:"rpc"`
	Errors      int64                  `json:"errors"`
	Verified    int64                  `json:"verified"`
}

type item struct {
	data []byte
	d    *rpb.Digest
}

func parsePids(s string) (out []int) {
	for _, f := range strings.Split(s, ",") {
		if f != "" {
			p, _ := strconv.Atoi(f)
			out = append(out, p)
		}
	}
	return
}

func main() {
	addr := flag.String("addr", "127.0.0.1:18980", "frontend address")
	caCert := flag.String("tls_ca_cert", "", "CA cert; empty means plaintext")
	corpus := flag.String("corpus", "corpus.txt", "file with one path per line")
	salt := flag.String("salt", "", "bytes appended to each blob so each run uploads new content")
	phases := flag.String("phases", "up-batch,up-stream,down-batch,down-stream", "")
	batchCompress := flag.Int("batch_compress", 0, "compress blobs larger than this in BatchUpdateBlobs and accept ZSTD in BatchReadBlobs; 0 = off")
	streamCompress := flag.Bool("stream_compress", false, "use compressed-blobs/zstd for ByteStream")
	zstdLevel := flag.String("zstd_level", "default", "default|fastest")
	streamThreshold := flag.Int("stream_threshold", 2<<20, "blobs at least this large use ByteStream")
	concurrency := flag.Int("concurrency", 64, "concurrent RPCs")
	batchBlobs := flag.Int("batch_blobs", 32, "max blobs per batch RPC")
	batchBytes := flag.Int("batch_bytes", 3<<20, "max payload bytes per batch RPC")
	frontendPid := flag.String("frontend_pid", "", "")
	shardPids := flag.String("shard_pids", "", "")
	proxyStats := flag.String("proxy_stats", "", "")
	out := flag.String("out", "", "")
	flag.Parse()
	fePids, shPids := parsePids(*frontendPid), parsePids(*shardPids)

	level := zstd.SpeedDefault
	if *zstdLevel == "fastest" {
		level = zstd.SpeedFastest
	}
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(level), zstd.WithEncoderConcurrency(4))
	dec, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(4))

	// Load corpus.
	var batchItems, streamItems []item
	f, err := os.Open(*corpus)
	if err != nil {
		log.Fatal(err)
	}
	seen := map[string]bool{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		b, err := os.ReadFile(sc.Text())
		if err != nil || len(b) == 0 {
			continue
		}
		b = append(b, []byte(*salt)...)
		h := sha256.Sum256(b)
		hs := hex.EncodeToString(h[:])
		if seen[hs] {
			continue
		}
		seen[hs] = true
		it := item{data: b, d: &rpb.Digest{Hash: hs, SizeBytes: int64(len(b))}}
		if len(b) >= *streamThreshold {
			streamItems = append(streamItems, it)
		} else {
			batchItems = append(batchItems, it)
		}
	}

	st := &rpcStats{m: map[string]*methodStats{}}
	creds := insecure.NewCredentials()
	if *caCert != "" {
		pem, err := os.ReadFile(*caCert)
		if err != nil {
			log.Fatal(err)
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(pem)
		creds = credentials.NewTLS(&tls.Config{RootCAs: pool})
	}
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(creds), grpc.WithStatsHandler(st),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(64<<20)))
	if err != nil {
		log.Fatal(err)
	}
	casc := rpb.NewContentAddressableStorageClient(conn)
	bsc := bytestream.NewByteStreamClient(conn)
	capc := rpb.NewCapabilitiesClient(conn)
	ctx := context.Background()
	caps, err := capc.GetCapabilities(ctx, &rpb.GetCapabilitiesRequest{})
	if err != nil {
		log.Fatal(err)
	}
	cc := caps.GetCacheCapabilities()
	has := func(l []rpb.Compressor_Value) bool {
		for _, c := range l {
			if c == rpb.Compressor_ZSTD {
				return true
			}
		}
		return false
	}
	// Mirror siso: BatchUpdate compression is gated on
	// supported_batch_update_compressors, everything else on
	// supported_compressors.
	upBatchZstd := *batchCompress > 0 && has(cc.GetSupportedBatchUpdateCompressors())
	downBatchZstd := *batchCompress > 0 && has(cc.GetSupportedCompressors())
	streamZstd := *streamCompress && has(cc.GetSupportedCompressors())
	fmt.Fprintf(os.Stderr, "caps: compressors=%v batch_update=%v -> up-batch zstd=%v down-batch zstd=%v stream zstd=%v\n",
		cc.GetSupportedCompressors(), cc.GetSupportedBatchUpdateCompressors(), upBatchZstd, downBatchZstd, streamZstd)

	mkBatches := func(items []item) [][]item {
		var batches [][]item
		var cur []item
		sz := 0
		for _, it := range items {
			if len(cur) > 0 && (len(cur) >= *batchBlobs || sz+len(it.data) > *batchBytes) {
				batches = append(batches, cur)
				cur, sz = nil, 0
			}
			cur = append(cur, it)
			sz += len(it.data)
		}
		if len(cur) > 0 {
			batches = append(batches, cur)
		}
		return batches
	}

	var results []phaseResult
	run := func(name string, items []item, fn func(errs, verified *int64) error) {
		var bytesTotal int64
		for _, it := range items {
			bytesTotal += int64(len(it.data))
		}
		before := st.snapshot()
		c0, f0, s0 := selfCPU(), pidsCPU(fePids), pidsCPU(shPids)
		u0, d0 := proxyBytes(*proxyStats)
		t0 := time.Now()
		var errs, verified int64
		if err := fn(&errs, &verified); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
			errs++
		}
		wall := time.Since(t0).Seconds()
		time.Sleep(300 * time.Millisecond) // let server-side work settle into /proc
		u1, d1 := proxyBytes(*proxyStats)
		after := st.snapshot()
		rpc := map[string]methodStats{}
		for k, v := range after {
			b := before[k]
			if v.Calls != b.Calls {
				rpc[k] = methodStats{v.Calls - b.Calls, v.SentWire - b.SentWire, v.RecvWire - b.RecvWire}
			}
		}
		r := phaseResult{name, len(items), bytesTotal, wall, selfCPU() - c0, pidsCPU(fePids) - f0, pidsCPU(shPids) - s0, u1 - u0, d1 - d0, rpc, errs, verified}
		results = append(results, r)
		fmt.Fprintf(os.Stderr, "%s: blobs=%d MB=%.1f wall=%.2fs client=%.2fs frontend=%.2fs shards=%.2fs errors=%d verified=%d rpc=%v\n",
			name, r.Blobs, float64(r.Bytes)/1e6, r.WallSec, r.ClientCPU, r.FrontendCPU, r.ShardCPU, r.Errors, r.Verified, rpc)
	}

	for _, phase := range strings.Split(*phases, ",") {
		switch phase {
		case "up-batch":
			run(phase, batchItems, func(errs, verified *int64) error {
				g, gctx := errgroup.WithContext(ctx)
				g.SetLimit(*concurrency)
				for _, batch := range mkBatches(batchItems) {
					g.Go(func() error {
						fm := &rpb.FindMissingBlobsRequest{}
						byHash := map[string]item{}
						for _, it := range batch {
							fm.BlobDigests = append(fm.BlobDigests, it.d)
							byHash[it.d.Hash] = it
						}
						resp, err := casc.FindMissingBlobs(gctx, fm)
						if err != nil {
							return err
						}
						req := &rpb.BatchUpdateBlobsRequest{}
						for _, d := range resp.MissingBlobDigests {
							it := byHash[d.Hash]
							r := &rpb.BatchUpdateBlobsRequest_Request{Digest: it.d, Data: it.data}
							if upBatchZstd && len(it.data) > *batchCompress {
								r.Data = enc.EncodeAll(it.data, make([]byte, 0, len(it.data)/2))
								r.Compressor = rpb.Compressor_ZSTD
							}
							req.Requests = append(req.Requests, r)
						}
						if len(req.Requests) == 0 {
							return nil
						}
						uresp, err := casc.BatchUpdateBlobs(gctx, req)
						if err != nil {
							return err
						}
						for _, r := range uresp.Responses {
							if r.GetStatus().GetCode() != 0 {
								atomic.AddInt64(errs, 1)
								fmt.Fprintf(os.Stderr, "update error: %v\n", r.GetStatus())
							}
						}
						return nil
					})
				}
				return g.Wait()
			})
		case "down-batch":
			run(phase, batchItems, func(errs, verified *int64) error {
				g, gctx := errgroup.WithContext(ctx)
				g.SetLimit(*concurrency)
				for _, batch := range mkBatches(batchItems) {
					g.Go(func() error {
						req := &rpb.BatchReadBlobsRequest{}
						if downBatchZstd {
							req.AcceptableCompressors = []rpb.Compressor_Value{rpb.Compressor_ZSTD}
						}
						for _, it := range batch {
							req.Digests = append(req.Digests, it.d)
						}
						resp, err := casc.BatchReadBlobs(gctx, req)
						if err != nil {
							return err
						}
						for _, r := range resp.Responses {
							if r.GetStatus().GetCode() != 0 {
								atomic.AddInt64(errs, 1)
								continue
							}
							data := r.Data
							if os.Getenv("DUMP_FRAMES") != "" {
								var h zstd.Header
								h.Decode(r.Data)
								fh := sha256.Sum256(r.Data)
								fmt.Printf("%s %d %x ss=%v win=%d fcs=%v first=%+v\n", r.Digest.Hash[:12], len(r.Data), fh[:6], h.SingleSegment, h.WindowSize, h.HasFCS, h.FirstBlock)
							}
							if r.Compressor == rpb.Compressor_ZSTD {
								if data, err = dec.DecodeAll(r.Data, nil); err != nil {
									atomic.AddInt64(errs, 1)
									continue
								}
							}
							h := sha256.Sum256(data)
							if hex.EncodeToString(h[:]) == r.Digest.Hash {
								atomic.AddInt64(verified, 1)
							} else {
								atomic.AddInt64(errs, 1)
							}
						}
						return nil
					})
				}
				return g.Wait()
			})
		case "up-stream":
			run(phase, streamItems, func(errs, verified *int64) error {
				g, gctx := errgroup.WithContext(ctx)
				g.SetLimit(*concurrency)
				for i, it := range streamItems {
					g.Go(func() error {
						fm, err := casc.FindMissingBlobs(gctx, &rpb.FindMissingBlobsRequest{BlobDigests: []*rpb.Digest{it.d}})
						if err != nil {
							return err
						}
						if len(fm.MissingBlobDigests) == 0 {
							return nil
						}
						name := fmt.Sprintf("uploads/%08d-0000-4000-8000-000000000000/blobs/%s/%d", i, it.d.Hash, it.d.SizeBytes)
						data := it.data
						if streamZstd {
							name = fmt.Sprintf("uploads/%08d-0000-4000-8000-000000000000/compressed-blobs/zstd/%s/%d", i, it.d.Hash, it.d.SizeBytes)
							data = enc.EncodeAll(it.data, make([]byte, 0, len(it.data)/2))
						}
						w, err := bsc.Write(gctx)
						if err != nil {
							return err
						}
						const chunk = 1 << 20
						for off := 0; ; off += chunk {
							end := min(off+chunk, len(data))
							req := &bytestream.WriteRequest{WriteOffset: int64(off), Data: data[off:end], FinishWrite: end == len(data)}
							if off == 0 {
								req.ResourceName = name
							}
							if err := w.Send(req); err != nil {
								if err == io.EOF {
									break
								}
								return err
							}
							if end == len(data) {
								break
							}
						}
						if _, err := w.CloseAndRecv(); err != nil {
							atomic.AddInt64(errs, 1)
							fmt.Fprintf(os.Stderr, "write error: %v\n", err)
						}
						return nil
					})
				}
				return g.Wait()
			})
		case "down-stream":
			run(phase, streamItems, func(errs, verified *int64) error {
				g, gctx := errgroup.WithContext(ctx)
				g.SetLimit(*concurrency)
				for _, it := range streamItems {
					g.Go(func() error {
						name := fmt.Sprintf("blobs/%s/%d", it.d.Hash, it.d.SizeBytes)
						if streamZstd {
							name = fmt.Sprintf("compressed-blobs/zstd/%s/%d", it.d.Hash, it.d.SizeBytes)
						}
						r, err := bsc.Read(gctx, &bytestream.ReadRequest{ResourceName: name})
						if err != nil {
							return err
						}
						var buf bytes.Buffer
						for {
							m, err := r.Recv()
							if err == io.EOF {
								break
							}
							if err != nil {
								atomic.AddInt64(errs, 1)
								fmt.Fprintf(os.Stderr, "read error: %v\n", err)
								return nil
							}
							buf.Write(m.Data)
						}
						data := buf.Bytes()
						if streamZstd {
							if data, err = dec.DecodeAll(data, nil); err != nil {
								atomic.AddInt64(errs, 1)
								return nil
							}
						}
						h := sha256.Sum256(data)
						if hex.EncodeToString(h[:]) == it.d.Hash {
							atomic.AddInt64(verified, 1)
						} else {
							atomic.AddInt64(errs, 1)
						}
						return nil
					})
				}
				return g.Wait()
			})
		}
	}
	if *out != "" {
		b, _ := json.MarshalIndent(results, "", " ")
		os.WriteFile(*out, b, 0o644)
	}
}
