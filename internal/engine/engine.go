package engine

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shaneburrell/quiksync/internal/autotune"
	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/delta"
	"github.com/shaneburrell/quiksync/internal/fsmeta"
	"github.com/shaneburrell/quiksync/internal/index"
	"github.com/shaneburrell/quiksync/internal/journal"
	"github.com/shaneburrell/quiksync/internal/transfer"
	"github.com/shaneburrell/quiksync/internal/transport"
	"github.com/shaneburrell/quiksync/internal/transport/daemon"
	"github.com/shaneburrell/quiksync/internal/transport/local"
	sshxfer "github.com/shaneburrell/quiksync/internal/transport/ssh"
)

type Config struct {
	Source          string
	Dest            string
	SyncMode        bool
	Delete          bool
	Resume          bool
	DryRun          bool
	Exclude         []string
	Checksum        bool
	StableWindow    time.Duration
	BandwidthLimit  int64
	SkipUnstable    bool
	MaxFileAttempts int
	Tune            autotune.Config
	Verbose         bool

	// Test hooks (nil in production).
	TestAfterFile    func(rel, status string)
	TestBeforeCommit func(rel string)
}

type Stats struct {
	FilesCopied  int64
	FilesSkipped int64
	FilesFailed  int64
	FilesDeleted int64
	BytesCopied  int64 // uncompressed payload written for missing chunks
	BytesPayload int64 // same as BytesCopied; wire-oriented accounting
	ChunksReused int64
	ChunksSent   int64
}

type fileResult struct {
	copied, skipped, failed int64
	bytes, reused, sent     int64
	err                     error
}

func Run(ctx context.Context, cfg Config) (Stats, error) {
	if cfg.MaxFileAttempts <= 0 {
		cfg.MaxFileAttempts = 5
	}
	srcEP, err := transport.ParseEndpoint(cfg.Source)
	if err != nil {
		return Stats{}, err
	}
	dstEP, err := transport.ParseEndpoint(cfg.Dest)
	if err != nil {
		return Stats{}, err
	}

	src, err := openTransport(srcEP)
	if err != nil {
		return Stats{}, fmt.Errorf("source: %w", err)
	}
	defer src.Close()
	dst, err := openTransport(dstEP)
	if err != nil {
		return Stats{}, fmt.Errorf("dest: %w", err)
	}
	defer dst.Close()

	jobID := "job"
	if cfg.Resume {
		jobID = "resume"
	}
	var journ *journal.Journal
	var idx *index.Cache
	if dstEP.Scheme == "file" {
		journ, err = journal.Open(dst.Root(), jobID)
		if err != nil {
			return Stats{}, err
		}
		idx, err = index.Open(dst.Root())
		if err != nil {
			return Stats{}, err
		}
	}

	hostKey := dstEP.Host
	if hostKey == "" {
		hostKey = "local"
	}
	if cfg.Tune.ProfilePath == "" && dstEP.Scheme == "file" {
		cfg.Tune.ProfilePath = dst.Root() + "/.quiksync/profiles/" + hostKey + ".json"
	}
	tuner := autotune.New(cfg.Tune, hostKey)
	limiter := transfer.NewLimiter(cfg.BandwidthLimit)

	srcFiles, err := src.Walk(ctx, cfg.Exclude)
	if err != nil {
		return Stats{}, err
	}

	sample := make([]byte, 0, 64*1024)
	for _, f := range srcFiles {
		if f.Size == 0 {
			continue
		}
		rc, err := src.OpenRead(ctx, f.RelPath)
		if err != nil {
			continue
		}
		buf := make([]byte, 64*1024)
		n, _ := io.ReadFull(rc, buf)
		_ = rc.Close()
		sample = buf[:max(0, n)]
		break
	}
	prof := tuner.Probe(sample, 10)
	if cfg.Verbose {
		fmt.Fprintf(os.Stderr, "autotune profile: streams=%d compress=%s frame=%d\n", prof.Streams, prof.Compress, prof.FrameSize)
	}

	type job struct{ meta transport.FileMeta }
	jobs := make(chan job, len(srcFiles))
	for _, f := range srcFiles {
		jobs <- job{meta: f}
	}
	close(jobs)

	var stats Stats
	var wg sync.WaitGroup
	workers := prof.Streams
	if workers < 1 {
		workers = 1
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if err := ctx.Err(); err != nil {
					return
				}
				res := transferOne(ctx, cfg, src, dst, j.meta, journ, idx, tuner, limiter)
				atomic.AddInt64(&stats.FilesCopied, res.copied)
				atomic.AddInt64(&stats.FilesSkipped, res.skipped)
				atomic.AddInt64(&stats.FilesFailed, res.failed)
				atomic.AddInt64(&stats.BytesCopied, res.bytes)
				atomic.AddInt64(&stats.BytesPayload, res.bytes)
				atomic.AddInt64(&stats.ChunksReused, res.reused)
				atomic.AddInt64(&stats.ChunksSent, res.sent)
				if res.err != nil && cfg.Verbose {
					fmt.Fprintf(os.Stderr, "warn %s: %v\n", j.meta.RelPath, res.err)
				}
				if cfg.TestAfterFile != nil {
					status := "failed"
					if res.skipped > 0 {
						status = "skipped"
					} else if res.copied > 0 {
						status = "ok"
					}
					cfg.TestAfterFile(j.meta.RelPath, status)
				}
				tuner.Observe(autotune.Sample{
					BytesVerified: res.bytes,
					Elapsed:       time.Second,
					ErrorRate:     float64(res.failed),
					CPUPercent:    20,
					CompressRatio: 1.2,
					RTTMs:         10,
				})
			}
		}()
	}
	wg.Wait()
	_ = tuner.Save()

	if cfg.SyncMode && cfg.Delete && !cfg.DryRun {
		deleted, err := deleteExtras(ctx, srcFiles, dst)
		stats.FilesDeleted = deleted
		if err != nil {
			return stats, err
		}
	}
	return stats, nil
}

func transferOne(
	ctx context.Context,
	cfg Config,
	src, dst transport.Transport,
	meta transport.FileMeta,
	journ *journal.Journal,
	idx *index.Cache,
	tuner *autotune.Tuner,
	limiter *transfer.Limiter,
) fileResult {
	attempts := 0
	for {
		attempts++
		n, reused, sent, status, err := tryTransfer(ctx, cfg, src, dst, meta, journ, idx, tuner, limiter)
		if status == "skipped" {
			return fileResult{skipped: 1}
		}
		if status == "ok" {
			return fileResult{copied: 1, bytes: n, reused: reused, sent: sent}
		}
		if err != nil && isUnstable(err) {
			if cfg.SkipUnstable || attempts >= cfg.MaxFileAttempts {
				if journ != nil {
					_ = journ.Put(journal.Entry{
						JobID: "resume", RelPath: meta.RelPath, Status: journal.StatusFailed,
						Attempts: attempts, LastError: err.Error(),
					})
				}
				return fileResult{failed: 1, err: err}
			}
			time.Sleep(time.Duration(attempts) * 200 * time.Millisecond)
			if nm, e := src.Stat(ctx, meta.RelPath); e == nil {
				meta = nm
			}
			continue
		}
		if err != nil {
			if journ != nil {
				_ = journ.Put(journal.Entry{
					JobID: "resume", RelPath: meta.RelPath, Status: journal.StatusFailed,
					Attempts: attempts, LastError: err.Error(),
				})
			}
			return fileResult{failed: 1, err: err}
		}
		return fileResult{failed: 1, err: err}
	}
}

type unstableError struct{ error }

func isUnstable(err error) bool {
	_, ok := err.(unstableError)
	return ok
}

func tryTransfer(
	ctx context.Context,
	cfg Config,
	src, dst transport.Transport,
	meta transport.FileMeta,
	journ *journal.Journal,
	idx *index.Cache,
	tuner *autotune.Tuner,
	limiter *transfer.Limiter,
) (bytes, reused, sent int64, status string, err error) {
	if cfg.StableWindow > 0 {
		fi := fsmeta.FileInfo{ModTime: meta.ModTime, Size: meta.Size}
		if !fsmeta.UnchangedFor(fi, cfg.StableWindow) {
			return 0, 0, 0, "failed", unstableError{fmt.Errorf("file not stable yet")}
		}
	}

	gen := fsmeta.Generation{Size: meta.Size, ModNano: meta.ModTime.UnixNano()}
	if journ != nil && cfg.Resume {
		if e, ok := journ.Get(meta.RelPath); ok && e.Status == journal.StatusComplete {
			if e.SrcSize == gen.Size && e.SrcModNano == gen.ModNano && !cfg.Checksum {
				return 0, 0, 0, "skipped", nil
			}
		}
	}

	if !cfg.Checksum {
		if dm, err := dst.Stat(ctx, meta.RelPath); err == nil {
			if dm.Size == meta.Size && dm.ModTime.UnixNano() == meta.ModTime.UnixNano() {
				return 0, 0, 0, "skipped", nil
			}
		}
	}

	rc, err := src.OpenRead(ctx, meta.RelPath)
	if err != nil {
		return 0, 0, 0, "failed", err
	}
	prof := tuner.Profile()
	opt := chunk.Options{AvgSize: prof.FrameSize, KeepData: true}
	sig, err := chunk.ChunkReader(rc, meta.Size, opt)
	_ = rc.Close()
	if err != nil {
		return 0, 0, 0, "failed", err
	}

	if st, err := src.Stat(ctx, meta.RelPath); err == nil {
		if st.Size != gen.Size || st.ModTime.UnixNano() != gen.ModNano {
			return 0, 0, 0, "failed", unstableError{fmt.Errorf("source changed during read")}
		}
	}

	var destSig chunk.FileSignature
	if idx != nil {
		if dm, err := dst.Stat(ctx, meta.RelPath); err == nil {
			if cached, ok := idx.Get(meta.RelPath, dm.Size, dm.ModTime.UnixNano()); ok {
				destSig = cached
			}
		}
	}
	if len(destSig.Chunks) == 0 {
		destSig, _ = dst.GetSignature(ctx, meta.RelPath)
	}

	if !delta.NeedsTransfer(sig, destSig, cfg.Checksum) && sig.Digest == destSig.Digest {
		return 0, 0, 0, "skipped", nil
	}
	plan := delta.Diff(sig, destSig)
	missingSet := make(map[chunk.Digest]struct{}, len(plan.Missing))
	for _, c := range plan.Missing {
		missingSet[c.Digest] = struct{}{}
	}

	if cfg.DryRun {
		var payload int64
		for _, c := range plan.Missing {
			payload += int64(c.Length)
		}
		if payload == 0 && sig.Digest != destSig.Digest {
			payload = sig.Size
		}
		return payload, int64(plan.Reuse), int64(len(plan.Missing)), "ok", nil
	}

	if journ != nil {
		_ = journ.Put(journal.Entry{
			JobID: "resume", RelPath: meta.RelPath, Status: journal.StatusInProgress,
			SrcDigest: sig.Digest.String(), SrcSize: gen.Size, SrcModNano: gen.ModNano,
		})
	}

	ws, err := dst.BeginWrite(ctx, meta.RelPath, sig.Size)
	if err != nil {
		return 0, 0, 0, "failed", err
	}

	var payload int64
	var chunksSent int64
	codecPref := prof.Compress
	for _, c := range sig.Chunks {
		data := c.Data
		if data == nil {
			_ = ws.Abort()
			return 0, 0, 0, "failed", fmt.Errorf("missing chunk data")
		}
		_, isMissing := missingSet[c.Digest]
		if isMissing || len(destSig.Chunks) == 0 {
			if err := limiter.Wait(ctx, len(data)); err != nil {
				_ = ws.Abort()
				return 0, 0, 0, "failed", err
			}
			codec, enc, err := compress.Encode(codecPref, data)
			if err != nil {
				_ = ws.Abort()
				return 0, 0, 0, "failed", err
			}
			if err := ws.WriteChunk(ctx, c.Offset, codec, len(data), enc); err != nil {
				_ = ws.Abort()
				return 0, 0, 0, "failed", err
			}
			payload += int64(len(data))
			chunksSent++
		} else {
			// Reused content: still materialize into temp from local source bytes
			// (already held) but do not count as wire payload.
			if err := ws.WriteChunk(ctx, c.Offset, compress.CodecNone, len(data), data); err != nil {
				_ = ws.Abort()
				return 0, 0, 0, "failed", err
			}
		}
	}

	if st, err := src.Stat(ctx, meta.RelPath); err == nil {
		if st.Size != gen.Size || st.ModTime.UnixNano() != gen.ModNano {
			_ = ws.Abort()
			return 0, 0, 0, "failed", unstableError{fmt.Errorf("source changed before finalize")}
		}
	}

	if cfg.TestBeforeCommit != nil {
		cfg.TestBeforeCommit(meta.RelPath)
		if st, err := src.Stat(ctx, meta.RelPath); err == nil {
			if st.Size != gen.Size || st.ModTime.UnixNano() != gen.ModNano {
				_ = ws.Abort()
				return 0, 0, 0, "failed", unstableError{fmt.Errorf("source changed before finalize")}
			}
		}
	}

	if err := ws.Commit(ctx, sig.Digest, meta.Mode, meta.ModTime); err != nil {
		_ = ws.Abort()
		return 0, 0, 0, "failed", err
	}

	if idx != nil {
		_ = idx.Put(meta.RelPath, sig.Size, gen.ModNano, chunk.FileSignature{
			Size: sig.Size, Digest: sig.Digest, Chunks: stripData(sig.Chunks),
		})
	}
	if journ != nil {
		_ = journ.Put(journal.Entry{
			JobID: "resume", RelPath: meta.RelPath, Status: journal.StatusComplete,
			SrcDigest: sig.Digest.String(), SrcSize: gen.Size, SrcModNano: gen.ModNano,
			ChunksDone: len(sig.Chunks),
		})
	}
	return payload, int64(plan.Reuse), chunksSent, "ok", nil
}

func stripData(in []chunk.Chunk) []chunk.Chunk {
	out := make([]chunk.Chunk, len(in))
	for i, c := range in {
		out[i] = chunk.Chunk{Offset: c.Offset, Length: c.Length, Digest: c.Digest}
	}
	return out
}

func deleteExtras(ctx context.Context, srcFiles []transport.FileMeta, dst transport.Transport) (int64, error) {
	keep := map[string]struct{}{}
	for _, f := range srcFiles {
		keep[f.RelPath] = struct{}{}
	}
	dstFiles, err := dst.Walk(ctx, nil)
	if err != nil {
		return 0, err
	}
	var n int64
	for _, f := range dstFiles {
		if _, ok := keep[f.RelPath]; ok {
			continue
		}
		if err := dst.Remove(ctx, f.RelPath); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func openTransport(ep transport.Endpoint) (transport.Transport, error) {
	switch ep.Scheme {
	case "file":
		return local.New(ep.Path)
	case "ssh":
		return sshxfer.New(ep)
	case "quiksync":
		return daemon.Dial(context.Background(), ep)
	case "s3":
		return nil, fmt.Errorf("s3 transport reserved for a later release")
	default:
		return nil, fmt.Errorf("unsupported endpoint scheme %q", ep.Scheme)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Verify compares digests of regular files.
func Verify(ctx context.Context, source, dest string) ([]string, error) {
	srcEP, err := transport.ParseEndpoint(source)
	if err != nil {
		return nil, err
	}
	dstEP, err := transport.ParseEndpoint(dest)
	if err != nil {
		return nil, err
	}
	src, err := openTransport(srcEP)
	if err != nil {
		return nil, err
	}
	defer src.Close()
	dst, err := openTransport(dstEP)
	if err != nil {
		return nil, err
	}
	defer dst.Close()

	files, err := src.Walk(ctx, nil)
	if err != nil {
		return nil, err
	}
	var mismatches []string
	for _, f := range files {
		sr, err := src.OpenRead(ctx, f.RelPath)
		if err != nil {
			mismatches = append(mismatches, f.RelPath+": source open: "+err.Error())
			continue
		}
		sd, _, err := chunk.HashFile(sr)
		_ = sr.Close()
		if err != nil {
			mismatches = append(mismatches, f.RelPath+": source hash: "+err.Error())
			continue
		}
		dr, err := dst.OpenRead(ctx, f.RelPath)
		if err != nil {
			mismatches = append(mismatches, f.RelPath+": missing on dest")
			continue
		}
		dd, _, err := chunk.HashFile(dr)
		_ = dr.Close()
		if err != nil {
			mismatches = append(mismatches, f.RelPath+": dest hash: "+err.Error())
			continue
		}
		if sd != dd {
			mismatches = append(mismatches, fmt.Sprintf("%s: digest mismatch", f.RelPath))
		}
	}
	return mismatches, nil
}
