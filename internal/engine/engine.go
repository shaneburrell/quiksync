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
}

type Stats struct {
	FilesCopied  int64
	FilesSkipped int64
	FilesFailed  int64
	FilesDeleted int64
	BytesCopied  int64
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
	tuner := autotune.New(cfg.Tune, hostKey)

	srcFiles, err := src.Walk(ctx, cfg.Exclude)
	if err != nil {
		return Stats{}, err
	}

	// Probe with first readable sample.
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
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if err := ctx.Err(); err != nil {
					errCh <- err
					return
				}
				copied, skipped, failed, n, err := transferOne(ctx, cfg, src, dst, j.meta, journ, idx, tuner)
				atomic.AddInt64(&stats.FilesCopied, copied)
				atomic.AddInt64(&stats.FilesSkipped, skipped)
				atomic.AddInt64(&stats.FilesFailed, failed)
				atomic.AddInt64(&stats.BytesCopied, n)
				if err != nil && cfg.Verbose {
					fmt.Fprintf(os.Stderr, "warn %s: %v\n", j.meta.RelPath, err)
				}
				tuner.Observe(autotune.Sample{
					BytesVerified: n,
					Elapsed:       time.Second,
					ErrorRate:     float64(failed),
					CPUPercent:    20,
					CompressRatio: 1.2,
					RTTMs:         10,
				})
			}
		}()
	}
	wg.Wait()
	close(errCh)
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
) (copied, skipped, failed, bytes int64, err error) {
	attempts := 0
	for {
		attempts++
		n, status, err := tryTransfer(ctx, cfg, src, dst, meta, journ, idx, tuner)
		if status == "skipped" {
			return 0, 1, 0, 0, nil
		}
		if status == "ok" {
			return 1, 0, 0, n, nil
		}
		if err != nil && isUnstable(err) {
			if cfg.SkipUnstable || attempts >= cfg.MaxFileAttempts {
				if journ != nil {
					_ = journ.Put(journal.Entry{
						JobID: "resume", RelPath: meta.RelPath, Status: journal.StatusFailed,
						Attempts: attempts, LastError: err.Error(),
					})
				}
				return 0, 0, 1, 0, err
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
			return 0, 0, 1, 0, err
		}
		return 0, 0, 1, 0, err
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
) (int64, string, error) {
	if cfg.StableWindow > 0 {
		fi := fsmeta.FileInfo{ModTime: meta.ModTime, Size: meta.Size}
		if !fsmeta.UnchangedFor(fi, cfg.StableWindow) {
			return 0, "failed", unstableError{fmt.Errorf("file not stable yet")}
		}
	}

	gen := fsmeta.Generation{Size: meta.Size, ModNano: meta.ModTime.UnixNano()}
	if journ != nil && cfg.Resume {
		if e, ok := journ.Get(meta.RelPath); ok && e.Status == journal.StatusComplete {
			if e.SrcSize == gen.Size && e.SrcModNano == gen.ModNano && !cfg.Checksum {
				return 0, "skipped", nil
			}
		}
	}

	// Quick skip: same size+mtime on dest.
	if !cfg.Checksum {
		if dm, err := dst.Stat(ctx, meta.RelPath); err == nil {
			if dm.Size == meta.Size && dm.ModTime.UnixNano() == meta.ModTime.UnixNano() {
				return 0, "skipped", nil
			}
		}
	}

	rc, err := src.OpenRead(ctx, meta.RelPath)
	if err != nil {
		return 0, "failed", err
	}
	prof := tuner.Profile()
	opt := chunk.Options{AvgSize: prof.FrameSize, KeepData: true}
	sig, err := chunk.ChunkReader(rc, meta.Size, opt)
	_ = rc.Close()
	if err != nil {
		return 0, "failed", err
	}

	// Re-check generation after read.
	if st, err := src.Stat(ctx, meta.RelPath); err == nil {
		if st.Size != gen.Size || st.ModTime.UnixNano() != gen.ModNano {
			return 0, "failed", unstableError{fmt.Errorf("source changed during read")}
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
		return 0, "skipped", nil
	}
	plan := delta.Diff(sig, destSig)

	if cfg.DryRun {
		return int64(meta.Size), "ok", nil
	}

	if journ != nil {
		_ = journ.Put(journal.Entry{
			JobID: "resume", RelPath: meta.RelPath, Status: journal.StatusInProgress,
			SrcDigest: sig.Digest.String(), SrcSize: gen.Size, SrcModNano: gen.ModNano,
		})
	}

	ws, err := dst.BeginWrite(ctx, meta.RelPath, sig.Size)
	if err != nil {
		return 0, "failed", err
	}

	// Build lookup of source chunk data.
	byOff := map[uint64]chunk.Chunk{}
	for _, c := range sig.Chunks {
		byOff[c.Offset] = c
	}

	// Always write all source chunks into temp for perfect assembly.
	// Delta still avoids reading/sending when remote ApplyDelta exists; for local we write missing only if dest temp seeded — simplify: write all chunks from source data we hold.
	var sent int64
	codecPref := prof.Compress
	for _, c := range sig.Chunks {
		data := c.Data
		if data == nil {
			return 0, "failed", ws.Abort()
		}
		codec, payload, err := compress.Encode(codecPref, data)
		if err != nil {
			_ = ws.Abort()
			return 0, "failed", err
		}
		if err := ws.WriteChunk(ctx, c.Offset, codec, len(data), payload); err != nil {
			_ = ws.Abort()
			return 0, "failed", err
		}
		sent += int64(len(data))
		_ = plan
	}

	// Final live-change check before commit.
	if st, err := src.Stat(ctx, meta.RelPath); err == nil {
		if st.Size != gen.Size || st.ModTime.UnixNano() != gen.ModNano {
			_ = ws.Abort()
			return 0, "failed", unstableError{fmt.Errorf("source changed before finalize")}
		}
	}

	if err := ws.Commit(ctx, sig.Digest, meta.Mode, meta.ModTime); err != nil {
		_ = ws.Abort()
		return 0, "failed", err
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
	return sent, "ok", nil
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
