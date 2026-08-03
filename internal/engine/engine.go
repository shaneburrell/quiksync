package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	"github.com/shaneburrell/quiksync/internal/progress"
	"github.com/shaneburrell/quiksync/internal/transfer"
	"github.com/shaneburrell/quiksync/internal/transport"
	"github.com/shaneburrell/quiksync/internal/transport/factory"
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
	Insecure        bool   // skip QUIC TOFU pin verification (labs only)
	AuthToken       string // QUIC daemon shared secret
	JobID           string // journal id; default "default"
	ConfigDir       string // for remote-dest journal/index (QUIKSYNC_CONFIG)
	S3Endpoint      string
	S3Region        string

	// LogFile enables tailable job event logging when non-empty.
	LogFile string
	// ProgressInterval is the stderr/file progress ticker period; 0 disables.
	ProgressInterval time.Duration
	// LogToStderr mirrors non-progress events to stderr (typically -v).
	LogToStderr bool

	// Test hooks (nil in production).
	TestAfterFile    func(rel, status string)
	TestBeforeCommit func(rel string)
}

type Stats struct {
	FilesCopied    int64
	FilesWouldCopy int64
	FilesSkipped   int64
	FilesFailed    int64
	FilesDeleted   int64
	BytesCopied    int64 // uncompressed verified payload
	BytesPayload   int64 // uncompressed bytes written for missing chunks
	BytesWired     int64 // on-wire compressed bytes when known
	ChunksReused   int64
	ChunksSent     int64
}

type fileResult struct {
	copied, wouldCopy, skipped, failed int64
	handled                            bool
	bytes, wired, reused, sent         int64
	elapsed                            time.Duration
	compressRatio                      float64
	attempts                           int
	err                                error
}

func Run(ctx context.Context, cfg Config) (Stats, error) {
	started := time.Now()
	if cfg.MaxFileAttempts <= 0 {
		cfg.MaxFileAttempts = 5
	}
	if cfg.JobID == "" {
		cfg.JobID = "default"
	}
	srcEP, err := transport.ParseEndpoint(cfg.Source)
	if err != nil {
		return Stats{}, err
	}
	dstEP, err := transport.ParseEndpoint(cfg.Dest)
	if err != nil {
		return Stats{}, err
	}

	var rep *progress.Reporter
	if cfg.LogFile != "" {
		rep, err = progress.Open(cfg.LogFile, progress.Options{
			AlsoLatest:       true,
			Stderr:           os.Stderr,
			MirrorActions:    cfg.LogToStderr || cfg.Verbose,
			ProgressToStderr: true,
		})
		if err != nil {
			return Stats{}, fmt.Errorf("open log: %w", err)
		}
		defer func() { _ = rep.Close() }()
	}

	openOpts := transport.OpenOptions{
		Insecure:   cfg.Insecure,
		AuthToken:  cfg.AuthToken,
		S3Endpoint: cfg.S3Endpoint,
		S3Region:   cfg.S3Region,
		StagingDir: "",
	}
	if cfg.ConfigDir != "" {
		openOpts.StagingDir = filepath.Join(cfg.ConfigDir, "s3-staging")
	}
	srcOpts := openOpts
	srcOpts.CreateRoot = false
	src, err := factory.Open(ctx, srcEP, srcOpts)
	if err != nil {
		return Stats{}, fmt.Errorf("source: %w", err)
	}
	defer func() { _ = src.Close() }()
	dstOpts := openOpts
	dstOpts.CreateRoot = true
	dst, err := factory.Open(ctx, dstEP, dstOpts)
	if err != nil {
		return Stats{}, fmt.Errorf("dest: %w", err)
	}
	defer func() { _ = dst.Close() }()

	var journ *journal.Journal
	var idx *index.Cache
	stateRoot := ""
	if dstEP.Scheme == "file" {
		stateRoot = dst.Root()
	} else if cfg.Resume {
		cfgDir := cfg.ConfigDir
		if cfgDir == "" {
			cfgDir = os.Getenv("QUIKSYNC_CONFIG")
		}
		if cfgDir == "" {
			if h, err := os.UserConfigDir(); err == nil {
				cfgDir = filepath.Join(h, "quiksync")
			} else {
				cfgDir = "."
			}
		}
		safe, err := journal.SanitizeJobID(cfg.JobID)
		if err != nil {
			return Stats{}, err
		}
		stateRoot = filepath.Join(cfgDir, "jobs", safe)
	}
	if stateRoot != "" {
		journ, err = journal.Open(stateRoot, cfg.JobID)
		if err != nil {
			return Stats{}, err
		}
		idx, err = index.Open(stateRoot)
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
	walkOK := true
	filesTotal := int64(len(srcFiles))

	sample := make([]byte, 0, 64*1024)
	for _, f := range srcFiles {
		if f.Size == 0 || f.Mode.IsDir() || f.Mode&os.ModeSymlink != 0 {
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

	mode := "copy"
	if cfg.SyncMode {
		mode = "sync"
	}
	workers := prof.Streams
	if workers < 1 {
		workers = 1
	}
	if workers > 32 {
		workers = 32
	}
	if !dst.Caps().SupportsMultiplex && workers > 1 {
		workers = 1
	}
	if rep != nil {
		rep.JobStart(cfg.JobID, cfg.Source, cfg.Dest, mode, workers)
		rep.Probe(prof.Streams, prof.Compress.String(), prof.FrameSize, prof.CDCAvg)
	}

	type job struct{ meta transport.FileMeta }
	jobs := make(chan job, len(srcFiles))
	for _, f := range srcFiles {
		jobs <- job{meta: f}
	}
	close(jobs)

	var stats Stats
	var wg sync.WaitGroup

	tickCtx, tickCancel := context.WithCancel(ctx)
	var tickWG sync.WaitGroup
	if rep != nil && cfg.ProgressInterval > 0 {
		tickWG.Add(1)
		go func() {
			defer tickWG.Done()
			progressTicker(tickCtx, rep, &stats, filesTotal, started, cfg.ProgressInterval)
		}()
	}
	defer func() {
		tickCancel()
		tickWG.Wait()
	}()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if err := ctx.Err(); err != nil {
					atomic.AddInt64(&stats.FilesSkipped, 1)
					if rep != nil {
						rep.FileSkip(j.meta.RelPath)
					}
					if cfg.TestAfterFile != nil {
						cfg.TestAfterFile(j.meta.RelPath, "skipped")
					}
					continue
				}
				res := transferOne(ctx, cfg, src, dst, j.meta, journ, idx, tuner, limiter)
				atomic.AddInt64(&stats.FilesCopied, res.copied)
				atomic.AddInt64(&stats.FilesWouldCopy, res.wouldCopy)
				atomic.AddInt64(&stats.FilesSkipped, res.skipped)
				atomic.AddInt64(&stats.FilesFailed, res.failed)
				atomic.AddInt64(&stats.BytesCopied, res.bytes)
				atomic.AddInt64(&stats.BytesPayload, res.bytes)
				atomic.AddInt64(&stats.BytesWired, res.wired)
				atomic.AddInt64(&stats.ChunksReused, res.reused)
				atomic.AddInt64(&stats.ChunksSent, res.sent)
				if rep != nil {
					switch {
					case res.skipped > 0 || res.wouldCopy > 0:
						rep.FileSkip(j.meta.RelPath)
					case res.copied > 0 || res.handled:
						rep.FileOK(j.meta.RelPath, res.bytes, res.wired, res.reused, res.sent, res.elapsed)
					default:
						rep.FileFail(j.meta.RelPath, res.err, res.attempts)
					}
				} else if res.err != nil && cfg.Verbose {
					fmt.Fprintf(os.Stderr, "warn %s: %v\n", j.meta.RelPath, res.err)
				}
				if cfg.TestAfterFile != nil {
					status := "failed"
					if res.skipped > 0 || res.wouldCopy > 0 {
						status = "skipped"
					} else if res.copied > 0 || res.handled {
						status = "ok"
					}
					cfg.TestAfterFile(j.meta.RelPath, status)
				}
				ratio := res.compressRatio
				if ratio <= 0 {
					ratio = 1
				}
				tuner.Observe(autotune.Sample{
					BytesVerified: res.bytes,
					BytesWired:    res.wired,
					Elapsed:       res.elapsed,
					ErrorRate:     float64(res.failed),
					CPUPercent:    0,
					CompressRatio: ratio,
					RTTMs:         0,
				})
			}
		}()
	}
	wg.Wait()
	tickCancel()
	tickWG.Wait()
	_ = tuner.Save()

	if cfg.SyncMode && cfg.Delete && !cfg.DryRun {
		if ctx.Err() == nil && stats.FilesFailed == 0 && walkOK {
			deleted, err := deleteExtras(ctx, srcFiles, dst, cfg.Exclude, rep)
			stats.FilesDeleted = deleted
			if err != nil {
				if rep != nil {
					rep.Warn("delete failed", progress.ErrField(err))
					rep.JobEnd(false, stats.FilesCopied, stats.FilesSkipped, stats.FilesFailed, stats.FilesDeleted, stats.BytesCopied, time.Since(started))
				}
				return stats, err
			}
			if rep != nil && deleted > 0 {
				rep.DeleteSummary(deleted)
			}
		} else if rep != nil {
			rep.Warn("skip --delete",
				progress.Int("failed", stats.FilesFailed),
				progress.Str("ctx", fmt.Sprint(ctx.Err())),
			)
		} else if cfg.Verbose {
			fmt.Fprintf(os.Stderr, "skip --delete: job not clean (failed=%d ctx=%v)\n", stats.FilesFailed, ctx.Err())
		}
	}

	ok := stats.FilesFailed == 0 && ctx.Err() == nil
	if rep != nil {
		rep.JobEnd(ok, stats.FilesCopied, stats.FilesSkipped, stats.FilesFailed, stats.FilesDeleted, stats.BytesCopied, time.Since(started))
	}
	if stats.FilesFailed > 0 {
		return stats, fmt.Errorf("%d file(s) failed", stats.FilesFailed)
	}
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	return stats, nil
}

func progressTicker(ctx context.Context, rep *progress.Reporter, stats *Stats, filesTotal int64, started time.Time, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	var lastBytes int64
	lastAt := started
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			copied := atomic.LoadInt64(&stats.FilesCopied)
			skipped := atomic.LoadInt64(&stats.FilesSkipped)
			failed := atomic.LoadInt64(&stats.FilesFailed)
			bytes := atomic.LoadInt64(&stats.BytesCopied)
			done := copied + skipped + failed
			elapsed := now.Sub(lastAt).Seconds()
			var rate int64
			if elapsed > 0 {
				rate = int64(float64(bytes-lastBytes) / elapsed)
			}
			lastBytes = bytes
			lastAt = now
			rep.Progress(done, filesTotal, copied, skipped, failed, bytes, rate)
		}
	}
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
		n, wired, reused, sent, status, elapsed, ratio, err := tryTransfer(ctx, cfg, src, dst, meta, journ, idx, tuner, limiter)
		if status == "skipped" {
			return fileResult{skipped: 1, elapsed: elapsed, attempts: attempts}
		}
		if status == "dry-run" {
			return fileResult{wouldCopy: 1, bytes: n, wired: wired, reused: reused, sent: sent, elapsed: elapsed, attempts: attempts}
		}
		if status == "ok" {
			if meta.Mode.IsDir() {
				return fileResult{handled: true, elapsed: elapsed, attempts: attempts}
			}
			return fileResult{copied: 1, bytes: n, wired: wired, reused: reused, sent: sent, elapsed: elapsed, compressRatio: ratio, attempts: attempts}
		}
		if err != nil && isUnstable(err) {
			if cfg.SkipUnstable || attempts >= cfg.MaxFileAttempts {
				if journ != nil {
					_ = journ.Put(journal.Entry{
						JobID: cfg.JobID, RelPath: meta.RelPath, Status: journal.StatusFailed,
						Attempts: attempts, LastError: err.Error(),
					})
				}
				return fileResult{failed: 1, err: err, elapsed: elapsed, attempts: attempts}
			}
			if err := sleepCtx(ctx, time.Duration(attempts)*200*time.Millisecond); err != nil {
				return fileResult{failed: 1, err: err, attempts: attempts}
			}
			if nm, e := src.Stat(ctx, meta.RelPath); e == nil {
				meta = nm
			}
			continue
		}
		if err != nil {
			if journ != nil {
				_ = journ.Put(journal.Entry{
					JobID: cfg.JobID, RelPath: meta.RelPath, Status: journal.StatusFailed,
					Attempts: attempts, LastError: err.Error(),
				})
			}
			return fileResult{failed: 1, err: err, elapsed: elapsed, attempts: attempts}
		}
		return fileResult{failed: 1, err: err, elapsed: elapsed, attempts: attempts}
	}
}

type unstableError struct{ error }

func isUnstable(err error) bool {
	var u unstableError
	return errors.As(err, &u)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
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
) (bytes, wired, reused, sent int64, status string, elapsed time.Duration, ratio float64, err error) {
	start := time.Now()
	defer func() { elapsed = time.Since(start) }()

	if meta.Mode.IsDir() {
		if cfg.DryRun {
			return 0, 0, 0, 0, "dry-run", 0, 0, nil
		}
		if err := dst.MkdirAll(ctx, meta.RelPath); err != nil {
			return 0, 0, 0, 0, "failed", 0, 0, err
		}
		if setter, ok := dst.(transport.ModeSetter); ok && meta.Mode != 0 {
			if err := setter.Chmod(ctx, meta.RelPath, meta.Mode); err != nil {
				return 0, 0, 0, 0, "failed", 0, 0, err
			}
		}
		return 0, 0, 0, 0, "ok", 0, 0, nil
	}
	if meta.Mode&os.ModeSymlink != 0 {
		if cfg.DryRun {
			return 0, 0, 0, 0, "dry-run", 0, 0, nil
		}
		reader, ok := src.(transport.Linker)
		if !ok {
			return 0, 0, 0, 0, "failed", 0, 0, fmt.Errorf("symlink not supported by source transport")
		}
		writer, ok := dst.(transport.Linker)
		if !ok {
			return 0, 0, 0, 0, "failed", 0, 0, fmt.Errorf("symlink not supported by destination transport")
		}
		target, err := reader.ReadLink(ctx, meta.RelPath)
		if err != nil {
			return 0, 0, 0, 0, "failed", 0, 0, err
		}
		if err := writer.Symlink(ctx, target, meta.RelPath); err != nil {
			return 0, 0, 0, 0, "failed", 0, 0, err
		}
		return 0, 0, 0, 0, "ok", 0, 0, nil
	}

	if cfg.StableWindow > 0 {
		fi := fsmeta.FileInfo{ModTime: meta.ModTime, Size: meta.Size}
		if !fsmeta.UnchangedFor(fi, cfg.StableWindow) {
			return 0, 0, 0, 0, "failed", 0, 0, unstableError{fmt.Errorf("file not stable yet")}
		}
	}

	gen := fsmeta.Generation{Size: meta.Size, ModNano: meta.ModTime.UnixNano()}
	forceTransfer := false
	if journ != nil && cfg.Resume {
		if e, ok := journ.Get(meta.RelPath); ok && e.Status == journal.StatusComplete {
			if e.SrcSize == gen.Size && e.SrcModNano == gen.ModNano && !cfg.Checksum {
				if dm, serr := dst.Stat(ctx, meta.RelPath); serr == nil &&
					dm.Size == e.SrcSize && dm.ModTime.UnixNano() == e.SrcModNano {
					if e.SrcDigest == "" {
						// Missing digest: do not trust journal skip.
						forceTransfer = true
						if idx != nil {
							_ = idx.Delete(meta.RelPath)
						}
					} else if match, herr := destDigestMatches(ctx, dst, meta.RelPath, e.SrcDigest); herr != nil {
						return 0, 0, 0, 0, "failed", 0, 0, herr
					} else if match {
						// Dest matches journal; also ensure source was not rewritten
						// in-place with the same size/mtime.
						srcMatch, serr := fileDigestMatches(ctx, src, meta.RelPath, e.SrcDigest)
						if serr != nil {
							return 0, 0, 0, 0, "failed", 0, 0, serr
						}
						if srcMatch {
							return 0, 0, 0, 0, "skipped", 0, 0, nil
						}
						forceTransfer = true
						if idx != nil {
							_ = idx.Delete(meta.RelPath)
						}
					} else {
						// Digest mismatch despite matching size/mtime — must re-copy.
						forceTransfer = true
						if idx != nil {
							_ = idx.Delete(meta.RelPath)
						}
					}
				}
			}
		}
	}

	if !cfg.Checksum && !forceTransfer {
		if dm, err := dst.Stat(ctx, meta.RelPath); err == nil {
			if dm.Size == meta.Size && dm.ModTime.UnixNano() == meta.ModTime.UnixNano() {
				return 0, 0, 0, 0, "skipped", 0, 0, nil
			}
		}
	}

	rc, err := src.OpenRead(ctx, meta.RelPath)
	if err != nil {
		return 0, 0, 0, 0, "failed", 0, 0, err
	}
	prof := tuner.Profile()
	cdcAvg := prof.CDCAvg
	if cdcAvg == 0 {
		cdcAvg = prof.FrameSize
	}
	// Pass 1: metadata-only signature (no whole-file RAM retention).
	opt := chunk.Options{AvgSize: cdcAvg, KeepData: false}
	sig, err := chunk.ChunkReader(rc, meta.Size, opt)
	closeErr := rc.Close()
	if err != nil {
		return 0, 0, 0, 0, "failed", 0, 0, err
	}
	if closeErr != nil {
		return 0, 0, 0, 0, "failed", 0, 0, closeErr
	}

	st, err := src.Stat(ctx, meta.RelPath)
	if err != nil {
		return 0, 0, 0, 0, "failed", 0, 0, unstableError{fmt.Errorf("source unavailable after read: %w", err)}
	}
	if st.Size != gen.Size || st.ModTime.UnixNano() != gen.ModNano {
		return 0, 0, 0, 0, "failed", 0, 0, unstableError{fmt.Errorf("source changed during read")}
	}

	var destSig chunk.FileSignature
	// Never trust a stale index when journal already proved dest content is wrong.
	// On cache hit, verify live dest content still matches the cached digest before
	// using chunk maps for skip/reuse (size/mtime alone is insufficient).
	if idx != nil && !cfg.Checksum && !forceTransfer {
		if dm, err := dst.Stat(ctx, meta.RelPath); err == nil {
			if cached, ok := idx.Get(meta.RelPath, dm.Size, dm.ModTime.UnixNano(), cdcAvg); ok {
				match, herr := fileDigestMatches(ctx, dst, meta.RelPath, cached.Digest.String())
				if herr != nil {
					return 0, 0, 0, 0, "failed", 0, 0, herr
				}
				if match {
					destSig = cached
				} else if derr := idx.Delete(meta.RelPath); derr != nil && cfg.Verbose {
					fmt.Fprintf(os.Stderr, "warn index delete %s: %v\n", meta.RelPath, derr)
				}
			}
		}
	}
	if len(destSig.Chunks) == 0 {
		var sigErr error
		destSig, sigErr = dst.GetSignature(ctx, meta.RelPath)
		if sigErr != nil {
			return 0, 0, 0, 0, "failed", 0, 0, sigErr
		}
	}

	if !forceTransfer && !delta.NeedsTransfer(sig, destSig, cfg.Checksum) && sig.Digest == destSig.Digest {
		return 0, 0, 0, 0, "skipped", 0, 0, nil
	}
	plan := delta.Diff(sig, destSig)
	missingSet := make(map[chunk.Digest]struct{}, len(plan.Missing))
	for _, c := range plan.Missing {
		missingSet[c.Digest] = struct{}{}
	}
	reuseByNew := make(map[uint64]delta.ReuseEntry, len(plan.Reuse))
	for _, r := range plan.Reuse {
		reuseByNew[r.NewOffset] = r
	}
	canReuse := dst.Caps().SupportsReuseChunk && len(destSig.Chunks) > 0
	fullWire := len(destSig.Chunks) == 0

	if cfg.DryRun {
		var payload int64
		for _, c := range plan.Missing {
			payload += int64(c.Length)
		}
		if payload == 0 && sig.Digest != destSig.Digest {
			payload = sig.Size
		}
		return payload, 0, int64(len(plan.Reuse)), int64(len(plan.Missing)), "dry-run", 0, 0, nil
	}

	if journ != nil {
		if err := journ.Put(journal.Entry{
			JobID: cfg.JobID, RelPath: meta.RelPath, Status: journal.StatusInProgress,
			SrcDigest: sig.Digest.String(), SrcSize: gen.Size, SrcModNano: gen.ModNano,
		}); err != nil {
			return 0, 0, 0, 0, "failed", 0, 0, fmt.Errorf("journal put: %w", err)
		}
	}

	ws, err := dst.BeginWrite(ctx, meta.RelPath, sig.Size)
	if err != nil {
		return 0, 0, 0, 0, "failed", 0, 0, err
	}
	committed := false
	defer func() {
		if !committed {
			if aerr := ws.Abort(); aerr != nil && cfg.Verbose {
				fmt.Fprintf(os.Stderr, "abort %s: %v\n", meta.RelPath, aerr)
			}
		}
	}()

	// Pass 2: re-read and stream chunks (payload held only for the current chunk).
	rc2, err := src.OpenRead(ctx, meta.RelPath)
	if err != nil {
		return 0, 0, 0, 0, "failed", 0, 0, err
	}
	var payload, wireBytes int64
	var chunksSent int64
	codecPref := prof.Compress
	sig2, err := chunk.StreamChunks(rc2, meta.Size, opt, func(c chunk.Chunk) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		data := c.Data
		if data == nil {
			return fmt.Errorf("missing chunk data")
		}
		_, isMissing := missingSet[c.Digest]
		if isMissing || fullWire {
			codec, enc, err := compress.Encode(codecPref, data)
			if err != nil {
				return err
			}
			if err := limiter.Wait(ctx, len(enc)); err != nil {
				return err
			}
			if err := ws.WriteChunk(ctx, c.Offset, codec, len(data), enc); err != nil {
				return err
			}
			payload += int64(len(data))
			wireBytes += int64(len(enc))
			chunksSent++
			return nil
		}
		re := reuseByNew[c.Offset]
		if canReuse {
			if err := limiter.Wait(ctx, re.Length); err != nil {
				return err
			}
			return ws.ReuseChunk(ctx, re.NewOffset, re.OldOffset, re.Digest, re.Length)
		}
		// Legacy full-wire path when dest cannot reuse.
		if err := limiter.Wait(ctx, len(data)); err != nil {
			return err
		}
		if err := ws.WriteChunk(ctx, c.Offset, compress.CodecNone, len(data), data); err != nil {
			return err
		}
		wireBytes += int64(len(data))
		return nil
	})
	closeErr = rc2.Close()
	if err != nil {
		return 0, 0, 0, 0, "failed", 0, 0, err
	}
	if closeErr != nil {
		return 0, 0, 0, 0, "failed", 0, 0, closeErr
	}
	if sig2.Digest != sig.Digest || sig2.Size != sig.Size {
		return 0, 0, 0, 0, "failed", 0, 0, unstableError{fmt.Errorf("source changed during transfer read")}
	}

	st, err = src.Stat(ctx, meta.RelPath)
	if err != nil {
		return 0, 0, 0, 0, "failed", 0, 0, unstableError{fmt.Errorf("source unavailable before finalize: %w", err)}
	}
	if st.Size != gen.Size || st.ModTime.UnixNano() != gen.ModNano {
		return 0, 0, 0, 0, "failed", 0, 0, unstableError{fmt.Errorf("source changed before finalize")}
	}

	if cfg.TestBeforeCommit != nil {
		cfg.TestBeforeCommit(meta.RelPath)
		st, err = src.Stat(ctx, meta.RelPath)
		if err != nil {
			return 0, 0, 0, 0, "failed", 0, 0, unstableError{fmt.Errorf("source unavailable before finalize: %w", err)}
		}
		if st.Size != gen.Size || st.ModTime.UnixNano() != gen.ModNano {
			return 0, 0, 0, 0, "failed", 0, 0, unstableError{fmt.Errorf("source changed before finalize")}
		}
	}

	if err := ws.Commit(ctx, sig.Digest, meta.Mode, meta.ModTime); err != nil {
		return 0, 0, 0, 0, "failed", 0, 0, err
	}
	committed = true

	if idx != nil {
		if err := idx.Put(meta.RelPath, sig.Size, gen.ModNano, cdcAvg, chunk.FileSignature{
			Size: sig.Size, Digest: sig.Digest, Chunks: stripData(sig.Chunks),
		}); err != nil && cfg.Verbose {
			fmt.Fprintf(os.Stderr, "warn index put %s: %v\n", meta.RelPath, err)
		}
	}
	if journ != nil {
		if err := journ.Put(journal.Entry{
			JobID: cfg.JobID, RelPath: meta.RelPath, Status: journal.StatusComplete,
			SrcDigest: sig.Digest.String(), SrcSize: gen.Size, SrcModNano: gen.ModNano,
			ChunksDone: len(sig.Chunks),
		}); err != nil {
			return 0, 0, 0, 0, "failed", 0, 0, fmt.Errorf("journal put: %w", err)
		}
	}
	compRatio := 1.0
	if wireBytes > 0 {
		compRatio = float64(payload) / float64(wireBytes)
	}
	return payload, wireBytes, int64(len(plan.Reuse)), chunksSent, "ok", 0, compRatio, nil
}

func stripData(in []chunk.Chunk) []chunk.Chunk {
	out := make([]chunk.Chunk, len(in))
	for i, c := range in {
		out[i] = chunk.Chunk{Offset: c.Offset, Length: c.Length, Digest: c.Digest}
	}
	return out
}

func deleteExtras(ctx context.Context, srcFiles []transport.FileMeta, dst transport.Transport, exclude []string, rep *progress.Reporter) (int64, error) {
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
		if err := ctx.Err(); err != nil {
			return n, err
		}
		if _, ok := keep[f.RelPath]; ok {
			continue
		}
		// rsync-like: excluded paths are protected from --delete
		if fsmeta.MatchExclude(f.RelPath, exclude) {
			continue
		}
		if f.Mode.IsDir() {
			continue
		}
		if err := dst.Remove(ctx, f.RelPath); err != nil {
			return n, fmt.Errorf("deleted %d then: %w", n, err)
		}
		if rep != nil {
			rep.Delete(f.RelPath)
		}
		n++
	}
	// Remove source-absent directories after files. Walk order is pre-order, so
	// reverse depth order to ensure only empty children are removed first.
	sort.Slice(dstFiles, func(i, j int) bool {
		return len(strings.Split(dstFiles[i].RelPath, "/")) > len(strings.Split(dstFiles[j].RelPath, "/"))
	})
	for _, f := range dstFiles {
		if !f.Mode.IsDir() {
			continue
		}
		if err := ctx.Err(); err != nil {
			return n, err
		}
		if _, ok := keep[f.RelPath]; ok || fsmeta.MatchExclude(f.RelPath, exclude) {
			continue
		}
		if err := dst.Remove(ctx, f.RelPath); err != nil {
			// It may still contain excluded or concurrently-created content.
			continue
		}
		if rep != nil {
			rep.Delete(f.RelPath)
		}
		n++
	}
	return n, nil
}

func destDigestMatches(ctx context.Context, dst transport.Transport, rel, want string) (bool, error) {
	return fileDigestMatches(ctx, dst, rel, want)
}

func fileDigestMatches(ctx context.Context, t transport.Transport, rel, want string) (bool, error) {
	rc, err := t.OpenRead(ctx, rel)
	if err != nil {
		return false, err
	}
	got, _, err := chunk.HashFile(rc)
	cerr := rc.Close()
	if err != nil {
		return false, err
	}
	if cerr != nil {
		return false, cerr
	}
	return got.String() == want, nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Verify compares digests of regular files.
func Verify(ctx context.Context, source, dest string) ([]string, error) {
	return VerifyWith(ctx, source, dest, transport.OpenOptions{})
}

// VerifyWith is Verify with transport open options (S3 endpoint/region, QUIC auth, …).
func VerifyWith(ctx context.Context, source, dest string, opts transport.OpenOptions) ([]string, error) {
	return VerifyFiltered(ctx, source, dest, opts, nil)
}

// VerifyFiltered is VerifyWith plus source-side exclude globs (same semantics as copy/sync --exclude).
func VerifyFiltered(ctx context.Context, source, dest string, opts transport.OpenOptions, exclude []string) ([]string, error) {
	srcEP, err := transport.ParseEndpoint(source)
	if err != nil {
		return nil, err
	}
	dstEP, err := transport.ParseEndpoint(dest)
	if err != nil {
		return nil, err
	}
	srcOpts := opts
	srcOpts.CreateRoot = false
	src, err := factory.Open(ctx, srcEP, srcOpts)
	if err != nil {
		return nil, err
	}
	defer func() { _ = src.Close() }()
	dstOpts := opts
	dstOpts.CreateRoot = false
	dst, err := factory.Open(ctx, dstEP, dstOpts)
	if err != nil {
		return nil, err
	}
	defer func() { _ = dst.Close() }()

	files, err := src.Walk(ctx, exclude)
	if err != nil {
		return nil, err
	}
	var mismatches []string
	for _, f := range files {
		if f.Mode.IsDir() {
			continue
		}
		sr, err := src.OpenRead(ctx, f.RelPath)
		if err != nil {
			mismatches = append(mismatches, f.RelPath+": source open: "+err.Error())
			continue
		}
		sd, _, err := chunk.HashFile(sr)
		cerr := sr.Close()
		if err != nil {
			mismatches = append(mismatches, f.RelPath+": source hash: "+err.Error())
			continue
		}
		if cerr != nil {
			mismatches = append(mismatches, f.RelPath+": source close: "+cerr.Error())
			continue
		}
		dr, err := dst.OpenRead(ctx, f.RelPath)
		if err != nil {
			mismatches = append(mismatches, f.RelPath+": missing on dest")
			continue
		}
		dd, _, err := chunk.HashFile(dr)
		cerr = dr.Close()
		if err != nil {
			mismatches = append(mismatches, f.RelPath+": dest hash: "+err.Error())
			continue
		}
		if cerr != nil {
			mismatches = append(mismatches, f.RelPath+": dest close: "+cerr.Error())
			continue
		}
		if sd != dd {
			mismatches = append(mismatches, fmt.Sprintf("%s: digest mismatch", f.RelPath))
		}
	}
	return mismatches, nil
}
