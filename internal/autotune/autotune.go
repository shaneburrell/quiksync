package autotune

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/shaneburrell/quiksync/internal/compress"
)

// Config is CLI-facing tuner configuration.
type Config struct {
	Enabled     bool
	Streams     int // 0 = auto
	Compress    compress.Codec
	ChunkAvg    uint32 // 0 = auto; pins CDC for the job
	ProfilePath string
}

// Profile is the live transfer profile.
type Profile struct {
	Streams   int            `json:"streams"`
	Window    int            `json:"window"`
	FrameSize uint32         `json:"frame_size"` // wire frame hint
	CDCAvg    uint32         `json:"cdc_avg"`    // pinned CDC avg for the job
	Compress  compress.Codec `json:"compress"`
	Goodput   float64        `json:"goodput"`
	Auto      bool           `json:"auto"`
}

// Sample is a telemetry interval.
type Sample struct {
	BytesVerified int64
	BytesWired    int64
	Elapsed       time.Duration
	RTTMs         float64
	ErrorRate     float64
	CPUPercent    float64
	CompressRatio float64
}

// Tuner performs probe + hill-climb optimization.
type Tuner struct {
	mu       sync.Mutex
	cfg      Config
	profile  Profile
	prev     Profile
	bytes    int64
	wired    int64
	started  time.Time
	lastTune time.Time
	step     int
	hostKey  string
	cdcPin   uint32 // fixed for job lifetime
}

func New(cfg Config, hostKey string) *Tuner {
	p := Profile{
		Streams:   4,
		Window:    16,
		FrameSize: 64 * 1024,
		CDCAvg:    64 * 1024,
		Compress:  compress.CodecNone,
		Auto:      cfg.Enabled,
	}
	if cfg.Streams > 0 {
		p.Streams = cfg.Streams
	}
	if cfg.ChunkAvg > 0 {
		p.FrameSize = cfg.ChunkAvg
		p.CDCAvg = cfg.ChunkAvg
	}
	if cfg.Compress != compress.CodecAuto {
		p.Compress = cfg.Compress
	}
	t := &Tuner{
		cfg:      cfg,
		profile:  p,
		started:  time.Now(),
		lastTune: time.Now(),
		hostKey:  hostKey,
		cdcPin:   p.CDCAvg,
	}
	if path := t.profilePath(); path != "" {
		if loaded, err := LoadProfile(path); err == nil {
			if cfg.Streams == 0 {
				p.Streams = loaded.Streams
			}
			if cfg.ChunkAvg == 0 {
				if loaded.CDCAvg > 0 {
					p.CDCAvg = loaded.CDCAvg
				} else if loaded.FrameSize > 0 {
					p.CDCAvg = loaded.FrameSize
				}
				if loaded.FrameSize > 0 {
					p.FrameSize = loaded.FrameSize
				}
			}
			if cfg.Compress == compress.CodecAuto {
				p.Compress = loaded.Compress
			}
			p.Window = loaded.Window
			t.profile = p
			t.cdcPin = p.CDCAvg
		}
	}
	return t
}

func (t *Tuner) Profile() Profile {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.profile
}

func (t *Tuner) profilePath() string {
	if t.cfg.ProfilePath != "" {
		return t.cfg.ProfilePath
	}
	if t.hostKey == "" {
		return ""
	}
	return filepath.Join(".quiksync", "profiles", sanitize(t.hostKey)+".json")
}

func sanitize(s string) string {
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' {
			b = append(b, c)
		} else {
			b = append(b, '_')
		}
	}
	return string(b)
}

// Probe picks an initial profile from a data sample and RTT estimate.
func (t *Tuner) Probe(sample []byte, rttMs float64) Profile {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.cfg.Enabled {
		t.profile.CDCAvg = t.cdcPin
		return t.profile
	}

	streamsCandidates := []int{1, 2, 4, 8}
	if t.cfg.Streams > 0 {
		streamsCandidates = []int{t.cfg.Streams}
	} else if rttMs > 80 {
		streamsCandidates = []int{4, 8, 16}
	} else if rttMs < 5 {
		streamsCandidates = []int{1, 2, 4}
	}

	frameCandidates := []uint32{16 * 1024, 64 * 1024, 256 * 1024}
	if t.cfg.ChunkAvg > 0 {
		// Pin CDC; still evaluate wire frame sizes separately from CDC.
		frameCandidates = []uint32{16 * 1024, 64 * 1024, 256 * 1024}
	} else if rttMs > 100 {
		frameCandidates = []uint32{16 * 1024, 64 * 1024}
	}

	codecCandidates := []compress.Codec{compress.CodecNone}
	if t.cfg.Compress == compress.CodecAuto {
		lz4r := compress.SampleRatio(compress.CodecLZ4, sample)
		zstr := compress.SampleRatio(compress.CodecZstd, sample)
		if lz4r >= 1.08 {
			codecCandidates = append(codecCandidates, compress.CodecLZ4)
		}
		if zstr >= 1.20 {
			codecCandidates = append(codecCandidates, compress.CodecZstd)
		}
	} else {
		codecCandidates = []compress.Codec{t.cfg.Compress}
	}

	best := t.profile
	bestScore := -1.0
	for _, s := range streamsCandidates {
		for _, f := range frameCandidates {
			for _, c := range codecCandidates {
				ratio := 1.0
				if c != compress.CodecNone {
					ratio = compress.SampleRatio(c, sample)
				}
				cpuCost := 1.0
				switch c {
				case compress.CodecZstd:
					cpuCost = 1.25
				case compress.CodecLZ4:
					cpuCost = 1.05
				}
				bdpFactor := 1.0 + math.Min(rttMs/50.0, 3.0)*float64(s)/4.0
				frameFactor := float64(f) / (64 * 1024)
				if rttMs > 80 && f > 64*1024 {
					frameFactor *= 0.7
				}
				score := bdpFactor * frameFactor * ratio / cpuCost
				if score > bestScore {
					bestScore = score
					cdc := t.cdcPin
					if t.cfg.ChunkAvg > 0 {
						cdc = t.cfg.ChunkAvg
					} else if cdc == 0 {
						cdc = f
					}
					best = Profile{
						Streams:   s,
						Window:    max(8, s*4),
						FrameSize: f,
						CDCAvg:    cdc,
						Compress:  c,
						Auto:      true,
						Goodput:   score,
					}
				}
			}
		}
	}
	if t.cfg.ChunkAvg > 0 {
		best.CDCAvg = t.cfg.ChunkAvg
	} else if best.CDCAvg == 0 {
		best.CDCAvg = best.FrameSize
	}
	t.cdcPin = best.CDCAvg
	t.profile = best
	t.prev = best
	return best
}

// Observe updates rolling goodput and optionally nudges knobs.
// CDCAvg is never changed mid-job.
func (t *Tuner) Observe(s Sample) Profile {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.bytes += s.BytesVerified
	t.wired += s.BytesWired
	elapsed := time.Since(t.started).Seconds()
	if elapsed > 0 {
		t.profile.Goodput = float64(t.bytes) / elapsed
	}
	t.profile.CDCAvg = t.cdcPin
	if !t.cfg.Enabled {
		return t.profile
	}
	if s.ErrorRate > 0.05 {
		t.profile.Streams = max(1, t.profile.Streams/2)
		t.profile.Window = max(4, t.profile.Window/2)
		if t.profile.FrameSize > 16*1024 {
			t.profile.FrameSize /= 2
		}
		if t.profile.Compress == compress.CodecZstd {
			t.profile.Compress = compress.CodecLZ4
		}
		t.profile.CDCAvg = t.cdcPin
		t.lastTune = time.Now()
		return t.profile
	}
	if time.Since(t.lastTune) < 2*time.Second {
		return t.profile
	}
	t.lastTune = time.Now()
	t.step = (t.step + 1) % 3
	candidate := t.profile
	switch t.step {
	case 0:
		if t.cfg.Streams == 0 && s.CPUPercent < 85 && s.ErrorRate < 0.01 {
			candidate.Streams = min(32, candidate.Streams+1)
			candidate.Window = candidate.Streams * 4
		}
	case 1:
		// Tune wire frame only; never mutate CDCAvg.
		if s.RTTMs < 20 && candidate.FrameSize < 256*1024 {
			candidate.FrameSize *= 2
		} else if s.RTTMs > 100 && candidate.FrameSize > 16*1024 {
			candidate.FrameSize /= 2
		}
	case 2:
		if t.cfg.Compress == compress.CodecAuto && s.CompressRatio >= 1.1 && s.CPUPercent < 80 {
			if candidate.Compress == compress.CodecNone {
				candidate.Compress = compress.CodecLZ4
			} else if candidate.Compress == compress.CodecLZ4 && s.CompressRatio >= 1.3 {
				candidate.Compress = compress.CodecZstd
			}
		}
	}
	candidate.CDCAvg = t.cdcPin
	inst := 0.0
	if s.Elapsed > 0 {
		inst = float64(s.BytesVerified) / s.Elapsed.Seconds()
	}
	if inst > 0 && t.profile.Goodput > 0 && inst+1024 < t.profile.Goodput*0.92 {
		t.profile = t.prev
		t.profile.CDCAvg = t.cdcPin
		return t.profile
	}
	t.prev = t.profile
	t.profile = candidate
	return t.profile
}

func (t *Tuner) Save() error {
	path := t.profilePath()
	if path == "" {
		return nil
	}
	t.mu.Lock()
	p := t.profile
	t.mu.Unlock()
	return SaveProfile(path, p)
}

func SaveProfile(path string, p Profile) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func LoadProfile(path string) (Profile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Profile{}, err
	}
	var p Profile
	err = json.Unmarshal(b, &p)
	return p, err
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
