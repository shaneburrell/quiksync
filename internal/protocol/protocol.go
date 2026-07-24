package protocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
)

type MsgType uint8

const (
	MsgHello MsgType = iota + 1
	MsgHelloOK
	MsgWalk
	MsgWalkOK
	MsgStat
	MsgStatOK
	MsgOpenRead
	MsgReadData
	MsgBeginWrite
	MsgWriteChunk
	MsgCommit
	MsgAbort
	MsgGetSig
	MsgSigOK
	MsgRemove
	MsgMkdir
	MsgOK
	MsgErr
	MsgPeerStats
	MsgTuneOffer
	MsgTuneApply
	MsgBye
)

type Header struct {
	Type MsgType
	Len  uint32
}

func WriteMsg(w io.Writer, typ MsgType, payload []byte) error {
	var hdr [5]byte
	hdr[0] = byte(typ)
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := w.Write(payload)
	return err
}

func ReadMsg(r io.Reader) (MsgType, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	typ := MsgType(hdr[0])
	n := binary.BigEndian.Uint32(hdr[1:])
	if n > 64<<20 {
		return 0, nil, fmt.Errorf("message too large: %d", n)
	}
	buf := make([]byte, n)
	if n > 0 {
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, nil, err
		}
	}
	return typ, buf, nil
}

func WriteJSON(w io.Writer, typ MsgType, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return WriteMsg(w, typ, b)
}

func DecodeJSON(payload []byte, v any) error {
	return json.Unmarshal(payload, v)
}

type Hello struct {
	Version string `json:"version"`
	Root    string `json:"root,omitempty"`
}

type FileMeta struct {
	RelPath string    `json:"rel_path"`
	Size    int64     `json:"size"`
	ModNano int64     `json:"mod_nano"`
	Mode    uint32    `json:"mode"`
}

type WalkReq struct {
	Exclude []string `json:"exclude"`
}

type WalkOK struct {
	Files []FileMeta `json:"files"`
}

type PathReq struct {
	Rel string `json:"rel"`
}

type BeginWriteReq struct {
	Rel  string `json:"rel"`
	Size int64  `json:"size"`
}

type WriteChunkReq struct {
	Offset          uint64         `json:"offset"`
	Codec           compress.Codec `json:"codec"`
	UncompressedLen int            `json:"uncompressed_len"`
	Data            []byte         `json:"data"`
}

type CommitReq struct {
	Digest  chunk.Digest `json:"digest"`
	Mode    uint32       `json:"mode"`
	ModNano int64        `json:"mod_nano"`
}

type SigOK struct {
	Size   int64           `json:"size"`
	Digest chunk.Digest    `json:"digest"`
	Chunks []chunk.Chunk   `json:"chunks"`
}

type ErrMsg struct {
	Error string `json:"error"`
}

type PeerStats struct {
	BytesSent     int64   `json:"bytes_sent"`
	BytesAcked    int64   `json:"bytes_acked"`
	BytesVerified int64   `json:"bytes_verified"`
	RTTp50Ms      float64 `json:"rtt_p50_ms"`
	RTTp95Ms      float64 `json:"rtt_p95_ms"`
	Retries       int64   `json:"retries"`
	CompressRatio float64 `json:"compress_ratio"`
	CPUPercent    float64 `json:"cpu_percent"`
	ActiveStreams int     `json:"active_streams"`
	QueueDepth    int     `json:"queue_depth"`
	At            time.Time `json:"at"`
}

type TuneProfile struct {
	Streams  int            `json:"streams"`
	Window   int            `json:"window"`
	FrameSize uint32        `json:"frame_size"`
	Compress compress.Codec `json:"compress"`
}

type OK struct {
	OK bool `json:"ok"`
}
