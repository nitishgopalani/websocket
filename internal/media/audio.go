package media

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

const (
	EncodingMuLaw = "audio/x-mulaw"
	EncodingL16   = "audio/x-l16"

	defaultTelephonySampleRate = 8000
)

var (
	ErrUnsupportedEncoding = errors.New("unsupported audio encoding")
	ErrInvalidPCM16Length  = errors.New("pcm16 frame length must be even")
)

// TargetFormat is the canonical PCM16 layout produced by CT-2 decoders.
type TargetFormat struct {
	SampleRate int // 8000 or 16000
	Channels   int // 1 = mono
}

// DefaultTargetFormat returns PCM16 mono at 16 kHz.
func DefaultTargetFormat() TargetFormat {
	return TargetFormat{SampleRate: 16000, Channels: 1}
}

// FrameSizeBytes returns the PCM16 byte size for a frame of frameDurationMs milliseconds.
func (t TargetFormat) FrameSizeBytes(frameDurationMs int) int {
	if frameDurationMs <= 0 {
		frameDurationMs = defaultFrameDurationMs
	}
	channels := t.Channels
	if channels <= 0 {
		channels = 1
	}
	samples := t.SampleRate * frameDurationMs / 1000
	return samples * 2 * channels
}

// Decoder transcodes one carrier frame into canonical PCM16 at the target rate.
type Decoder interface {
	Decode(frame []byte) ([]byte, error)
}

// MuLawToPCM16 decodes G.711 μ-law bytes into little-endian PCM16 samples.
func MuLawToPCM16(b []byte) []byte {
	out := make([]byte, len(b)*2)
	for i, u := range b {
		sample := mulawTable[u]
		binary.LittleEndian.PutUint16(out[i*2:], uint16(sample))
	}
	return out
}

// PCM16Identity returns a copy of already-linear PCM16 input.
func PCM16Identity(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// Resample8kTo16k upsamples PCM16 mono 8 kHz to 16 kHz via linear interpolation.
func Resample8kTo16k(pcm16 []byte) []byte {
	if len(pcm16) == 0 {
		return nil
	}
	if len(pcm16)%2 != 0 {
		return pcm16
	}
	numSamples := len(pcm16) / 2
	if numSamples == 1 {
		out := make([]byte, 4)
		copy(out, pcm16)
		copy(out[2:], pcm16)
		return out
	}

	out := make([]byte, numSamples*4)
	for i := 0; i < numSamples-1; i++ {
		s0 := int16(binary.LittleEndian.Uint16(pcm16[i*2:]))
		s1 := int16(binary.LittleEndian.Uint16(pcm16[(i+1)*2:]))
		mid := int16((int32(s0) + int32(s1)) / 2)
		binary.LittleEndian.PutUint16(out[i*4:], uint16(s0))
		binary.LittleEndian.PutUint16(out[i*4+2:], uint16(mid))
	}
	last := pcm16[(numSamples-1)*2 : numSamples*2]
	copy(out[(numSamples-1)*4:], last)
	copy(out[(numSamples-1)*4+2:], last)
	return out
}

// NewDecoder builds a carrier-to-canonical decoder from the session start format.
func NewDecoder(format AudioFormat, target TargetFormat) (Decoder, error) {
	if target.Channels <= 0 {
		target.Channels = 1
	}
	if target.SampleRate != 8000 && target.SampleRate != 16000 {
		return nil, fmt.Errorf("unsupported target sample rate: %d", target.SampleRate)
	}

	sourceRate := format.SampleRate
	if sourceRate <= 0 {
		sourceRate = defaultTelephonySampleRate
	}

	enc := normalizeEncoding(format.Encoding)
	switch enc {
	case EncodingMuLaw, "audio/pcmu", "pcmu", "mulaw":
		return &muLawDecoder{sourceRate: sourceRate, target: target}, nil
	case EncodingL16, "audio/l16", "audio/pcm", "slin", "linear16", "l16":
		return &l16Decoder{sourceRate: sourceRate, target: target}, nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedEncoding, format.Encoding)
	}
}

func normalizeEncoding(enc string) string {
	enc = strings.ToLower(strings.TrimSpace(enc))
	if idx := strings.Index(enc, ";"); idx >= 0 {
		enc = enc[:idx]
	}
	return enc
}

type muLawDecoder struct {
	sourceRate int
	target     TargetFormat
}

func (d *muLawDecoder) Decode(frame []byte) ([]byte, error) {
	pcm := MuLawToPCM16(frame)
	return resampleToTarget(pcm, d.sourceRate, d.target.SampleRate)
}

type l16Decoder struct {
	sourceRate int
	target     TargetFormat
}

func (d *l16Decoder) Decode(frame []byte) ([]byte, error) {
	if len(frame)%2 != 0 {
		return nil, ErrInvalidPCM16Length
	}
	pcm := PCM16Identity(frame)
	return resampleToTarget(pcm, d.sourceRate, d.target.SampleRate)
}

func resampleToTarget(pcm16 []byte, sourceRate, targetRate int) ([]byte, error) {
	switch {
	case sourceRate == targetRate:
		return pcm16, nil
	case sourceRate == 8000 && targetRate == 16000:
		return Resample8kTo16k(pcm16), nil
	default:
		return nil, fmt.Errorf("unsupported resample: %d Hz -> %d Hz", sourceRate, targetRate)
	}
}

// ITU-T G.711 μ-law decode table.
var mulawTable [256]int16

func init() {
	for i := range mulawTable {
		mulawTable[i] = decodeMuLawSample(byte(i))
	}
}

func decodeMuLawSample(u byte) int16 {
	u = ^u
	t := (int(u&0x0F) << 3) + 0x84
	t <<= (u & 0x70) >> 4
	if u&0x80 != 0 {
		return int16(0x84 - t)
	}
	return int16(t - 0x84)
}
