package sim

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const defaultMuLawFrameBytes = 160 // 20 ms @ 8 kHz μ-law

// FrameSource yields fixed-size telephony μ-law frames for carrier simulation.
type FrameSource interface {
	NextFrame() ([]byte, error)
	Reset() error
}

// OpenFrameSource opens a file (.ulaw raw μ-law or .wav PCM16 mono 8 kHz) or returns a generator.
func OpenFrameSource(path string, frameBytes int) (FrameSource, error) {
	if frameBytes <= 0 {
		frameBytes = defaultMuLawFrameBytes
	}
	if path == "" {
		return NewSilenceGenerator(frameBytes, 50), nil
	}
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".ulaw", ".ul":
		return openULawFile(path, frameBytes)
	case ".wav":
		return openWAVFile(path, frameBytes)
	default:
		return nil, fmt.Errorf("unsupported input format %q (use .ulaw or .wav)", ext)
	}
}

// GeneratorKind selects synthetic frame patterns.
type GeneratorKind int

const (
	GeneratorSilence GeneratorKind = iota
	GeneratorTone
)

// GeneratorSource emits repeating synthetic μ-law frames.
type GeneratorSource struct {
	frameBytes int
	kind       GeneratorKind
	framesLeft int
	idx        int
}

// NewSilenceGenerator returns μ-law silence frames (0xFF).
func NewSilenceGenerator(frameBytes, frameCount int) *GeneratorSource {
	return &GeneratorSource{frameBytes: frameBytes, kind: GeneratorSilence, framesLeft: frameCount}
}

// NewToneGenerator returns a simple alternating-tone μ-law pattern.
func NewToneGenerator(frameBytes, frameCount int) *GeneratorSource {
	return &GeneratorSource{frameBytes: frameBytes, kind: GeneratorTone, framesLeft: frameCount}
}

func (g *GeneratorSource) NextFrame() ([]byte, error) {
	if g.framesLeft <= 0 {
		return nil, io.EOF
	}
	g.framesLeft--
	frame := make([]byte, g.frameBytes)
	switch g.kind {
	case GeneratorTone:
		v := byte(0x80)
		if g.idx%2 == 0 {
			v = 0x20
		}
		for i := range frame {
			frame[i] = v
		}
	default:
		for i := range frame {
			frame[i] = 0xFF
		}
	}
	g.idx++
	return frame, nil
}

func (g *GeneratorSource) Reset() error {
	g.idx = 0
	return nil
}

type ulawFileSource struct {
	data       []byte
	frameBytes int
	offset     int
}

func openULawFile(path string, frameBytes int) (*ulawFileSource, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return &ulawFileSource{data: data, frameBytes: frameBytes}, nil
}

func (s *ulawFileSource) NextFrame() ([]byte, error) {
	if s.offset >= len(s.data) {
		return nil, io.EOF
	}
	end := s.offset + s.frameBytes
	if end > len(s.data) {
		end = len(s.data)
	}
	frame := make([]byte, s.frameBytes)
	copy(frame, s.data[s.offset:end])
	s.offset = end
	return frame, nil
}

func (s *ulawFileSource) Reset() error {
	s.offset = 0
	return nil
}

type wavFileSource struct {
	pcm        []byte
	frameBytes int
	offset     int
}

func openWAVFile(path string, frameBytes int) (*wavFileSource, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pcm, err := extractWAVPCM16(data)
	if err != nil {
		return nil, err
	}
	return &wavFileSource{pcm: pcm, frameBytes: frameBytes * 2}, nil // PCM16 is 2x μ-law frame size at 8k
}

func (s *wavFileSource) NextFrame() ([]byte, error) {
	if s.offset >= len(s.pcm) {
		return nil, io.EOF
	}
	end := s.offset + s.frameBytes
	if end > len(s.pcm) {
		// pad last partial frame with silence PCM
		chunk := make([]byte, s.frameBytes)
		copy(chunk, s.pcm[s.offset:])
		s.offset = len(s.pcm)
		return pcm16ToMuLaw(chunk), nil
	}
	frame := pcm16ToMuLaw(s.pcm[s.offset:end])
	s.offset = end
	return frame, nil
}

func (s *wavFileSource) Reset() error {
	s.offset = 0
	return nil
}

func extractWAVPCM16(data []byte) ([]byte, error) {
	if len(data) < 44 || string(data[0:4]) != "RIFF" || string(data[8:12]) != "WAVE" {
		return nil, fmt.Errorf("invalid wav header")
	}
	offset := 12
	var sampleRate uint32
	var bitsPerSample uint16
	var channels uint16
	var pcm []byte
	for offset+8 <= len(data) {
		chunkID := string(data[offset : offset+4])
		chunkSize := binary.LittleEndian.Uint32(data[offset+4 : offset+8])
		offset += 8
		if offset+int(chunkSize) > len(data) {
			break
		}
		switch chunkID {
		case "fmt ":
			if chunkSize < 16 {
				return nil, fmt.Errorf("wav fmt chunk too short")
			}
			channels = binary.LittleEndian.Uint16(data[offset+2 : offset+4])
			sampleRate = binary.LittleEndian.Uint32(data[offset+4 : offset+8])
			bitsPerSample = binary.LittleEndian.Uint16(data[offset+14 : offset+16])
		case "data":
			pcm = append([]byte(nil), data[offset:offset+int(chunkSize)]...)
		}
		offset += int(chunkSize)
	}
	if len(pcm) == 0 {
		return nil, fmt.Errorf("wav missing data chunk")
	}
	if channels != 1 {
		return nil, fmt.Errorf("wav must be mono (got %d channels)", channels)
	}
	if bitsPerSample != 16 {
		return nil, fmt.Errorf("wav must be 16-bit PCM (got %d)", bitsPerSample)
	}
	if sampleRate != 8000 {
		return nil, fmt.Errorf("wav must be 8 kHz (got %d)", sampleRate)
	}
	return pcm, nil
}

func pcm16ToMuLaw(pcm []byte) []byte {
	out := make([]byte, len(pcm)/2)
	for i := 0; i+1 < len(pcm); i += 2 {
		sample := int16(binary.LittleEndian.Uint16(pcm[i : i+2]))
		out[i/2] = linearToMuLaw(sample)
	}
	return out
}

func linearToMuLaw(sample int16) byte {
	const muLawMax = 0x1FFF
	sign := byte(0)
	if sample < 0 {
		sign = 0x80
		sample = -sample
	}
	if sample > muLawMax {
		sample = muLawMax
	}
	sample = sample + 132
	exp := 7
	for exp > 0 && sample < (1<<(exp+3)) {
		exp--
	}
	mantissa := (sample >> (exp + 3)) & 0x0F
	return ^(sign | byte(exp<<4) | byte(mantissa))
}
