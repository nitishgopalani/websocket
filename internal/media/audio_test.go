package media_test

import (
	"encoding/binary"
	"testing"

	"websocket/internal/media"
)

func TestMuLawToPCM16KnownVectors(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected []int16
	}{
		{
			name:     "silence",
			input:    []byte{0xFF},
			expected: []int16{0},
		},
		{
			name:     "negative_maxish",
			input:    []byte{0x00},
			expected: []int16{-32124},
		},
		{
			name:     "positive_small",
			input:    []byte{0xFE},
			expected: []int16{8},
		},
		{
			name:     "multi_sample",
			input:    []byte{0xFF, 0x00, 0xFE},
			expected: []int16{0, -32124, 8},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := media.MuLawToPCM16(tc.input)
			if len(out) != len(tc.expected)*2 {
				t.Fatalf("len(out) = %d, want %d", len(out), len(tc.expected)*2)
			}
			for i, want := range tc.expected {
				got := int16(binary.LittleEndian.Uint16(out[i*2:]))
				if got != want {
					t.Fatalf("sample[%d] = %d, want %d", i, got, want)
				}
			}
		})
	}
}

func TestPCM16Identity(t *testing.T) {
	in := []byte{0x01, 0x02, 0x03, 0x04}
	out := media.PCM16Identity(in)
	if string(out) != string(in) {
		t.Fatalf("identity changed bytes: %v -> %v", in, out)
	}
	if &out[0] == &in[0] {
		t.Fatal("expected a copy, got same backing array")
	}
}

func TestResample8kTo16kDoublesSamples(t *testing.T) {
	in := make([]byte, 4) // two PCM16 samples
	binary.LittleEndian.PutUint16(in[0:2], 1000)
	binary.LittleEndian.PutUint16(in[2:4], 3000)

	out := media.Resample8kTo16k(in)
	if len(out) != 8 {
		t.Fatalf("len(out) = %d, want 8 (4 samples)", len(out))
	}

	s0 := int16(binary.LittleEndian.Uint16(out[0:2]))
	s1 := int16(binary.LittleEndian.Uint16(out[2:4]))
	s2 := int16(binary.LittleEndian.Uint16(out[4:6]))
	s3 := int16(binary.LittleEndian.Uint16(out[6:8]))

	if s0 != 1000 {
		t.Fatalf("first sample = %d, want 1000", s0)
	}
	if s1 != 2000 {
		t.Fatalf("interpolated sample = %d, want 2000", s1)
	}
	if s2 != 3000 {
		t.Fatalf("third sample = %d, want 3000", s2)
	}
	if s3 != 3000 {
		t.Fatalf("last duplicated sample = %d, want 3000", s3)
	}
}

func TestNewDecoderMuLawTo16k(t *testing.T) {
	dec, err := media.NewDecoder(media.AudioFormat{
		Encoding:   "audio/x-mulaw;rate=8000",
		SampleRate: 8000,
		Channels:   1,
	}, media.TargetFormat{SampleRate: 16000, Channels: 1})
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}

	out, err := dec.Decode([]byte{0xFF, 0xFF})
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(out) != 8 {
		t.Fatalf("len(out) = %d, want 8 bytes (4 samples at 16k)", len(out))
	}
}

func TestNewDecoderL16Passthrough8k(t *testing.T) {
	dec, err := media.NewDecoder(media.AudioFormat{
		Encoding:   "audio/x-l16",
		SampleRate: 8000,
		Channels:   1,
	}, media.TargetFormat{SampleRate: 8000, Channels: 1})
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}

	in := []byte{0x10, 0x00, 0x20, 0x00}
	out, err := dec.Decode(in)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if string(out) != string(in) {
		t.Fatalf("output = %v, want %v", out, in)
	}
}

func TestNewDecoderUnsupportedEncoding(t *testing.T) {
	_, err := media.NewDecoder(media.AudioFormat{
		Encoding:   "audio/opus",
		SampleRate: 48000,
	}, media.DefaultTargetFormat())
	if err == nil {
		t.Fatal("expected error for unsupported encoding")
	}
}

func TestTargetFormatFrameSizeBytes(t *testing.T) {
	if got := (media.TargetFormat{SampleRate: 16000, Channels: 1}).FrameSizeBytes(20); got != 640 {
		t.Fatalf("16k 20ms frame = %d bytes, want 640", got)
	}
	if got := (media.TargetFormat{SampleRate: 8000, Channels: 1}).FrameSizeBytes(20); got != 320 {
		t.Fatalf("8k 20ms frame = %d bytes, want 320", got)
	}
}
