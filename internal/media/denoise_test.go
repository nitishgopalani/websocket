package media

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

func TestNoopDenoiserPassthrough(t *testing.T) {
	d := NoopDenoiser{}
	in := []byte{0x01, 0x00, 0x02, 0x00}
	out, err := d.Process(context.Background(), in, 8000)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if string(out) != string(in) {
		t.Fatalf("out = %v, want %v", out, in)
	}
}

func TestNewDenoiserDisabledReturnsNoop(t *testing.T) {
	d, err := NewDenoiser(DefaultDenoiseConfig())
	if err != nil {
		t.Fatalf("NewDenoiser: %v", err)
	}
	if _, ok := d.(NoopDenoiser); !ok {
		t.Fatalf("expected NoopDenoiser, got %T", d)
	}
}

func TestRemoteDenoiserWithFakeWorker(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go runFakeDenoiseWorker(t, ln, fakeWorkerModeFlip)

	d, err := NewRemoteDenoiser(DenoiseConfig{
		Enabled: true,
		Addr:    ln.Addr().String(),
		Timeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRemoteDenoiser: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	in := []byte{0x01, 0x00, 0x02, 0x00, 0x03, 0x00}
	out, err := d.Process(context.Background(), in, 8000)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("len(out)=%d, want %d", len(out), len(in))
	}
	for i := range in {
		want := in[i] ^ 0xFF
		if out[i] != want {
			t.Fatalf("byte[%d]=%#x, want %#x", i, out[i], want)
		}
	}
}

func TestRemoteDenoiserFailOpenOnWorkerError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go runFakeDenoiseWorker(t, ln, fakeWorkerModeError)

	d, err := NewRemoteDenoiser(DenoiseConfig{
		Enabled: true,
		Addr:    ln.Addr().String(),
		Timeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRemoteDenoiser: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	in := []byte{0x0A, 0x00, 0x0B, 0x00}
	_, err = d.Process(context.Background(), in, 8000)
	if err == nil {
		t.Fatal("expected worker error")
	}
	if d.Fallbacks() != 1 {
		t.Fatalf("fallbacks = %d, want 1", d.Fallbacks())
	}
}

func TestRemoteDenoiserFailOpenOnTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go runFakeDenoiseWorker(t, ln, fakeWorkerModeSlow)

	d, err := NewRemoteDenoiser(DenoiseConfig{
		Enabled: true,
		Addr:    ln.Addr().String(),
		Timeout: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRemoteDenoiser: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	in := []byte{0x01, 0x00}
	_, err = d.Process(context.Background(), in, 8000)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestEncodeDecodeDenoiseWireFormat(t *testing.T) {
	in := []byte{0x01, 0x00, 0x02, 0x00}
	req, err := encodeDenoiseRequest(in, 8000)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if binary.LittleEndian.Uint32(req[0:4]) != uint32(2+len(in)) {
		t.Fatalf("unexpected request length prefix")
	}
	if binary.LittleEndian.Uint16(req[4:6]) != 8000 {
		t.Fatalf("unexpected rate field")
	}

	resp := encodeDenoiseResponse(in)
	out, err := readDenoiseResponse(bytes.NewReader(resp))
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if string(out) != string(in) {
		t.Fatalf("roundtrip = %v, want %v", out, in)
	}
}

type fakeWorkerMode int

const (
	fakeWorkerModeFlip fakeWorkerMode = iota
	fakeWorkerModeError
	fakeWorkerModeSlow
)

func runFakeDenoiseWorker(t *testing.T, ln net.Listener, mode fakeWorkerMode) {
	t.Helper()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go handleFakeDenoiseConn(t, conn, mode)
	}
}

func handleFakeDenoiseConn(t *testing.T, conn net.Conn, mode fakeWorkerMode) {
	t.Helper()
	defer conn.Close()

	hdr := make([]byte, denoiseHeaderSize)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return
	}
	bodyLen := binary.LittleEndian.Uint32(hdr)
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(conn, body); err != nil {
		return
	}
	if len(body) < denoiseRateFieldSize {
		return
	}
	pcm := body[denoiseRateFieldSize:]

	switch mode {
	case fakeWorkerModeError:
		_, _ = conn.Write([]byte("bad"))
		return
	case fakeWorkerModeSlow:
		time.Sleep(500 * time.Millisecond)
	}

	out := make([]byte, len(pcm))
	copy(out, pcm)
	if mode == fakeWorkerModeFlip {
		for i := range out {
			out[i] ^= 0xFF
		}
	}
	_, _ = conn.Write(encodeDenoiseResponse(out))
}

func TestNewDenoiserEnabledRequiresEndpoint(t *testing.T) {
	_, err := NewDenoiser(DenoiseConfig{Enabled: true})
	if err == nil {
		t.Fatal("expected error when enabled without endpoint")
	}
}

var _ Denoiser = NoopDenoiser{}

func TestRemoteDenoiserConcurrentProcess(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go runFakeDenoiseWorker(t, ln, fakeWorkerModeFlip)

	d, err := NewRemoteDenoiser(DenoiseConfig{
		Enabled: true,
		Addr:    ln.Addr().String(),
		Timeout: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRemoteDenoiser: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	in := []byte{0x01, 0x00, 0x02, 0x00}
	for i := 0; i < 5; i++ {
		if _, err := d.Process(context.Background(), in, 8000); err != nil {
			t.Fatalf("Process[%d]: %v", i, err)
		}
	}
}
