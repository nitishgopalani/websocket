package media

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"testing"
	"time"
)

func TestNoopAMDClassifier(t *testing.T) {
	clf := NoopAMDClassifier{}
	d, err := clf.Classify(context.Background(), []byte{0, 0}, 8000)
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if d.Result != AMDHuman {
		t.Fatalf("result = %v, want human", d.Result)
	}
}

func TestNewAMDClassifierDisabled(t *testing.T) {
	clf, err := NewAMDClassifier(DefaultAMDConfig())
	if err != nil {
		t.Fatalf("NewAMDClassifier: %v", err)
	}
	if !IsNoopAMD(clf) {
		t.Fatalf("expected noop, got %T", clf)
	}
}

func TestApplyAMDThreshold(t *testing.T) {
	d := applyAMDThreshold(AMDDecision{Result: AMDMachine, ProbaHuman: 0.5, Reason: "x"}, 0.4)
	if d.Result != AMDHuman {
		t.Fatalf("expected fail-open human, got %v", d.Result)
	}
	d = applyAMDThreshold(AMDDecision{Result: AMDMachine, ProbaHuman: 0.2, Reason: "vm"}, 0.4)
	if d.Result != AMDMachine {
		t.Fatalf("expected machine, got %v", d.Result)
	}
}

func TestRemoteAMDClassifierFakeWorker(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleFakeAMDConn(conn, `{"result":"machine","proba_human":0.2,"reason":"voicemail"}`)
		}
	}()

	clf, err := NewRemoteAMDClassifier(AMDConfig{
		Enabled:             true,
		Addr:                ln.Addr().String(),
		Timeout:             500 * time.Millisecond,
		ProbaHumanThreshold: 0.4,
	})
	if err != nil {
		t.Fatalf("NewRemoteAMDClassifier: %v", err)
	}

	decision, err := clf.Classify(context.Background(), make([]byte, 640), 8000)
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if decision.Result != AMDMachine {
		t.Fatalf("result = %v, want machine", decision.Result)
	}
}

func handleFakeAMDConn(conn net.Conn, responseJSON string) {
	defer conn.Close()
	hdr := make([]byte, denoiseHeaderSize)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return
	}
	bodyLen := binaryLittleEndianUint32(hdr)
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(conn, body); err != nil {
		return
	}
	payload := []byte(responseJSON)
	resp := make([]byte, denoiseHeaderSize+len(payload))
	putUint32LE(resp[0:4], uint32(len(payload)))
	copy(resp[4:], payload)
	_, _ = conn.Write(resp)
}

func putUint32LE(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}

func TestParseAMDResponse(t *testing.T) {
	d, err := parseAMDResponse([]byte(`{"result":"human","proba_human":0.9,"reason":"no match"}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if d.Result != AMDHuman {
		t.Fatalf("result = %v", d.Result)
	}
}

func TestAMDWireResponseRoundtrip(t *testing.T) {
	wire := amdWireResponse{Result: "machine", ProbaHuman: 0.1, Reason: "tone"}
	data, _ := json.Marshal(wire)
	d, err := parseAMDResponse(data)
	if err != nil || d.Result != AMDMachine {
		t.Fatalf("roundtrip failed: %+v err=%v", d, err)
	}
}

func TestPCMBytesForDuration(t *testing.T) {
	if got := pcmBytesForDurationMs(2000, 8000); got != 32000 {
		t.Fatalf("2s @ 8k = %d bytes, want 32000", got)
	}
}
