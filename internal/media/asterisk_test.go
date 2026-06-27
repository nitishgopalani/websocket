package media_test

import (
	"encoding/json"
	"testing"

	"websocket/internal/media"
)

func TestParseAsteriskSessionStart(t *testing.T) {
	raw := []byte(`{
		"type":"session_start",
		"session_id":"sess-1",
		"client_id":"cli-1",
		"customer_phone":"+911234567890",
		"business_phone":"+918000000000",
		"audio":{"codec":"pcm16","input_sample_rate":16000,"output_sample_rate":24000,"channels":1},
		"metadata":{"language":"en-IN","agent_id":"agent-42"}
	}`)
	ctrl, err := media.ParseAsteriskControl(raw)
	if err != nil {
		t.Fatal(err)
	}
	if ctrl.Type != media.AsteriskMsgSessionStart || ctrl.Start == nil {
		t.Fatalf("type=%q start=%v", ctrl.Type, ctrl.Start)
	}
	start := media.AsteriskStartToStartEvent(*ctrl.Start)
	if start.StreamSID != "sess-1" {
		t.Fatalf("stream_sid=%q", start.StreamSID)
	}
	if start.MediaFormat.SampleRate != 16000 {
		t.Fatalf("sample_rate=%d", start.MediaFormat.SampleRate)
	}
	if start.CustomParameters["language"] != "en-IN" || start.CustomParameters["agent_id"] != "agent-42" {
		t.Fatalf("params=%v", start.CustomParameters)
	}
	if start.CustomParameters["asr_language"] != "en-IN" {
		t.Fatalf("asr_language=%q", start.CustomParameters["asr_language"])
	}
}

func TestAsteriskSerializerControlMessages(t *testing.T) {
	ser := media.AsteriskSerializer{}
	ready, err := ser.Ready()
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]string
	if err := json.Unmarshal(ready, &m); err != nil || m["type"] != media.AsteriskMsgReady {
		t.Fatalf("ready=%s err=%v", ready, err)
	}
	end, err := ser.EndOfCall()
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(end, &m); err != nil || m["type"] != media.AsteriskMsgEndOfCall {
		t.Fatalf("end=%s", end)
	}
}

func TestNewCarrierSerializerAsterisk(t *testing.T) {
	if _, ok := media.NewCarrierSerializer(media.CarrierConfig{Variant: "asterisk"}).(media.AsteriskSerializer); !ok {
		t.Fatal("expected AsteriskSerializer")
	}
}

func TestCarrierAsteriskProfile(t *testing.T) {
	p := media.CarrierConfig{Variant: media.CarrierAsterisk}.Profile()
	if !p.BinaryIngress || !p.BinaryEgress {
		t.Fatal("expected binary ingress/egress")
	}
	if p.InputSampleRate != 16000 || p.EgressSampleRate != 24000 {
		t.Fatalf("rates in=%d out=%d", p.InputSampleRate, p.EgressSampleRate)
	}
	if p.RequiresMarkEcho || p.BargeInFlushSupported {
		t.Fatal("asterisk should not use mark echo or carrier flush")
	}
}
