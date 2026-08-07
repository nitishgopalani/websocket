package media

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultSarvamTTSBaseURL = "https://api.sarvam.ai"
	defaultSarvamTTSModel   = "bulbul:v2"
	defaultSarvamTTSSpeaker = "abhilash" // deep, authoritative male Hindi voice (bulbul:v2)
	defaultSarvamTTSLang    = "hi-IN"
	sarvamTTSMaxChars       = 1400 // per-request text cap; replies are short, split if longer
	sarvamTTSEmitChunkBytes = 4096 // ~128ms of PCM16@16k per emitted chunk
)

// sarvamTTSConfigFromEnv populates a Sarvam-flavoured TTSConfig. APIKey reuses the
// same SARVAM_API_KEY already used for ASR; speaker/model/language are overridable.
func sarvamTTSConfigFromEnv(cfg TTSConfig) TTSConfig {
	cfg.Provider = "sarvam"
	cfg.APIKey = strings.TrimSpace(os.Getenv("SARVAM_API_KEY"))

	cfg.VoiceID = defaultSarvamTTSSpeaker
	if v := strings.TrimSpace(os.Getenv("SARVAM_TTS_SPEAKER")); v != "" {
		cfg.VoiceID = v
	}
	cfg.Model = defaultSarvamTTSModel
	if v := strings.TrimSpace(os.Getenv("SARVAM_TTS_MODEL")); v != "" {
		cfg.Model = v
	}
	cfg.Language = defaultSarvamTTSLang
	if v := strings.TrimSpace(os.Getenv("SARVAM_TTS_LANGUAGE")); v != "" {
		cfg.Language = v
	}
	cfg.BaseURL = defaultSarvamTTSBaseURL
	if v := strings.TrimSpace(os.Getenv("SARVAM_TTS_BASE_URL")); v != "" {
		cfg.BaseURL = v
	}
	if v := os.Getenv("TTS_OUTPUT_FORMAT"); v != "" {
		cfg.OutputFormat = v
	}
	return cfg
}

// SarvamTTSProvider synthesizes speech via Sarvam's REST text-to-speech endpoint.
// Unlike ElevenLabs (WebSocket streaming) this is request/response: each Speak call
// POSTs the full utterance and emits the returned PCM16 audio as chunks.
type SarvamTTSProvider struct {
	apiKey  string
	baseURL string
	model   string
	speaker string
	lang    string
	client  *http.Client
	logger  *slog.Logger
}

// NewSarvamTTSProvider constructs a Sarvam REST TTS provider.
func NewSarvamTTSProvider(cfg TTSConfig) (*SarvamTTSProvider, error) {
	cfg = cfg.withDefaults()
	if cfg.APIKey == "" {
		return nil, ErrTTSNotConfigured
	}
	base := strings.TrimRight(cfg.BaseURL, "/")
	if base == "" || strings.Contains(base, "elevenlabs") {
		base = defaultSarvamTTSBaseURL
	}
	model := cfg.Model
	if model == "" || strings.HasPrefix(model, "eleven") {
		model = defaultSarvamTTSModel
	}
	speaker := cfg.VoiceID
	if speaker == "" || speaker == defaultElevenLabsVoice {
		speaker = defaultSarvamTTSSpeaker
	}
	lang := cfg.Language
	if lang == "" {
		lang = defaultSarvamTTSLang
	} else if !strings.Contains(lang, "-") {
		lang = lang + "-IN" // normalise "hi" -> "hi-IN"
	}
	return &SarvamTTSProvider{
		apiKey:  cfg.APIKey,
		baseURL: base,
		model:   model,
		speaker: speaker,
		lang:    lang,
		client:  &http.Client{Timeout: 30 * time.Second},
		logger:  slog.Default(),
	}, nil
}

func (p *SarvamTTSProvider) Open(_ context.Context, meta TTSSessionMeta) (TTSStream, error) {
	rate := SampleRateFromPCMFormat(meta.OutputFormat)
	sampleRate := sarvamSupportedRate(rate)
	s := &sarvamTTSStream{
		provider:   p,
		meta:       meta,
		sampleRate: sampleRate,
		audio:      make(chan TTSAudioChunk, defaultTTSAudioBuffer),
		done:       make(chan struct{}),
		cancelled:  make(map[string]struct{}),
		turnSeq:    make(map[string]int),
		turnVoice:  make(map[string]sarvamTurnVoice),
	}
	p.logger.Info("sarvam tts session opened",
		"stream_sid", meta.StreamSID,
		"speaker", p.speaker,
		"model", p.model,
		"language", p.lang,
		"sample_rate", sampleRate,
	)
	return s, nil
}

// sarvamSupportedRate snaps a desired Hz to Sarvam's supported set {8000,16000,22050,24000}.
func sarvamSupportedRate(hz int) int {
	switch {
	case hz <= 8000:
		return 8000
	case hz <= 16000:
		return 16000
	case hz <= 22050:
		return 22050
	default:
		return 24000
	}
}

type sarvamTurnVoice struct {
	speaker string
	model   string
	pace    *float64
}

type sarvamTTSStream struct {
	provider   *SarvamTTSProvider
	meta       TTSSessionMeta
	sampleRate int

	mu         sync.Mutex
	closed     bool
	cancelled  map[string]struct{}
	turnSeq    map[string]int
	turnVoice  map[string]sarvamTurnVoice

	audio chan TTSAudioChunk
	done  chan struct{}
	wg    sync.WaitGroup

	fallbacks atomic.Int64
}

// SetTurnVoice records optional per-turn speaker/model/pace overrides for Speak.
func (s *sarvamTTSStream) SetTurnVoice(turnID, voiceID, model string, pace *float64) {
	if turnID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.turnVoice == nil {
		s.turnVoice = make(map[string]sarvamTurnVoice)
	}
	cur := s.turnVoice[turnID]
	if v := strings.TrimSpace(voiceID); v != "" {
		cur.speaker = v
	}
	if m := strings.TrimSpace(model); m != "" {
		cur.model = m
	}
	if pace != nil {
		p := *pace
		cur.pace = &p
	}
	s.turnVoice[turnID] = cur
}

func (s *sarvamTTSStream) resolveVoice(turnID string) (speaker, model string, pace *float64) {
	speaker = s.provider.speaker
	model = s.provider.model
	s.mu.Lock()
	defer s.mu.Unlock()
	if ov, ok := s.turnVoice[turnID]; ok {
		if ov.speaker != "" {
			speaker = ov.speaker
		}
		if ov.model != "" {
			model = ov.model
		}
		if ov.pace != nil {
			p := *ov.pace
			pace = &p
		}
	}
	return speaker, model, pace
}

func (s *sarvamTTSStream) Speak(turnID string, text string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrTTSStreamClosed
	}
	if _, cancelled := s.cancelled[turnID]; cancelled {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	if strings.TrimSpace(text) == "" {
		s.emitFinal(turnID)
		return nil
	}

	speaker, model, pace := s.resolveVoice(turnID)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.synthesize(turnID, text, speaker, model, pace)
	}()
	return nil
}

func (s *sarvamTTSStream) synthesize(turnID, text, speaker, model string, pace *float64) {
	for _, part := range splitForSarvam(text, sarvamTTSMaxChars) {
		if s.isCancelled(turnID) {
			return
		}
		pcm, err := s.requestPCM(part, speaker, model, pace)
		if err != nil {
			s.provider.logger.Warn("sarvam tts request failed",
				"stream_sid", s.meta.StreamSID,
				"speaker", speaker,
				"model", model,
				"error", err)
			s.emitFailSoft(turnID)
			return
		}
		if !s.emitPCM(turnID, pcm) {
			return
		}
	}
	s.emitFinal(turnID)
}

// SarvamTTSRequest is the exact JSON body posted to Sarvam REST /text-to-speech.
// Always sent: text, target_language_code, speaker, model, speech_sample_rate,
// enable_preprocessing. Optional: pace (omitempty). Pitch / loudness are never
// sent — bulbul:v3 returns HTTP 400 if they are present.
type SarvamTTSRequest struct {
	Text                string   `json:"text"`
	TargetLanguageCode  string   `json:"target_language_code"`
	Speaker             string   `json:"speaker"`
	Model               string   `json:"model"`
	SpeechSampleRate    int      `json:"speech_sample_rate"`
	EnablePreprocessing bool     `json:"enable_preprocessing"`
	Pace                *float64 `json:"pace,omitempty"`
}

// SarvamTTSBuildOpts are optional knobs for BuildSarvamTTSRequest.
// Pitch/Loudness may be set by callers migrating from v2 docs but are always
// dropped — bulbul:v3 returns HTTP 400 if either key is present in the JSON.
type SarvamTTSBuildOpts struct {
	Pace     *float64
	Pitch    *float64
	Loudness *float64
}

// BuildSarvamTTSRequest constructs the REST body used by requestPCM (D-4 probes).
// Optional opts: Pace is included when set (clamped to [0.5, 2.0] on bulbul:v3).
// Pitch and Loudness are accepted on the opts struct but never serialized.
func BuildSarvamTTSRequest(text, lang, speaker, model string, sampleRate int, opts ...SarvamTTSBuildOpts) SarvamTTSRequest {
	req := SarvamTTSRequest{
		Text:                text,
		TargetLanguageCode:  lang,
		Speaker:             speaker,
		Model:               model,
		SpeechSampleRate:    sampleRate,
		EnablePreprocessing: true,
	}
	if len(opts) == 0 {
		return req
	}
	o := opts[0]
	// Explicitly ignore — do not copy onto req (no json fields exist for them).
	_ = o.Pitch
	_ = o.Loudness
	if o.Pace != nil {
		p := *o.Pace
		if isSarvamBulbulV3(model) {
			p = clampSarvamV3Pace(p)
		}
		req.Pace = &p
	}
	return req
}

func isSarvamBulbulV3(model string) bool {
	m := strings.ToLower(strings.TrimSpace(model))
	return strings.Contains(m, "bulbul:v3") || strings.HasSuffix(m, ":v3") || m == "v3"
}

func clampSarvamV3Pace(pace float64) float64 {
	switch {
	case pace < 0.5:
		return 0.5
	case pace > 2.0:
		return 2.0
	default:
		return pace
	}
}

type sarvamTTSResponse struct {
	Audios []string `json:"audios"`
}

func (s *sarvamTTSStream) requestPCM(text, speaker, model string, pace *float64) ([]byte, error) {
	var body []byte
	var err error
	if pace != nil {
		p := *pace
		body, err = json.Marshal(BuildSarvamTTSRequest(
			text, s.provider.lang, speaker, model, s.sampleRate,
			SarvamTTSBuildOpts{Pace: &p},
		))
	} else {
		body, err = json.Marshal(BuildSarvamTTSRequest(
			text, s.provider.lang, speaker, model, s.sampleRate,
		))
	}
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, s.provider.baseURL+"/text-to-speech", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("api-subscription-key", s.provider.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.provider.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("sarvam tts http %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var parsed sarvamTTSResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("sarvam tts decode: %w", err)
	}
	var pcm []byte
	for _, b64 := range parsed.Audios {
		wav, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			continue
		}
		pcmPart, wavRate := wavParsePCM16(wav)
		if wavRate <= 0 {
			wavRate = s.sampleRate
		}
		if wavRate != s.sampleRate {
			resampled, err := ResamplePCM16Linear(pcmPart, wavRate, s.sampleRate)
			if err != nil {
				return nil, fmt.Errorf("sarvam tts resample %d->%d: %w", wavRate, s.sampleRate, err)
			}
			pcmPart = resampled
		}
		pcm = append(pcm, pcmPart...)
	}
	if len(pcm) == 0 {
		return nil, fmt.Errorf("sarvam tts returned no audio")
	}
	return pcm, nil
}

// emitPCM chunks PCM16 bytes onto the audio channel. Returns false if cancelled/closed.
func (s *sarvamTTSStream) emitPCM(turnID string, pcm []byte) bool {
	for off := 0; off < len(pcm); off += sarvamTTSEmitChunkBytes {
		if s.isCancelled(turnID) {
			return false
		}
		end := off + sarvamTTSEmitChunkBytes
		if end > len(pcm) {
			end = len(pcm)
		}
		frame := make([]byte, end-off)
		copy(frame, pcm[off:end])

		s.mu.Lock()
		s.turnSeq[turnID]++
		seq := s.turnSeq[turnID]
		s.mu.Unlock()

		select {
		case <-s.done:
			return false
		case s.audio <- TTSAudioChunk{TurnID: turnID, Seq: seq, MuLaw: frame, Final: false}:
		}
	}
	return true
}

func (s *sarvamTTSStream) emitFinal(turnID string) {
	s.mu.Lock()
	s.turnSeq[turnID]++
	seq := s.turnSeq[turnID]
	s.mu.Unlock()
	select {
	case <-s.done:
	case s.audio <- TTSAudioChunk{TurnID: turnID, Seq: seq, Final: true}:
	}
}

func (s *sarvamTTSStream) emitFailSoft(turnID string) {
	s.fallbacks.Add(1)
	s.emitFinal(turnID)
}

func (s *sarvamTTSStream) isCancelled(turnID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return true
	}
	_, c := s.cancelled[turnID]
	return c
}

func (s *sarvamTTSStream) Cancel(turnID string) error {
	s.mu.Lock()
	s.cancelled[turnID] = struct{}{}
	s.mu.Unlock()
	return nil
}

func (s *sarvamTTSStream) Audio() <-chan TTSAudioChunk { return s.audio }

func (s *sarvamTTSStream) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	close(s.done)
	s.wg.Wait()
	close(s.audio)
	return nil
}

func (s *sarvamTTSStream) Fallbacks() int64 { return s.fallbacks.Load() }

// wavParsePCM16 extracts PCM16 LE samples and the WAV fmt sample rate when present.
func wavParsePCM16(wav []byte) (pcm []byte, sampleRate int) {
	if len(wav) < 12 || !bytes.Equal(wav[0:4], []byte("RIFF")) || !bytes.Equal(wav[8:12], []byte("WAVE")) {
		return wav, 0 // not a WAV container; assume already raw PCM
	}
	pos := 12
	for pos+8 <= len(wav) {
		id := wav[pos : pos+4]
		size := int(binary.LittleEndian.Uint32(wav[pos+4 : pos+8]))
		dataStart := pos + 8
		if bytes.Equal(id, []byte("fmt ")) && size >= 16 && dataStart+16 <= len(wav) {
			sampleRate = int(binary.LittleEndian.Uint32(wav[dataStart+4 : dataStart+8]))
		}
		if bytes.Equal(id, []byte("data")) {
			end := dataStart + size
			if end > len(wav) || size <= 0 {
				end = len(wav)
			}
			return wav[dataStart:end], sampleRate
		}
		if size <= 0 {
			break
		}
		pos = dataStart + size
		if size%2 == 1 {
			pos++
		}
	}
	if len(wav) > 44 {
		return wav[44:], sampleRate
	}
	return nil, sampleRate
}

// wavToPCM16 extracts the raw PCM16 sample bytes from a WAV container.
func wavToPCM16(wav []byte) []byte {
	pcm, _ := wavParsePCM16(wav)
	return pcm
}

// splitForSarvam breaks text into <=maxChars parts on sentence/space boundaries.
func splitForSarvam(text string, maxChars int) []string {
	text = strings.TrimSpace(text)
	if len(text) <= maxChars {
		return []string{text}
	}
	var parts []string
	for len(text) > maxChars {
		cut := maxChars
		if idx := strings.LastIndexAny(text[:maxChars], "।.!?\n "); idx > 0 {
			cut = idx + 1
		}
		parts = append(parts, strings.TrimSpace(text[:cut]))
		text = strings.TrimSpace(text[cut:])
	}
	if text != "" {
		parts = append(parts, text)
	}
	return parts
}
