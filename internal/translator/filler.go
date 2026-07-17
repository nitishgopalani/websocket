package translator

import (
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"
)

const defaultFillerMaxWords = 3

// FillerLexicon holds per-language filler tokens for suppression checks.
type FillerLexicon struct {
	byLang map[string]map[string]struct{}
	maxWords int
}

var defaultFillers = map[string][]string{
	"hi": {
		"हाँ", "हूँ", "हम्म", "अच्छा", "ठीक", "ठीक है", "हाँ जी", "जी",
		"hmm", "hm", "uh", "um",
	},
	"en": {
		"hmm", "hm", "uh", "um", "ok", "okay", "yeah", "yes", "right",
	},
}

// NewFillerLexicon loads defaults plus optional JSON file from path.
// File format: {"hi":["token",...],"en":["token",...]}
func NewFillerLexicon(langBase, filePath string) *FillerLexicon {
	lex := &FillerLexicon{
		byLang:   map[string]map[string]struct{}{},
		maxWords: defaultFillerMaxWords,
	}
	for lang, words := range defaultFillers {
		lex.addWords(lang, words)
	}
	if filePath != "" {
		if data, err := os.ReadFile(filePath); err == nil {
			var custom map[string][]string
			if json.Unmarshal(data, &custom) == nil {
				for lang, words := range custom {
					lex.addWords(lang, words)
				}
			}
		}
	}
	_ = langBase
	return lex
}

func (f *FillerLexicon) addWords(lang string, words []string) {
	lang = baseLang(lang)
	if f.byLang[lang] == nil {
		f.byLang[lang] = map[string]struct{}{}
	}
	for _, w := range words {
		w = normalizeFillerToken(w)
		if w != "" {
			f.byLang[lang][w] = struct{}{}
		}
	}
}

// Words returns filler tokens for completeness stripping for a language.
func (f *FillerLexicon) Words(lang string) []string {
	lang = baseLang(lang)
	set := f.byLang[lang]
	out := make([]string, 0, len(set))
	for w := range set {
		out = append(out, w)
	}
	return out
}

// IsPureFiller reports whether text is only lexicon tokens (≤ maxWords).
func (f *FillerLexicon) IsPureFiller(lang, text string) bool {
	text = strings.TrimSpace(text)
	if text == "" {
		return false
	}
	tokens := tokenizeFiller(text)
	if len(tokens) == 0 || len(tokens) > f.maxWords {
		return false
	}
	set := f.byLang[baseLang(lang)]
	if len(set) == 0 {
		return false
	}
	joined := strings.Join(tokens, " ")
	if _, ok := set[normalizeFillerToken(joined)]; ok {
		return true
	}
	for _, tok := range tokens {
		if _, ok := set[normalizeFillerToken(tok)]; !ok {
			return false
		}
	}
	return true
}

func baseLang(lang string) string {
	lang = strings.ToLower(strings.TrimSpace(lang))
	if i := strings.IndexByte(lang, '-'); i >= 0 {
		lang = lang[:i]
	}
	return lang
}

func normalizeFillerToken(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func tokenizeFiller(text string) []string {
	fields := strings.FieldsFunc(text, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsPunct(r)
	})
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if f != "" {
			out = append(out, f)
		}
	}
	return out
}

// TurnAudit tracks ASR finals vs translations for zero-loss verification.
type TurnAudit struct {
	lane string
	log  *slog.Logger

	ASRFinalsReceived   atomic.Int64
	TranslationsEmitted atomic.Int64
	FillersSuppressed   atomic.Int64
	MergedTurns         atomic.Int64

	mu      sync.Mutex
	dropped []string
}

func NewTurnAudit(lane string, logger *slog.Logger) *TurnAudit {
	return &TurnAudit{lane: lane, log: logger}
}

func (a *TurnAudit) RecordASRFinal() {
	a.ASRFinalsReceived.Add(1)
}

func (a *TurnAudit) RecordTranslation() {
	a.TranslationsEmitted.Add(1)
}

func (a *TurnAudit) RecordFillerSuppressed(text string) {
	a.FillersSuppressed.Add(1)
	if a.log != nil {
		a.log.Info("translator_filler_suppressed",
			"lane", a.lane,
			"text", text,
		)
	}
}

func (a *TurnAudit) RecordMerged() {
	a.MergedTurns.Add(1)
}

func (a *TurnAudit) RecordDropped(text string) {
	a.mu.Lock()
	a.dropped = append(a.dropped, text)
	a.mu.Unlock()
}

// VerifyAndLog checks the zero-loss invariant and logs totals at teardown.
func (a *TurnAudit) VerifyAndLog() {
	received := a.ASRFinalsReceived.Load()
	emitted := a.TranslationsEmitted.Load()
	suppressed := a.FillersSuppressed.Load()
	merged := a.MergedTurns.Load()
	accounted := emitted + suppressed + merged
	ok := received == accounted

	a.mu.Lock()
	dropped := append([]string(nil), a.dropped...)
	a.mu.Unlock()

	if a.log != nil {
		a.log.Info("translator_turn_audit",
			"lane", a.lane,
			"asr_finals_received", received,
			"translations_emitted", emitted,
			"fillers_suppressed", suppressed,
			"merged_turns", merged,
			"invariant_ok", ok,
		)
		if !ok {
			a.log.Warn("translator_turn_audit_invariant_failed",
				"lane", a.lane,
				"asr_finals_received", received,
				"accounted", accounted,
				"delta", received-accounted,
				"dropped_samples", dropped,
			)
		}
	}
}
