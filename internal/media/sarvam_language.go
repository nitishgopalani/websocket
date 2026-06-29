package media

import (
	"log/slog"
	"os"
	"strings"
)

// Sarvam accepts these BCP-47-style language-code query values (Saaras v3).
var sarvamValidLocales = map[string]struct{}{
	"unknown": {},
	"hi-IN":   {}, "bn-IN": {}, "kn-IN": {}, "ml-IN": {}, "mr-IN": {},
	"od-IN":   {}, "pa-IN": {}, "ta-IN": {}, "te-IN": {}, "en-IN": {},
	"gu-IN":   {}, "as-IN": {}, "ur-IN": {}, "ne-IN": {}, "kok-IN": {},
	"ks-IN":   {}, "sd-IN": {}, "sa-IN": {}, "sat-IN": {}, "mni-IN": {},
	"brx-IN":  {}, "mai-IN": {}, "doi-IN": {},
}

// bareLanguageToSarvam maps loose 2-letter (or alias) codes to Sarvam locales.
var bareLanguageToSarvam = map[string]string{
	"en": "en-IN", "hi": "hi-IN", "bn": "bn-IN", "kn": "kn-IN", "ml": "ml-IN",
	"mr": "mr-IN", "od": "od-IN", "or": "od-IN", "pa": "pa-IN", "ta": "ta-IN",
	"te": "te-IN", "gu": "gu-IN", "as": "as-IN", "ur": "ur-IN", "ne": "ne-IN",
	"kok": "kok-IN", "ks": "ks-IN", "sd": "sd-IN", "sa": "sa-IN", "sat": "sat-IN",
	"mni": "mni-IN", "brx": "brx-IN", "mai": "mai-IN", "doi": "doi-IN",
}

const defaultSarvamLanguage = "hi-IN"

// SarvamLanguageDefault returns the configured fallback locale (LANG_DEFAULT, then ASR_LANGUAGE).
func SarvamLanguageDefault() string {
	if v := strings.TrimSpace(os.Getenv("LANG_DEFAULT")); v != "" {
		return NormalizeSarvamLanguage(v, nil, "")
	}
	if v := strings.TrimSpace(os.Getenv("ASR_LANGUAGE")); v != "" {
		return NormalizeSarvamLanguage(v, nil, "")
	}
	return defaultSarvamLanguage
}

// NormalizeSarvamLanguage maps session/env language values to a Sarvam-accepted locale.
func NormalizeSarvamLanguage(raw string, logger *slog.Logger, streamSID string) string {
	original := strings.TrimSpace(raw)
	if original == "" || strings.EqualFold(original, "unknown") {
		out := SarvamLanguageDefault()
		if original != "" && logger != nil {
			logger.Warn("sarvam language normalized",
				"stream_sid", streamSID,
				"original", original,
				"language_code", out,
			)
		}
		return out
	}

	key := strings.ToLower(original)
	if _, ok := sarvamValidLocales[key]; ok {
		return key
	}

	// Accept en_IN style by normalizing underscore.
	if strings.Contains(key, "_") {
		key = strings.ReplaceAll(key, "_", "-")
		if _, ok := sarvamValidLocales[key]; ok {
			return key
		}
	}

	if mapped, ok := bareLanguageToSarvam[key]; ok {
		if logger != nil {
			logger.Warn("sarvam language normalized",
				"stream_sid", streamSID,
				"original", original,
				"language_code", mapped,
			)
		}
		return mapped
	}

	// Bare prefix before region (e.g. en-us -> en).
	if i := strings.Index(key, "-"); i > 0 {
		prefix := key[:i]
		if mapped, ok := bareLanguageToSarvam[prefix]; ok {
			if logger != nil {
				logger.Warn("sarvam language normalized",
					"stream_sid", streamSID,
					"original", original,
					"language_code", mapped,
				)
			}
			return mapped
		}
	}

	out := SarvamLanguageDefault()
	if logger != nil {
		logger.Warn("sarvam language normalized",
			"stream_sid", streamSID,
			"original", original,
			"language_code", out,
		)
	}
	return out
}

// ResolveSessionASRLanguage picks Sarvam locale from session params (asr_language, language, locale).
func ResolveSessionASRLanguage(params map[string]string, logger *slog.Logger, streamSID string) string {
	if params != nil {
		for _, key := range []string{"asr_language", "language", "locale"} {
			if v := strings.TrimSpace(params[key]); v != "" {
				return NormalizeSarvamLanguage(v, logger, streamSID)
			}
		}
	}
	return SarvamLanguageDefault()
}

// ApplySessionASRLanguage stores normalized locale on the session for ASR dial.
func ApplySessionASRLanguage(session *Session, lang string, logger *slog.Logger) {
	if session == nil {
		return
	}
	if session.Params == nil {
		session.Params = map[string]string{}
	}
	resolved := NormalizeSarvamLanguage(lang, logger, session.StreamSID)
	session.Params["asr_language"] = resolved
	if strings.TrimSpace(session.Params["language"]) == "" {
		session.Params["language"] = resolved
	}
	if logger != nil {
		logger.Info("asr language resolved",
			"stream_sid", session.StreamSID,
			"language_code", resolved,
		)
	}
}
