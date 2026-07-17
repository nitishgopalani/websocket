package media

import (
	"strings"
	"unicode"
)

var (
	englishConnectors = []string{
		"and", "but", "because", "so", "or", "that", "then",
	}
	hindiConnectors = []string{
		"और", "लेकिन", "कि", "तो", "पर", "क्योंकि", "मगर",
	}
)

// TextIsComplete applies a cheap rule-based completeness heuristic (no model/API).
// lang is a base tag ("en", "hi"). Empty lang treats text as complete.
func TextIsComplete(text, lang string, fillerWords ...string) bool {
	text = strings.TrimSpace(text)
	if text == "" {
		return false
	}
	if endsWithConnector(text, lang) {
		return false
	}
	contentWords := contentWordCount(text, lang, fillerWords...)
	return contentWords >= 2
}

func endsWithConnector(text, lang string) bool {
	words := tokenizeUtterance(text)
	if len(words) == 0 {
		return false
	}
	last := normalizeToken(words[len(words)-1])
	connectors := englishConnectors
	if baseLangTag(lang) == "hi" {
		connectors = hindiConnectors
	}
	for _, c := range connectors {
		if last == normalizeToken(c) {
			return true
		}
	}
	return false
}

func contentWordCount(text, lang string, fillerWords ...string) int {
	fillerSet := buildFillerSet(lang, fillerWords)
	count := 0
	for _, w := range tokenizeUtterance(text) {
		if fillerSet[normalizeToken(w)] {
			continue
		}
		count++
	}
	return count
}

func buildFillerSet(lang string, extra []string) map[string]bool {
	set := map[string]bool{}
	for _, w := range extra {
		set[normalizeToken(w)] = true
	}
	return set
}

func tokenizeUtterance(text string) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
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

func normalizeToken(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func baseLangTag(lang string) string {
	lang = strings.ToLower(strings.TrimSpace(lang))
	if i := strings.IndexByte(lang, '-'); i >= 0 {
		lang = lang[:i]
	}
	return lang
}
