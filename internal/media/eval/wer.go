package eval

import (
	"strings"
	"unicode"
)

// WER computes word error rate between reference and hypothesis (0..1, lower is better).
func WER(reference, hypothesis string) float64 {
	refWords := tokenize(reference)
	hypWords := tokenize(hypothesis)
	if len(refWords) == 0 {
		if len(hypWords) == 0 {
			return 0
		}
		return 1
	}
	dist := levenshteinWords(refWords, hypWords)
	return float64(dist) / float64(len(refWords))
}

// WERFromFinals joins ASR finals and compares to reference.
func WERFromFinals(reference string, finals []string) float64 {
	hyp := strings.Join(finals, " ")
	return WER(reference, hyp)
}

func tokenize(s string) []string {
	s = strings.ToLower(s)
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsPunct(r)
	})
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		if f != "" {
			out = append(out, f)
		}
	}
	return out
}

func levenshteinWords(a, b []string) int {
	if len(a) == 0 {
		return len(b)
	}
	if len(b) == 0 {
		return len(a)
	}
	prev := make([]int, len(b)+1)
	curr := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		curr[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			curr[j] = min3(curr[j-1]+1, prev[j]+1, prev[j-1]+cost)
		}
		prev, curr = curr, prev
	}
	return prev[len(b)]
}

func min3(a, b, c int) int {
	if a <= b && a <= c {
		return a
	}
	if b <= c {
		return b
	}
	return c
}
