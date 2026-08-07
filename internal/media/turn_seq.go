package media

import (
	"strconv"
	"strings"
)

// TurnSeq extracts a monotonic turn ordinal from IDs like "{session}-t4" or "t4".
// Returns 0 when the ID has no parseable suffix.
func TurnSeq(turnID string) int {
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return 0
	}
	if i := strings.LastIndex(turnID, "-t"); i >= 0 {
		n, err := strconv.Atoi(turnID[i+2:])
		if err == nil && n > 0 {
			return n
		}
	}
	if strings.HasPrefix(turnID, "t") {
		n, err := strconv.Atoi(turnID[1:])
		if err == nil && n > 0 {
			return n
		}
	}
	return 0
}

// TurnSeqLess reports whether a is strictly older than b by TurnSeq.
// When either side is unparseable, falls back to string inequality only if b
// is non-empty and a differs (treated as not-less so callers must not demote).
func TurnSeqLess(a, b string) bool {
	sa, sb := TurnSeq(a), TurnSeq(b)
	if sa > 0 && sb > 0 {
		return sa < sb
	}
	return false
}
