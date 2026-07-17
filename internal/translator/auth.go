package translator

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// VerifyMediaSignature validates X-Fonada-Media-Signature for a session UUID.
// Copied from internal/callbridge/auth.go (callbridge stays untouched).
func VerifyMediaSignature(secret, sessionID, signature string, now time.Time, maxSkew time.Duration) bool {
	secret = strings.TrimSpace(secret)
	sessionID = strings.TrimSpace(sessionID)
	signature = strings.TrimSpace(signature)
	if secret == "" || sessionID == "" || signature == "" {
		return false
	}
	if maxSkew <= 0 {
		maxSkew = 5 * time.Minute
	}

	var ts int64
	var sig string
	for _, part := range strings.Split(signature, ",") {
		part = strings.TrimSpace(part)
		switch {
		case strings.HasPrefix(part, "t="):
			v, err := strconv.ParseInt(strings.TrimPrefix(part, "t="), 10, 64)
			if err != nil {
				return false
			}
			ts = v
		case strings.HasPrefix(part, "v1="):
			sig = strings.TrimPrefix(part, "v1=")
		}
	}
	if ts == 0 || sig == "" {
		return false
	}
	nowUnix := now.Unix()
	if ts < nowUnix-int64(maxSkew.Seconds()) || ts > nowUnix+60 {
		return false
	}
	msg := strconv.FormatInt(ts, 10) + "." + sessionID
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(msg))
	expected := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(sig))
}

func sessionIDFromRequest(r *http.Request) string {
	if v := strings.TrimSpace(r.Header.Get("X-Fonada-Session-Id")); v != "" {
		return v
	}
	return ""
}

func signatureFromRequest(r *http.Request) string {
	return strings.TrimSpace(r.Header.Get("X-Fonada-Media-Signature"))
}
