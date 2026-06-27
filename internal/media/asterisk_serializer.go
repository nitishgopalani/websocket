package media

// AsteriskSerializer emits Dinesh protocol outbound frames (binary PCM16 + text control).
type AsteriskSerializer struct{}

func (AsteriskSerializer) Media(_ string, pcm []byte) ([]byte, error) {
	// Raw PCM16 LE bytes; CarrierEgress sends as a binary WS frame.
	out := make([]byte, len(pcm))
	copy(out, pcm)
	return out, nil
}

func (AsteriskSerializer) Mark(_ string, _ string) ([]byte, error) {
	return nil, nil
}

func (AsteriskSerializer) Clear(_ string) ([]byte, error) {
	return nil, nil
}

func (AsteriskSerializer) Ready() ([]byte, error) {
	return AsteriskReadyMessage()
}

func (AsteriskSerializer) EndOfCall() ([]byte, error) {
	return AsteriskEndOfCallMessage()
}

func (AsteriskSerializer) Error(message, code string) ([]byte, error) {
	return AsteriskErrorMessage(message, code)
}
