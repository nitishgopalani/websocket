package callbridge

import "errors"

var (
	errMissingBridgeID = errors.New("callbridge: missing bridge id")
	errRoomClosed      = errors.New("callbridge: room closed")
	errBridgeFull      = errors.New("callbridge: bridge full")
	errLegTaken        = errors.New("callbridge: leg already taken")
	errRateMismatch    = errors.New("callbridge: audio rate mismatch")
)
