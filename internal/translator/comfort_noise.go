package translator

import "encoding/binary"

// 20ms @ 8kHz mono PCM16 — low-level line comfort noise for Party A when B is quiet.
const comfortFrameBytes = 320

func comfortNoisePCM() []byte {
	buf := make([]byte, comfortFrameBytes)
	for i := 0; i < comfortFrameBytes/2; i++ {
		// ~±48 LSB — audible line hiss, not silence.
		sample := int16(((i * 7919) + 104729) % 97)
		sample -= 48
		binary.LittleEndian.PutUint16(buf[i*2:], uint16(sample))
	}
	return buf
}
