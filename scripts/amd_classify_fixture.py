#!/usr/bin/env python3
"""Send first 2s of ulaw fixture to AMD worker; print JSON result."""
import audioop
import json
import socket
import struct
import sys

def classify(path: str) -> dict:
    ulaw = open(path, "rb").read()[:16000]
    pcm = audioop.ulaw2lin(ulaw, 2)
    body = struct.pack("<H", 8000) + pcm
    hdr = struct.pack("<I", len(body))
    s = socket.create_connection(("127.0.0.1", 9092), timeout=60)
    s.sendall(hdr + body)
    ln = struct.unpack("<I", s.recv(4))[0]
    return json.loads(s.recv(ln))

if __name__ == "__main__":
    for label, path in [
        ("human_real", "testdata/calls/human_real.ulaw"),
        ("voicemail_real", "testdata/calls/voicemail_real.ulaw"),
    ]:
        r = classify(path)
        print(f"{label}: result={r.get('result')} proba={r.get('proba_human')} reason={r.get('reason')}")
