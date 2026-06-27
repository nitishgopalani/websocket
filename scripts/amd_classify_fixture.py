#!/usr/bin/env python3
"""Send first 2s of ulaw fixture to AMD worker; print transcript + decision."""
import audioop
import json
import re
import socket
import struct
import sys


def classify(path: str) -> dict:
    ulaw = open(path, "rb").read()[:16000]
    pcm = audioop.ulaw2lin(ulaw, 2)
    body = struct.pack("<H", 8000) + pcm
    hdr = struct.pack("<I", len(body))
    s = socket.create_connection(("127.0.0.1", 9092), timeout=120)
    s.sendall(hdr + body)
    ln = struct.unpack("<I", s.recv(4))[0]
    return json.loads(s.recv(ln))


def extract_transcript(reason: str) -> str:
    m = re.search(r"transcript=([^;]+)", reason or "")
    return m.group(1).strip() if m else ""


if __name__ == "__main__":
    paths = sys.argv[1:] or [
        "testdata/calls/human_real.ulaw",
        "testdata/calls/voicemail_real.ulaw",
    ]
    for path in paths:
        label = path.split("/")[-1].replace(".ulaw", "")
        r = classify(path)
        reason = r.get("reason", "")
        transcript = extract_transcript(reason)
        print(
            f"{label}: result={r.get('result')} proba={r.get('proba_human')} "
            f"latency_ms=(see workers.log) transcript={transcript!r} reason={reason}"
        )
