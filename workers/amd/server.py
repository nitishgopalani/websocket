#!/usr/bin/env python3
"""Whisper-small AMD worker — length-prefixed PCM16 over UDS or TCP.

Wire protocol (matches Go internal/media/amd.go / denoise request format):
  Request:  [uint32 len LE][uint16 rate_hz LE][pcm16 bytes]   len = 2 + pcm_bytes
  Response: [uint32 len LE][json bytes]
            {"result":"human"|"machine"|"unknown","proba_human":0.0-1.0,"reason":"..."}

Defaults favor human precision: proba_human < threshold -> machine only when confident.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import socket
import struct
from typing import Tuple

import numpy as np

logger = logging.getLogger("amd_worker")

HEADER = struct.Struct("<I")
RATE_FIELD = struct.Struct("<H")

DEFAULT_THRESHOLD = float(os.environ.get("AMD_PROBA_HUMAN_THRESHOLD", "0.4"))

VOICEMAIL_PATTERNS = [
    r"leave a message",
    r"after the (tone|beep)",
    r"record your message",
    r"not available",
    r"uplabdh",
    r"sandesh",
    r"message chhod",
    r"baad mein (call|phone)",
    r"voicemail",
    r"press \d",
]


class WhisperAMDClassifier:
    """Transcribes ~2s audio with faster-whisper small and keyword-scores voicemail."""

    def __init__(self, threshold: float = DEFAULT_THRESHOLD) -> None:
        self.threshold = threshold
        self._model = None
        device = os.environ.get("WHISPER_DEVICE", "cpu").strip().lower()
        compute_type = "float16" if device == "cuda" else "int8"
        try:
            from faster_whisper import WhisperModel

            try:
                self._model = WhisperModel("small", device=device, compute_type=compute_type)
                logger.info("faster-whisper small loaded (device=%s, compute_type=%s)", device, compute_type)
            except Exception as cuda_exc:  # noqa: BLE001
                if device == "cuda":
                    logger.warning("CUDA init failed (%s); falling back to CPU int8", cuda_exc)
                    self._model = WhisperModel("small", device="cpu", compute_type="int8")
                    logger.info("faster-whisper small loaded (device=cpu, compute_type=int8)")
                else:
                    raise
        except Exception as exc:  # noqa: BLE001
            logger.warning("Whisper unavailable; keyword-only fallback: %s", exc)

    @property
    def available(self) -> bool:
        return self._model is not None

    def classify(self, pcm: np.ndarray, rate: int) -> dict:
        text = self._transcribe(pcm, rate)
        result = self._score_text(text)
        logger.info(
            "amd classify transcript=%r result=%s proba_human=%.2f reason=%s",
            text[:200],
            result["result"],
            result["proba_human"],
            result["reason"][:160],
        )
        return result

    def _transcribe(self, pcm: np.ndarray, rate: int) -> str:
        if self._model is None or len(pcm) == 0:
            return ""
        audio = pcm.astype(np.float32) / 32768.0
        segments, _ = self._model.transcribe(
            audio,
            language=None,
            beam_size=1,
            vad_filter=False,
        )
        return " ".join(seg.text.strip() for seg in segments).lower()

    def _score_text(self, text: str) -> dict:
        if not text.strip():
            return {"result": "human", "proba_human": 0.85, "reason": "no_transcript_fail_open"}

        hits = []
        for pat in VOICEMAIL_PATTERNS:
            if re.search(pat, text, re.IGNORECASE):
                hits.append(pat)

        if hits:
            proba = max(0.05, self.threshold - 0.2)
            return {
                "result": "machine",
                "proba_human": proba,
                "reason": f"voicemail_keywords:{','.join(hits[:3])}; transcript={text[:120]}",
            }
        return {
            "result": "human",
            "proba_human": 0.9,
            "reason": f"no_voicemail_match; transcript={text[:120]}",
        }


def pcm16_bytes_to_array(data: bytes) -> np.ndarray:
    return np.frombuffer(data, dtype="<i2").copy()


def read_frame(conn: socket.socket) -> Tuple[int, bytes]:
    hdr = _read_exact(conn, HEADER.size)
    body_len = HEADER.unpack(hdr)[0]
    if body_len < RATE_FIELD.size or body_len > 1 << 20:
        raise ValueError(f"invalid request body length: {body_len}")
    body = _read_exact(conn, body_len)
    rate = RATE_FIELD.unpack(body[: RATE_FIELD.size])[0]
    pcm = body[RATE_FIELD.size :]
    if len(pcm) % 2 != 0:
        raise ValueError("pcm16 payload must be even length")
    return rate, pcm


def write_response(conn: socket.socket, payload: dict) -> None:
    body = json.dumps(payload).encode("utf-8")
    conn.sendall(HEADER.pack(len(body)) + body)


def _read_exact(conn: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("unexpected EOF")
        buf.extend(chunk)
    return bytes(buf)


def handle_conn(conn: socket.socket, classifier: WhisperAMDClassifier) -> None:
    rate, pcm_bytes = read_frame(conn)
    if rate not in {8000, 16000}:
        write_response(conn, {"result": "human", "proba_human": 1.0, "reason": f"unsupported_rate_{rate}_fail_open"})
        return
    pcm = pcm16_bytes_to_array(pcm_bytes)
    result = classifier.classify(pcm, rate)
    write_response(conn, result)


def serve(bind: str, use_unix: bool, classifier: WhisperAMDClassifier) -> None:
    if use_unix:
        if os.path.exists(bind):
            os.unlink(bind)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(bind)
    else:
        host, port_str = bind.rsplit(":", 1)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, int(port_str)))
    sock.listen(128)
    logger.info("amd worker listening on %s (%s)", bind, "unix" if use_unix else "tcp")

    while True:
        conn, _ = sock.accept()
        try:
            handle_conn(conn, classifier)
        except Exception as exc:  # noqa: BLE001
            logger.warning("connection error: %s", exc)
            try:
                write_response(conn, {"result": "human", "proba_human": 1.0, "reason": f"error_fail_open:{exc}"})
            except Exception:  # noqa: BLE001
                pass
        finally:
            conn.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Whisper-small AMD worker")
    parser.add_argument("--socket", default=os.environ.get("AMD_SOCKET", ""), help="Unix domain socket path")
    parser.add_argument("--addr", default=os.environ.get("AMD_ADDR", "127.0.0.1:9092"), help="TCP listen address")
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD, help="Human precision threshold")
    args = parser.parse_args()

    classifier = WhisperAMDClassifier(threshold=args.threshold)
    if args.socket:
        serve(args.socket, use_unix=True, classifier=classifier)
    else:
        serve(args.addr, use_unix=False, classifier=classifier)


if __name__ == "__main__":
    main()
