"""Smoke tests for the Whisper-small AMD worker."""

from __future__ import annotations

import json
import socket
import struct
from pathlib import Path

import numpy as np
import pytest

from server import WhisperAMDClassifier, handle_conn, write_response

HEADER = struct.Struct("<I")
RATE_FIELD = struct.Struct("<H")


def _model_available() -> bool:
    clf = WhisperAMDClassifier()
    return clf.available


def test_keyword_classifier_marks_voicemail() -> None:
    clf = WhisperAMDClassifier(threshold=0.4)
    result = clf._score_text("please leave a message after the tone")
    assert result["result"] == "machine"
    assert result["proba_human"] < 0.4


def test_keyword_classifier_defaults_human() -> None:
    clf = WhisperAMDClassifier(threshold=0.4)
    result = clf._score_text("haan main bol raha hoon")
    assert result["result"] == "human"
    assert result["proba_human"] >= 0.4


def test_wire_roundtrip_passthrough() -> None:
    client_sock, server_sock = socket.socketpair()
    pcm = np.array([100, -200, 300, -400], dtype=np.int16)
    body = RATE_FIELD.pack(8000) + pcm.astype("<i2", copy=False).tobytes()
    client_sock.sendall(HEADER.pack(len(body)) + body)

    clf = WhisperAMDClassifier()
    handle_conn(server_sock, clf)

    hdr = _recv_exact(client_sock, HEADER.size)
    resp_len = HEADER.unpack(hdr)[0]
    resp = json.loads(_recv_exact(client_sock, resp_len).decode("utf-8"))
    assert resp["result"] in {"human", "machine", "unknown"}
    assert "proba_human" in resp
    client_sock.close()
    server_sock.close()


@pytest.mark.skipif(not _model_available(), reason="Whisper model weights not available")
def test_whisper_smoke_on_silence() -> None:
    clf = WhisperAMDClassifier()
    pcm = np.zeros(16000, dtype=np.int16)
    result = clf.classify(pcm, 8000)
    assert result["result"] == "human"


def test_readme_exists() -> None:
    assert Path(__file__).with_name("README.md").exists()


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("EOF")
        buf.extend(chunk)
    return bytes(buf)
