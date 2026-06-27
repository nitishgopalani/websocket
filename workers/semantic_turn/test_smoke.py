"""Smoke tests for the Smart Turn v3 semantic EOU worker."""

from __future__ import annotations

import base64
import json
import socket
import struct
from pathlib import Path

import numpy as np
import pytest

from server import SmartTurnPredictor, handle_conn, resolve_onnx_path, transcript_heuristic

HEADER = struct.Struct("<I")


def _model_available() -> bool:
    return resolve_onnx_path() is not None and SmartTurnPredictor().available


def test_transcript_heuristic_complete_sentence() -> None:
    result = transcript_heuristic("I want to pay my bill.")
    assert result["complete"] is True
    assert result["confidence"] >= 0.5


def test_transcript_heuristic_incomplete_trailing() -> None:
    result = transcript_heuristic("my account number is")
    assert result["complete"] is False


def test_wire_roundtrip_json() -> None:
    client_sock, server_sock = socket.socketpair()
    body = json.dumps(
        {
            "transcript": "haan ji",
            "rate_hz": 8000,
            "audio_b64": base64.b64encode(np.zeros(320, dtype=np.int16).tobytes()).decode("ascii"),
        }
    ).encode("utf-8")
    client_sock.sendall(HEADER.pack(len(body)) + body)

    predictor = SmartTurnPredictor()
    handle_conn(server_sock, predictor)

    hdr = _recv_exact(client_sock, HEADER.size)
    resp_len = HEADER.unpack(hdr)[0]
    resp = json.loads(_recv_exact(client_sock, resp_len).decode("utf-8"))
    assert "complete" in resp
    assert "confidence" in resp
    assert isinstance(resp["complete"], bool)
    assert isinstance(resp["confidence"], (int, float))
    client_sock.close()
    server_sock.close()


@pytest.mark.skipif(not _model_available(), reason="smart-turn ONNX weights not available")
def test_model_inference_on_silence() -> None:
    predictor = SmartTurnPredictor()
    pcm = np.zeros(16000, dtype=np.int16)
    result = predictor.predict("", pcm, 16000)
    assert "complete" in result
    assert "confidence" in result


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
