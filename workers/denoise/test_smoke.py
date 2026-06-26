"""Smoke tests for the DeepFilterNet3 denoise worker."""

from __future__ import annotations

import socket
import struct
from pathlib import Path

import numpy as np
import pytest

from server import DeepFilterDenoiser, handle_conn, pcm16_array_to_bytes, pcm16_bytes_to_array

HEADER = struct.Struct("<I")
RATE_FIELD = struct.Struct("<H")


def _model_weights_present() -> bool:
    denoiser = DeepFilterDenoiser()
    return denoiser.available


@pytest.mark.skipif(not _model_weights_present(), reason="DeepFilterNet model weights not available")
def test_denoise_noisy_sample_changes_signal() -> None:
    denoiser = DeepFilterDenoiser()
    rng = np.random.default_rng(0)
    pcm = (rng.standard_normal(160).astype(np.float32) * 2000).astype(np.int16)
    out = denoiser.process(pcm, 8000)
    assert len(out) == len(pcm)
    assert not np.array_equal(out, pcm)


def test_wire_roundtrip_passthrough_without_model(monkeypatch: pytest.MonkeyPatch) -> None:
    """Protocol smoke test using passthrough when model is absent."""

    class Passthrough:
        def process(self, pcm: np.ndarray, rate: int) -> np.ndarray:
            return pcm

    client_sock, server_sock = socket.socketpair()
    pcm = np.array([100, -200, 300, -400], dtype=np.int16)
    pcm_bytes = pcm16_array_to_bytes(pcm)
    body = RATE_FIELD.pack(8000) + pcm_bytes
    client_sock.sendall(HEADER.pack(len(body)) + body)

    class FakeDenoiser:
        def process(self, pcm_in: np.ndarray, rate: int) -> np.ndarray:
            return Passthrough().process(pcm_in, rate)

    handle_conn(server_sock, FakeDenoiser())  # type: ignore[arg-type]

    hdr = _recv_exact(client_sock, HEADER.size)
    resp_len = HEADER.unpack(hdr)[0]
    resp = _recv_exact(client_sock, resp_len)
    assert pcm16_bytes_to_array(resp).tolist() == pcm.tolist()
    client_sock.close()
    server_sock.close()


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("EOF")
        buf.extend(chunk)
    return bytes(buf)


def test_readme_exists() -> None:
    assert Path(__file__).with_name("README.md").exists()
