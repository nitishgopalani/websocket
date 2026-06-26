#!/usr/bin/env python3
"""DeepFilterNet3 denoise worker — length-prefixed PCM16 over UDS or TCP.

Wire protocol (little-endian, matches Go internal/media/denoise.go):
  Request:  [uint32 len][uint16 rate_hz][pcm16 bytes]   len = 2 + pcm_bytes
  Response: [uint32 len][pcm16 bytes]                   same sample count as input

Go sends canonical telephony PCM16 mono at 8000 or 16000 Hz. This worker
resamples to 48 kHz for DeepFilterNet, denoises, then resamples back.
"""

from __future__ import annotations

import argparse
import logging
import os
import socket
import struct
import sys
from typing import Tuple

import numpy as np

logger = logging.getLogger("denoise_worker")

HEADER = struct.Struct("<I")
RATE_FIELD = struct.Struct("<H")

SUPPORTED_RATES = {8000, 16000}
MODEL_RATE = 48000


def resample_pcm16(pcm: np.ndarray, src_rate: int, dst_rate: int) -> np.ndarray:
    if src_rate == dst_rate:
        return pcm
    if src_rate == 8000 and dst_rate == 48000:
        return np.repeat(pcm, 6)
    if src_rate == 48000 and dst_rate == 8000:
        return pcm[::6]
    if src_rate == 16000 and dst_rate == 48000:
        return np.repeat(pcm, 3)
    if src_rate == 48000 and dst_rate == 16000:
        return pcm[::3]
    # Generic linear fallback
    out_len = int(len(pcm) * dst_rate / src_rate)
    x = np.linspace(0, len(pcm) - 1, num=out_len, dtype=np.float32)
    idx = np.floor(x).astype(np.int64)
    frac = x - idx
    idx1 = np.minimum(idx + 1, len(pcm) - 1)
    return ((1.0 - frac) * pcm[idx] + frac * pcm[idx1]).astype(np.int16)


def pcm16_bytes_to_array(data: bytes) -> np.ndarray:
    return np.frombuffer(data, dtype="<i2").copy()


def pcm16_array_to_bytes(arr: np.ndarray) -> bytes:
    return arr.astype("<i2", copy=False).tobytes()


class DeepFilterDenoiser:
    """Wraps DeepFilterNet3; falls back to passthrough if model unavailable."""

    def __init__(self) -> None:
        self._model = None
        self._state = None
        self._enhance = None
        try:
            from df.enhance import enhance, init_df

            self._model, self._state, _ = init_df()
            self._enhance = enhance
            logger.info("DeepFilterNet3 model loaded")
        except Exception as exc:  # noqa: BLE001 - startup path
            logger.warning("DeepFilterNet3 unavailable, passthrough mode: %s", exc)

    @property
    def available(self) -> bool:
        return self._enhance is not None

    def process(self, pcm: np.ndarray, rate: int) -> np.ndarray:
        if len(pcm) == 0:
            return pcm
        at_48k = resample_pcm16(pcm, rate, MODEL_RATE)
        if self._enhance is None:
            out_48k = at_48k
        else:
            import torch

            audio = torch.from_numpy(at_48k.astype(np.float32) / 32768.0).unsqueeze(0)
            enhanced = self._enhance(self._model, self._state, audio)
            out_48k = (enhanced.squeeze(0).cpu().numpy() * 32768.0).astype(np.int16)
        return resample_pcm16(out_48k, MODEL_RATE, rate)


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


def write_frame(conn: socket.socket, pcm: bytes) -> None:
    conn.sendall(HEADER.pack(len(pcm)) + pcm)


def _read_exact(conn: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("unexpected EOF")
        buf.extend(chunk)
    return bytes(buf)


def handle_conn(conn: socket.socket, denoiser: DeepFilterDenoiser) -> None:
    rate, pcm_bytes = read_frame(conn)
    if rate not in SUPPORTED_RATES:
        raise ValueError(f"unsupported sample rate: {rate}")
    pcm = pcm16_bytes_to_array(pcm_bytes)
    out = denoiser.process(pcm, rate)
    if len(out) * 2 != len(pcm_bytes):
        raise ValueError("denoiser changed frame sample count")
    write_frame(conn, pcm16_array_to_bytes(out))


def serve(bind: str, use_unix: bool, denoiser: DeepFilterDenoiser) -> None:
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
    logger.info("denoise worker listening on %s (%s)", bind, "unix" if use_unix else "tcp")

    while True:
        conn, _ = sock.accept()
        try:
            handle_conn(conn, denoiser)
        except Exception as exc:  # noqa: BLE001 - per-connection fail-open
            logger.warning("connection error: %s", exc)
        finally:
            conn.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="DeepFilterNet3 denoise worker")
    parser.add_argument(
        "--socket",
        default=os.environ.get("DENOISE_SOCKET", ""),
        help="Unix domain socket path (preferred on Linux)",
    )
    parser.add_argument(
        "--addr",
        default=os.environ.get("DENOISE_ADDR", "127.0.0.1:9091"),
        help="TCP listen address (Windows/dev fallback)",
    )
    args = parser.parse_args()

    denoiser = DeepFilterDenoiser()
    if args.socket:
        serve(args.socket, use_unix=True, denoiser=denoiser)
    else:
        serve(args.addr, use_unix=False, denoiser=denoiser)


if __name__ == "__main__":
    main()
