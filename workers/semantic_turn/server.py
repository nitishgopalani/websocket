#!/usr/bin/env python3
"""Smart Turn v3 semantic EOU worker — length-prefixed JSON over UDS or TCP.

Wire protocol (matches Go internal/media/semantic_turn.go):
  Request:  [uint32 len LE][json bytes]
            {"transcript":"...","rate_hz":8000,"audio_b64":"<optional pcm16 mono b64>"}
  Response: [uint32 len LE][json bytes]
            {"complete":true,"confidence":0.92}

Audio path: decode PCM16 mono, resample 8k->16k if needed, truncate/pad to 8 s,
run smart-turn-v3 ONNX. Transcript-only requests use a lightweight heuristic fallback.
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import re
import signal
import socket
import struct
from pathlib import Path
from typing import Any, Tuple

import numpy as np

from whisper_features import compute_whisper_log_mel_features

logger = logging.getLogger("semantic_turn_worker")

HEADER = struct.Struct("<I")
MODEL_SAMPLE_RATE = 16000
MAX_AUDIO_SECONDS = 8

INCOMPLETE_SUFFIXES = (
    " and",
    " but",
    " because",
    " so",
    " my",
    " the",
    " is",
    " account number is",
    " date of birth is",
)


def truncate_audio_to_last_n_seconds(
    audio_array: np.ndarray,
    n_seconds: int = MAX_AUDIO_SECONDS,
    sample_rate: int = MODEL_SAMPLE_RATE,
) -> np.ndarray:
    max_samples = n_seconds * sample_rate
    if len(audio_array) > max_samples:
        return audio_array[-max_samples:]
    if len(audio_array) < max_samples:
        padding = max_samples - len(audio_array)
        return np.pad(audio_array, (padding, 0), mode="constant", constant_values=0)
    return audio_array


def resample_pcm16(pcm: np.ndarray, src_rate: int, dst_rate: int) -> np.ndarray:
    if src_rate == dst_rate:
        return pcm
    if src_rate == 8000 and dst_rate == 16000:
        return np.repeat(pcm, 2)
    if src_rate == 16000 and dst_rate == 8000:
        return pcm[::2]
    out_len = int(len(pcm) * dst_rate / src_rate)
    x = np.linspace(0, len(pcm) - 1, num=out_len, dtype=np.float32)
    idx = np.floor(x).astype(np.int64)
    frac = x - idx
    idx1 = np.minimum(idx + 1, len(pcm) - 1)
    return ((1.0 - frac) * pcm[idx] + frac * pcm[idx1]).astype(np.int16)


def resolve_onnx_path() -> Path | None:
    env_path = os.environ.get("SMART_TURN_ONNX", "").strip()
    if env_path:
        p = Path(env_path)
        if p.is_file():
            return p
        logger.warning("SMART_TURN_ONNX set but file missing: %s", env_path)

    here = Path(__file__).resolve().parent
    for name in ("smart-turn-v3.1.onnx", "smart-turn-v3.0.onnx", "smart-turn-v3.2-cpu.onnx"):
        candidate = here / name
        if candidate.is_file():
            return candidate
    return None


def transcript_heuristic(transcript: str) -> dict[str, Any]:
    text = (transcript or "").strip()
    if not text:
        return {"complete": False, "confidence": 0.3}

    lower = text.lower()
    for suffix in INCOMPLETE_SUFFIXES:
        if lower.endswith(suffix):
            return {"complete": False, "confidence": 0.65}

    if re.search(r"[.!?]$", text):
        return {"complete": True, "confidence": 0.85}

    if len(text.split()) >= 4:
        return {"complete": True, "confidence": 0.7}

    return {"complete": False, "confidence": 0.55}


class SmartTurnPredictor:
    """Loads smart-turn-v3 ONNX via onnxruntime (CPU default, CUDA optional)."""

    def __init__(self) -> None:
        self._session = None
        onnx_path = resolve_onnx_path()
        if onnx_path is None:
            logger.warning("smart-turn ONNX not found; transcript heuristic only")
            return

        try:
            import onnxruntime as ort

            providers = ["CPUExecutionProvider"]
            available = ort.get_available_providers()
            if "CUDAExecutionProvider" in available:
                providers = ["CUDAExecutionProvider", "CPUExecutionProvider"]
            so = ort.SessionOptions()
            so.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
            so.inter_op_num_threads = 1
            so.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
            self._session = ort.InferenceSession(str(onnx_path), sess_options=so, providers=providers)
            logger.info("smart-turn ONNX loaded from %s (providers=%s)", onnx_path, self._session.get_providers())
        except Exception as exc:  # noqa: BLE001
            logger.warning("smart-turn ONNX load failed; heuristic fallback: %s", exc)

    @property
    def available(self) -> bool:
        return self._session is not None

    def predict(self, transcript: str, pcm: np.ndarray | None, rate: int) -> dict[str, Any]:
        if pcm is not None and len(pcm) > 0 and self._session is not None:
            return self._predict_audio(pcm, rate)
        return transcript_heuristic(transcript)

    def _predict_audio(self, pcm: np.ndarray, rate: int) -> dict[str, Any]:
        assert self._session is not None
        at_rate = resample_pcm16(pcm, rate, MODEL_SAMPLE_RATE) if rate != MODEL_SAMPLE_RATE else pcm
        audio = at_rate.astype(np.float32) / 32768.0
        audio = truncate_audio_to_last_n_seconds(audio, n_seconds=MAX_AUDIO_SECONDS, sample_rate=MODEL_SAMPLE_RATE)

        log_mel = compute_whisper_log_mel_features(audio, do_normalize=True)
        input_features = np.expand_dims(log_mel, axis=0)
        outputs = self._session.run(None, {"input_features": input_features})
        probability = float(outputs[0][0].item())
        complete = probability > 0.5
        return {"complete": complete, "confidence": probability if complete else 1.0 - probability}


def read_frame(conn: socket.socket) -> bytes:
    hdr = _read_exact(conn, HEADER.size)
    body_len = HEADER.unpack(hdr)[0]
    if body_len == 0 or body_len > 1 << 20:
        raise ValueError(f"invalid request body length: {body_len}")
    return _read_exact(conn, body_len)


def write_response(conn: socket.socket, payload: dict[str, Any]) -> None:
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


def decode_request(body: bytes) -> Tuple[str, np.ndarray | None, int]:
    wire = json.loads(body.decode("utf-8"))
    transcript = str(wire.get("transcript", ""))
    rate = int(wire.get("rate_hz") or 8000)
    audio_b64 = wire.get("audio_b64") or ""
    pcm: np.ndarray | None = None
    if audio_b64:
        raw = base64.b64decode(audio_b64)
        if len(raw) % 2 != 0:
            raise ValueError("pcm16 payload must be even length")
        pcm = np.frombuffer(raw, dtype="<i2").copy()
    return transcript, pcm, rate


def handle_conn(conn: socket.socket, predictor: SmartTurnPredictor) -> None:
    conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    try:
        while True:
            body = read_frame(conn)
            transcript, pcm, rate = decode_request(body)
            result = predictor.predict(transcript, pcm, rate)
            write_response(conn, result)
    except ConnectionError:
        return
    except ValueError as exc:
        logger.warning("connection error: %s", exc)
        try:
            write_response(conn, {"complete": True, "confidence": 0.5})
        except Exception:  # noqa: BLE001
            pass


def serve(bind: str, use_unix: bool, predictor: SmartTurnPredictor, stop_event: Any) -> None:
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
    sock.settimeout(1.0)
    logger.info("semantic turn worker listening on %s (%s)", bind, "unix" if use_unix else "tcp")

    while not stop_event.is_set():
        try:
            conn, _ = sock.accept()
        except socket.timeout:
            continue
        except OSError:
            if stop_event.is_set():
                break
            raise
        try:
            handle_conn(conn, predictor)
        except Exception as exc:  # noqa: BLE001
            logger.warning("connection error: %s", exc)
            try:
                write_response(conn, {"complete": True, "confidence": 0.5})
            except Exception:  # noqa: BLE001
                pass
        finally:
            conn.close()

    sock.close()
    logger.info("semantic turn worker stopped")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Smart Turn v3 semantic EOU worker")
    parser.add_argument(
        "--socket",
        default=os.environ.get("SEMANTIC_TURN_SOCKET", ""),
        help="Unix domain socket path",
    )
    parser.add_argument(
        "--addr",
        default=os.environ.get("SEMANTIC_TURN_ADDR", "127.0.0.1:9093"),
        help="TCP listen address (Windows/dev fallback)",
    )
    args = parser.parse_args()

    import threading

    stop_event = threading.Event()

    def _shutdown(signum: int, _frame: Any) -> None:
        logger.info("received signal %s, shutting down", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    predictor = SmartTurnPredictor()
    if args.socket:
        serve(args.socket, use_unix=True, predictor=predictor, stop_event=stop_event)
    else:
        serve(args.addr, use_unix=False, predictor=predictor, stop_event=stop_event)


if __name__ == "__main__":
    main()
