"""RSA signing for Kalshi WebSocket authentication.

Kalshi WS auth requires signing a timestamp with the RSA private key.
The signature is sent as part of the login command.
"""

from __future__ import annotations

import base64
import time
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa


def load_private_key(key_path: str) -> rsa.RSAPrivateKey:
    """Load an RSA private key from a PEM file."""
    pem_data = Path(key_path).read_bytes()
    key = serialization.load_pem_private_key(pem_data, password=None)
    if not isinstance(key, rsa.RSAPrivateKey):
        raise TypeError(f"Expected RSA private key, got {type(key).__name__}")
    return key


def sign_ws_auth(
    private_key: rsa.RSAPrivateKey,
    api_key_id: str,
    timestamp_ms: int | None = None,
) -> dict[str, str | int]:
    """Create a signed authentication payload for the Kalshi WebSocket.

    Returns a dict with fields ready to include in the WS login command:
        {"id": <msg_id>, "cmd": "login", "api_key": ..., "timestamp": ..., "signature": ...}
    """
    if timestamp_ms is None:
        timestamp_ms = int(time.time() * 1000)

    # Kalshi expects: sign(timestamp_ms + "\n" + api_key_id)
    message = f"{timestamp_ms}\n{api_key_id}".encode()

    signature = private_key.sign(
        message,
        padding.PKCS1v15(),
        hashes.SHA256(),
    )
    sig_b64 = base64.b64encode(signature).decode("ascii")

    return {
        "api_key": api_key_id,
        "timestamp": timestamp_ms,
        "signature": sig_b64,
    }
