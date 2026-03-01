"""Tests for WebSocket RSA authentication signing."""

from __future__ import annotations

import base64
import tempfile

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from sports_pipeline.realtime.websocket.auth import load_private_key, sign_ws_auth


def _generate_test_key() -> tuple[rsa.RSAPrivateKey, str]:
    """Generate a temporary RSA key pair and write PEM to a temp file."""
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    tmp = tempfile.NamedTemporaryFile(suffix=".pem", delete=False)
    tmp.write(pem)
    tmp.close()
    return private_key, tmp.name


class TestLoadPrivateKey:
    def test_loads_rsa_key(self):
        _, path = _generate_test_key()
        key = load_private_key(path)
        assert isinstance(key, rsa.RSAPrivateKey)

    def test_invalid_path_raises(self):
        import pytest

        with pytest.raises(FileNotFoundError):
            load_private_key("/nonexistent/key.pem")


class TestSignWsAuth:
    def test_produces_valid_signature(self):
        private_key, _ = _generate_test_key()
        api_key_id = "test-api-key-123"
        timestamp_ms = 1709136000000

        result = sign_ws_auth(private_key, api_key_id, timestamp_ms=timestamp_ms)

        assert result["api_key"] == api_key_id
        assert result["timestamp"] == timestamp_ms
        assert isinstance(result["signature"], str)

        # Verify the signature is valid
        sig_bytes = base64.b64decode(result["signature"])
        message = f"{timestamp_ms}\n{api_key_id}".encode()
        public_key = private_key.public_key()
        # Should not raise
        public_key.verify(sig_bytes, message, padding.PKCS1v15(), hashes.SHA256())

    def test_auto_timestamp(self):
        import time

        private_key, _ = _generate_test_key()
        before = int(time.time() * 1000)
        result = sign_ws_auth(private_key, "key")
        after = int(time.time() * 1000)

        assert before <= result["timestamp"] <= after

    def test_different_timestamps_produce_different_signatures(self):
        private_key, _ = _generate_test_key()
        r1 = sign_ws_auth(private_key, "key", timestamp_ms=1000)
        r2 = sign_ws_auth(private_key, "key", timestamp_ms=2000)
        assert r1["signature"] != r2["signature"]
