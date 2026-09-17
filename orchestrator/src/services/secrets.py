"""Encryption for secrets the orchestrator stores in Redis.

Only the per-workflow services API key is stored today. It is encrypted with
Fernet (AES-CBC plus HMAC) using a key derived from ``ORCHESTRATOR_SECRET``, so
a Redis dump or an exposed Redis port does not reveal it. It does **not**
protect against someone who can read the orchestrator's own environment.

Without ``ORCHESTRATOR_SECRET`` the box is disabled and values are stored as
before, so an existing deployment keeps working; a warning is logged once.
Decryption always accepts plaintext, so values written before the secret was
configured remain readable.
"""

import base64
import hashlib
import logging
import os

from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger(__name__)

PREFIX = "enc:v1:"


class SecretDecryptionError(Exception):
    """Raised when a stored value cannot be decrypted."""


class SecretBox:
    """Encrypts and decrypts short secrets.

    Args:
        secret: Passphrase or Fernet key. Empty or None disables encryption.
    """

    def __init__(self, secret: str | None):
        self._fernet = Fernet(self._derive_key(secret)) if secret else None
        if self._fernet is None:
            logger.warning(
                "ORCHESTRATOR_SECRET is not set: service API keys are stored "
                "unencrypted in Redis",
                extra={"event": "secrets_disabled"},
            )

    @classmethod
    def from_env(cls) -> "SecretBox":
        """Build a box from the ORCHESTRATOR_SECRET environment variable."""
        return cls(os.environ.get("ORCHESTRATOR_SECRET"))

    @staticmethod
    def _derive_key(secret: str) -> bytes:
        """Accept a Fernet key as-is, otherwise derive one from a passphrase."""
        candidate = secret.encode("utf-8")
        try:
            if len(base64.urlsafe_b64decode(candidate)) == 32:
                return candidate
        except Exception:
            pass
        return base64.urlsafe_b64encode(hashlib.sha256(candidate).digest())

    @property
    def enabled(self) -> bool:
        """True when a secret is configured and values are encrypted."""
        return self._fernet is not None

    def encrypt(self, value: str | None) -> str | None:
        """Encrypt a value, or return it unchanged when disabled."""
        if value is None or self._fernet is None:
            return value
        if not isinstance(value, str):
            raise ValueError("value must be a string")
        token = self._fernet.encrypt(value.encode("utf-8")).decode("ascii")
        return f"{PREFIX}{token}"

    def decrypt(self, value: str | bytes | None) -> str | None:
        """Decrypt a stored value; plaintext is returned unchanged.

        Raises:
            SecretDecryptionError: The value is encrypted but this box cannot
                read it (no secret configured, or the wrong one).
        """
        if value is None:
            return None
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        if not value.startswith(PREFIX):
            return value

        if self._fernet is None:
            raise SecretDecryptionError(
                "stored value is encrypted but ORCHESTRATOR_SECRET is not set"
            )

        token = value[len(PREFIX) :].encode("ascii")
        try:
            return self._fernet.decrypt(token).decode("utf-8")
        except InvalidToken as e:
            raise SecretDecryptionError(
                "stored value could not be decrypted; ORCHESTRATOR_SECRET may "
                "have changed"
            ) from e
