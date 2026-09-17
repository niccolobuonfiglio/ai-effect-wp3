"""Limits on what data the orchestrator stores, forwards and logs.

The orchestrator is a control plane: it should hold references to data, not the
data itself, and it should not spread whatever a service happens to put into an
error message. This module centralises three rules:

* **Inline payloads** - ``protocol: "inline"`` carries base64 data inside the
  reference, so it ends up in Redis and in API responses. Their size is capped
  and they can be forbidden entirely.
* **Masking** - inline payloads are replaced by a short placeholder in API
  responses; workers keep using the stored value.
* **Error sanitising** - error text stored on tasks, in service health and in
  alerts is truncated and stripped of obvious personal identifiers.

None of this can stop a service from producing sensitive data; it limits how
far that data travels through the orchestrator.
"""

import os
import re

from models.data_reference import DataReference, Protocol

DEFAULT_MAX_INLINE_BYTES = 64 * 1024
DEFAULT_MAX_ERROR_CHARS = 500

REDACTED = "[redacted]"
TRUNCATED = " ... [truncated]"

# Conservative patterns: identifiers that are personal data in almost any
# context, plus long opaque blobs that are usually payload rather than message.
_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_LONG_DIGITS = re.compile(r"(?<!\d)\d{9,}(?!\d)")
# A blob is a long base64-looking token that mixes cases and digits. The
# lookaheads keep ordinary text out: a long file path or a repeated word is
# lower case only, so it stays readable in the error message.
_BLOB = re.compile(
    r"(?<![A-Za-z0-9+/=])"
    r"(?=[A-Za-z0-9+/]*[a-z])(?=[A-Za-z0-9+/]*[A-Z])(?=[A-Za-z0-9+/]*\d)"
    r"[A-Za-z0-9+/]{40,}={0,2}"
    r"(?![A-Za-z0-9+/=])"
)


class DataPolicyError(Exception):
    """Raised when a data reference breaks the configured policy."""


def sanitize_error(text: str | None, max_chars: int | None = None) -> str | None:
    """Redact identifiers in an error message and cap its length.

    Args:
        text: The raw error text, or None.
        max_chars: Maximum length; defaults to ``MAX_ERROR_CHARS`` or 500.
    """
    if text is None:
        return None

    if max_chars is None:
        max_chars = _int_env("MAX_ERROR_CHARS", DEFAULT_MAX_ERROR_CHARS)
    if max_chars <= 0:
        raise ValueError("max_chars must be positive")

    cleaned = _EMAIL.sub(REDACTED, text)
    cleaned = _BLOB.sub(REDACTED, cleaned)
    cleaned = _LONG_DIGITS.sub(REDACTED, cleaned)

    if len(cleaned) > max_chars:
        cleaned = cleaned[:max_chars].rstrip() + TRUNCATED
    return cleaned


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError as e:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from e


def _bool_env(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


class DataPolicy:
    """Decides which references may pass and how they are shown.

    Args:
        allow_inline: False rejects every inline reference.
        max_inline_bytes: Largest inline payload accepted, in decoded bytes.
        max_error_chars: Length cap applied by :meth:`sanitize`.
    """

    def __init__(
        self,
        allow_inline: bool = True,
        max_inline_bytes: int = DEFAULT_MAX_INLINE_BYTES,
        max_error_chars: int = DEFAULT_MAX_ERROR_CHARS,
    ):
        if max_inline_bytes <= 0:
            raise ValueError("max_inline_bytes must be positive")
        if max_error_chars <= 0:
            raise ValueError("max_error_chars must be positive")

        self.allow_inline = allow_inline
        self.max_inline_bytes = max_inline_bytes
        self.max_error_chars = max_error_chars

    @classmethod
    def from_env(cls) -> "DataPolicy":
        """Build a policy from ALLOW_INLINE, MAX_INLINE_BYTES, MAX_ERROR_CHARS."""
        return cls(
            allow_inline=_bool_env("ALLOW_INLINE", True),
            max_inline_bytes=_int_env("MAX_INLINE_BYTES", DEFAULT_MAX_INLINE_BYTES),
            max_error_chars=_int_env("MAX_ERROR_CHARS", DEFAULT_MAX_ERROR_CHARS),
        )

    # ------------------------------------------------------------------
    # references
    # ------------------------------------------------------------------
    @staticmethod
    def inline_size(reference: DataReference) -> int:
        """Decoded size of an inline payload, 0 for any other protocol."""
        if reference.protocol != Protocol.INLINE:
            return 0
        try:
            return len(reference.get_inline_data())
        except Exception:
            # The reference model already validated the base64; be defensive.
            return len(reference.uri)

    def check(self, reference: DataReference) -> None:
        """Raise DataPolicyError if this reference may not be stored."""
        if reference is None:
            raise ValueError("reference is required")
        if reference.protocol != Protocol.INLINE:
            return

        if not self.allow_inline:
            raise DataPolicyError(
                "inline data references are disabled (ALLOW_INLINE=false); "
                "pass data by reference instead"
            )

        size = self.inline_size(reference)
        if size > self.max_inline_bytes:
            raise DataPolicyError(
                f"inline payload of {size} bytes exceeds the limit of "
                f"{self.max_inline_bytes} bytes; pass data by reference instead"
            )

    def check_all(self, references: list[DataReference] | None) -> None:
        """Apply :meth:`check` to every reference."""
        for reference in references or []:
            self.check(reference)

    def masked_uri(self, reference: DataReference) -> str:
        """URI safe to show in an API response.

        Inline payloads become a placeholder; every other protocol keeps its
        URI, which is a location rather than the data.
        """
        if reference.protocol != Protocol.INLINE:
            return reference.uri
        return f"<inline data, {self.inline_size(reference)} bytes>"

    # ------------------------------------------------------------------
    # errors
    # ------------------------------------------------------------------
    def sanitize(self, text: str | None) -> str | None:
        """Sanitise error text with this policy's length cap."""
        return sanitize_error(text, self.max_error_chars)
