"""Integrity checks on data references.

A reference may carry ``checksum`` ("algorithm:hex") and ``size_bytes`` for the
data it points to. The orchestrator never fetches that data - it routes
references, not payloads - so it can only check what it already holds: inline
payloads. For every other protocol the two fields travel with the reference
unchanged, to the service that reads the data and can check it
(``use-cases/common/integrity.py`` does this for files).
"""

import hashlib
import hmac

from models.data_reference import DataReference, Protocol

DEFAULT_ALGORITHM = "sha256"


class IntegrityError(Exception):
    """Raised when data does not match the checksum or size its reference states."""


def is_supported(algorithm: str) -> bool:
    """True for hash algorithms every Python provides with a fixed-size digest."""
    name = algorithm.lower()
    return name in hashlib.algorithms_guaranteed and not name.startswith("shake_")


def compute_checksum(data: bytes, algorithm: str = DEFAULT_ALGORITHM) -> str:
    """Checksum of ``data`` in the ``algorithm:hex`` form references use."""
    if not is_supported(algorithm):
        raise ValueError(f"unsupported checksum algorithm: {algorithm}")
    name = algorithm.lower()
    return f"{name}:{hashlib.new(name, data).hexdigest()}"


def verify_bytes(data: bytes, checksum: str | None, size_bytes: int | None) -> bool:
    """Check ``data`` against a stated checksum and size.

    Returns True if something was checked, False if there was nothing to check
    (no checksum or size, or a checksum algorithm this Python does not have).

    Raises:
        IntegrityError: The size or the checksum does not match.
    """
    checked = False
    if size_bytes is not None:
        if len(data) != size_bytes:
            raise IntegrityError(
                f"size mismatch: reference states {size_bytes} bytes, data has {len(data)}"
            )
        checked = True

    if checksum:
        algorithm, expected = checksum.split(":", 1)
        if is_supported(algorithm):
            actual = hashlib.new(algorithm.lower(), data).hexdigest()
            # Constant-time for the same reason as key checks; hex is case-insensitive
            if not hmac.compare_digest(actual, expected.strip().lower()):
                raise IntegrityError(
                    f"checksum mismatch: reference states {algorithm}:{expected[:12]}..., "
                    f"data has {algorithm}:{actual[:12]}..."
                )
            checked = True
    return checked


def verify_reference(reference: DataReference) -> bool:
    """Verify a reference whose data the orchestrator holds (inline).

    Returns True if the reference was verified, False if it was passed through
    unchecked: another protocol, or no checksum and size to check against.

    Raises:
        IntegrityError: An inline payload does not match its checksum or size.
    """
    if reference.protocol != Protocol.INLINE:
        return False
    return verify_bytes(reference.get_inline_data(), reference.checksum, reference.size_bytes)


def verify_all(references: list[DataReference] | None) -> None:
    """Apply :func:`verify_reference` to every reference."""
    for reference in references or []:
        verify_reference(reference)
