"""Integrity checks on data references.
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
    """Check data against a stated checksum and size.
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
            if not hmac.compare_digest(actual, expected.strip().lower()):
                raise IntegrityError(
                    f"checksum mismatch: reference states {algorithm}:{expected[:12]}..., "
                    f"data has {algorithm}:{actual[:12]}..."
                )
            checked = True
    return checked


def verify_reference(reference: DataReference) -> bool:
    """Verify a reference whose data the orchestrator holds (inline).
    """
    if reference.protocol != Protocol.INLINE:
        return False
    return verify_bytes(reference.get_inline_data(), reference.checksum, reference.size_bytes)


def verify_all(references: list[DataReference] | None) -> None:
    """Apply :func:`verify_reference` to every reference."""
    for reference in references or []:
        verify_reference(reference)
