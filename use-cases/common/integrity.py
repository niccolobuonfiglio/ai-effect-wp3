"""Checksums for the data a DataReference points to.

The orchestrator never reads the data behind a reference, so the service that
does read it is the one that can check it arrived intact. A producer states the
checksum and size of what it wrote; the consumer checks them before using it:

    output = DataReference(protocol="file", uri=str(path), format="csv",
                           **describe_file(path))
    ...
    verify_file(reference, path)   # raises IntegrityError on a mismatch
"""

import hashlib
import hmac
from pathlib import Path

DEFAULT_ALGORITHM = "sha256"
_CHUNK = 1 << 20


class IntegrityError(Exception):
    """The data does not match the checksum or size its reference states."""


def _supported(algorithm: str) -> bool:
    name = algorithm.lower()
    return name in hashlib.algorithms_guaranteed and not name.startswith("shake_")


def checksum_bytes(data: bytes, algorithm: str = DEFAULT_ALGORITHM) -> str:
    """Checksum of ``data`` as ``algorithm:hex``."""
    return f"{algorithm}:{hashlib.new(algorithm, data).hexdigest()}"


def checksum_file(path, algorithm: str = DEFAULT_ALGORITHM) -> str:
    """Checksum of a file as ``algorithm:hex``, read in chunks."""
    digest = hashlib.new(algorithm)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(_CHUNK), b""):
            digest.update(chunk)
    return f"{algorithm}:{digest.hexdigest()}"


def describe_file(path, algorithm: str = DEFAULT_ALGORITHM) -> dict:
    """``checksum`` and ``size_bytes`` of a file, ready to put in a DataReference."""
    return {
        "checksum": checksum_file(path, algorithm),
        "size_bytes": Path(path).stat().st_size,
    }


def verify_file(reference: dict, path) -> bool:
    """Check a file against the checksum and size its reference states.

    Returns True if something was checked, False if the reference states
    nothing to check (or an algorithm this Python does not have).

    Raises:
        IntegrityError: The size or the checksum does not match.
    """
    checked = False
    size = reference.get("size_bytes")
    if size is not None:
        actual_size = Path(path).stat().st_size
        if actual_size != size:
            raise IntegrityError(
                f"size mismatch for {path}: reference states {size} bytes, file has {actual_size}"
            )
        checked = True

    checksum = reference.get("checksum")
    if checksum:
        algorithm, expected = checksum.split(":", 1)
        if _supported(algorithm):
            actual = checksum_file(path, algorithm.lower()).split(":", 1)[1]
            if not hmac.compare_digest(actual, expected.strip().lower()):
                raise IntegrityError(
                    f"checksum mismatch for {path}: reference states "
                    f"{algorithm}:{expected[:12]}..., file has {algorithm}:{actual[:12]}..."
                )
            checked = True
    return checked
