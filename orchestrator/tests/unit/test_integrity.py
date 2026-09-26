"""Unit tests for integrity checks on data references."""

import hashlib

import pytest

from models.data_reference import DataReference, Format, Protocol
from services.integrity import (
    IntegrityError,
    compute_checksum,
    is_supported,
    verify_all,
    verify_bytes,
    verify_reference,
)

DATA = b"timestamp,power\n2025-01-01T00:00:00Z,120.5\n"
SHA = hashlib.sha256(DATA).hexdigest()


def inline(data=DATA, **kwargs) -> DataReference:
    return DataReference.from_inline_data(data, Format.CSV, **kwargs)


class TestComputeChecksum:
    def test_sha256_by_default(self):
        assert compute_checksum(DATA) == f"sha256:{SHA}"

    def test_other_algorithm(self):
        assert compute_checksum(DATA, "sha512") == f"sha512:{hashlib.sha512(DATA).hexdigest()}"

    def test_unsupported_algorithm_raises(self):
        with pytest.raises(ValueError, match="unsupported"):
            compute_checksum(DATA, "crc32")

    @pytest.mark.parametrize("name,supported", [
        ("sha256", True), ("SHA256", True), ("md5", True), ("blake2b", True),
        ("shake_128", False), ("crc32", False),
    ])
    def test_supported_algorithms(self, name, supported):
        assert is_supported(name) is supported


class TestVerifyBytes:
    def test_matching_checksum_and_size(self):
        assert verify_bytes(DATA, f"sha256:{SHA}", len(DATA)) is True

    def test_uppercase_hex_accepted(self):
        assert verify_bytes(DATA, f"sha256:{SHA.upper()}", None) is True

    def test_checksum_mismatch_raises(self):
        with pytest.raises(IntegrityError, match="checksum mismatch"):
            verify_bytes(DATA + b"x", f"sha256:{SHA}", None)

    def test_size_mismatch_raises(self):
        with pytest.raises(IntegrityError, match="size mismatch"):
            verify_bytes(DATA, None, len(DATA) + 1)

    def test_nothing_stated_nothing_checked(self):
        assert verify_bytes(DATA, None, None) is False

    def test_unknown_algorithm_is_not_checked(self):
        assert verify_bytes(DATA, "crc32:deadbeef", None) is False


class TestVerifyReference:
    def test_inline_with_correct_checksum(self):
        assert verify_reference(inline(checksum=f"sha256:{SHA}")) is True

    def test_inline_size_is_checked_even_without_checksum(self):
        # from_inline_data states the size
        assert verify_reference(inline()) is True

    def test_inline_with_wrong_checksum_raises(self):
        wrong = hashlib.sha256(b"other").hexdigest()
        with pytest.raises(IntegrityError):
            verify_reference(inline(checksum=f"sha256:{wrong}"))

    def test_inline_with_wrong_size_raises(self):
        ref = DataReference(
            protocol=Protocol.INLINE,
            uri=inline().uri,
            format=Format.CSV,
            size_bytes=3,
        )
        with pytest.raises(IntegrityError, match="size mismatch"):
            verify_reference(ref)

    def test_other_protocols_pass_through_unchecked(self):
        ref = DataReference(
            protocol=Protocol.FILE,
            uri="/app/data/raw_energy.csv",
            format=Format.CSV,
            checksum="sha256:" + "0" * 64,
            size_bytes=1,
        )
        # The orchestrator cannot read the file: nothing is checked, nothing raised
        assert verify_reference(ref) is False

    def test_verify_all_stops_at_first_bad_reference(self):
        good = inline(checksum=f"sha256:{SHA}")
        bad = inline(checksum="sha256:" + "0" * 64)
        verify_all([good])
        with pytest.raises(IntegrityError):
            verify_all([good, bad])

    def test_verify_all_accepts_none(self):
        verify_all(None)
