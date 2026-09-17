"""Unit tests for the data-exposure policy."""

import base64

import pytest

from models.data_reference import DataReference, Format, Protocol
from services.privacy import (
    DataPolicy,
    DataPolicyError,
    REDACTED,
    sanitize_error,
)


def inline_ref(size: int) -> DataReference:
    return DataReference.from_inline_data(b"x" * size, Format.CSV)


def file_ref(uri: str = "/shared/wf-1/out.csv") -> DataReference:
    return DataReference(protocol=Protocol.FILE, uri=uri, format=Format.CSV)


class TestSanitizeError:
    def test_none_passes_through(self):
        assert sanitize_error(None) is None

    def test_plain_message_unchanged(self):
        msg = "Connection failed: [Errno 111] Connection refused"
        assert sanitize_error(msg) == msg

    def test_email_redacted(self):
        out = sanitize_error("row rejected for patient.name@hospital.eu in chunk 3")

        assert "patient.name@hospital.eu" not in out
        assert REDACTED in out

    def test_long_digit_sequence_redacted(self):
        out = sanitize_error("invalid national id 198412345678 in row 7")

        assert "198412345678" not in out
        assert "row 7" in out

    def test_short_numbers_kept(self):
        out = sanitize_error("HTTP 503 after 30 seconds, task 12345678")

        assert "503" in out and "30" in out and "12345678" in out

    def test_base64_blob_redacted(self):
        blob = base64.b64encode(b"sensitive payload " * 8).decode()
        out = sanitize_error(f"could not parse {blob}")

        assert blob not in out
        assert REDACTED in out

    def test_long_path_is_kept(self):
        path = "/shared/workflow/outputs/chronics/generated/values/load.csv"
        out = sanitize_error(f"missing file {path}")

        assert path in out

    def test_repeated_word_is_kept(self):
        out = sanitize_error("timeout " * 20)

        assert REDACTED not in out

    def test_truncated_to_limit(self):
        out = sanitize_error("e" * 900, max_chars=100)

        assert out.startswith("e" * 100)
        assert out.endswith("[truncated]")
        assert len(out) < 200

    def test_limit_must_be_positive(self):
        with pytest.raises(ValueError, match="max_chars"):
            sanitize_error("boom", max_chars=0)

    def test_limit_from_env(self, monkeypatch):
        monkeypatch.setenv("MAX_ERROR_CHARS", "10")

        assert sanitize_error("e" * 50).startswith("e" * 10)


class TestInlinePolicy:
    def test_file_reference_always_allowed(self):
        DataPolicy(allow_inline=False).check(file_ref())

    def test_small_inline_allowed(self):
        DataPolicy(max_inline_bytes=1024).check(inline_ref(100))

    def test_oversized_inline_rejected(self):
        policy = DataPolicy(max_inline_bytes=64)

        with pytest.raises(DataPolicyError, match="exceeds the limit"):
            policy.check(inline_ref(500))

    def test_inline_can_be_disabled(self):
        with pytest.raises(DataPolicyError, match="disabled"):
            DataPolicy(allow_inline=False).check(inline_ref(1))

    def test_check_all_scans_every_reference(self):
        policy = DataPolicy(max_inline_bytes=64)

        with pytest.raises(DataPolicyError):
            policy.check_all([file_ref(), inline_ref(500)])

    def test_check_all_accepts_none(self):
        DataPolicy().check_all(None)

    def test_inline_size_is_decoded_size(self):
        assert DataPolicy.inline_size(inline_ref(300)) == 300
        assert DataPolicy.inline_size(file_ref()) == 0

    def test_invalid_limits_rejected(self):
        with pytest.raises(ValueError, match="max_inline_bytes"):
            DataPolicy(max_inline_bytes=0)
        with pytest.raises(ValueError, match="max_error_chars"):
            DataPolicy(max_error_chars=-1)


class TestMasking:
    def test_inline_uri_masked(self):
        masked = DataPolicy().masked_uri(inline_ref(42))

        assert masked == "<inline data, 42 bytes>"

    def test_location_uri_kept(self):
        assert DataPolicy().masked_uri(file_ref()) == "/shared/wf-1/out.csv"


class TestFromEnv:
    def test_defaults(self, monkeypatch):
        for name in ("ALLOW_INLINE", "MAX_INLINE_BYTES", "MAX_ERROR_CHARS"):
            monkeypatch.delenv(name, raising=False)
        policy = DataPolicy.from_env()

        assert policy.allow_inline is True
        assert policy.max_inline_bytes == 64 * 1024
        assert policy.max_error_chars == 500

    def test_reads_environment(self, monkeypatch):
        monkeypatch.setenv("ALLOW_INLINE", "false")
        monkeypatch.setenv("MAX_INLINE_BYTES", "128")
        policy = DataPolicy.from_env()

        assert policy.allow_inline is False
        assert policy.max_inline_bytes == 128

    def test_invalid_number_rejected(self, monkeypatch):
        monkeypatch.setenv("MAX_INLINE_BYTES", "big")

        with pytest.raises(ValueError, match="MAX_INLINE_BYTES"):
            DataPolicy.from_env()
