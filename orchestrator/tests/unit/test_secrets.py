"""Unit tests for secret storage."""

import pytest
from cryptography.fernet import Fernet

from services.secrets import PREFIX, SecretBox, SecretDecryptionError

SECRET = "test-passphrase"
VALUE = "services-api-key-value"


class TestEnabledBox:
    @pytest.fixture
    def box(self):
        return SecretBox(SECRET)

    def test_enabled(self, box):
        assert box.enabled is True

    def test_round_trip(self, box):
        assert box.decrypt(box.encrypt(VALUE)) == VALUE

    def test_ciphertext_hides_value(self, box):
        stored = box.encrypt(VALUE)

        assert stored.startswith(PREFIX)
        assert VALUE not in stored

    def test_same_value_encrypts_differently(self, box):
        assert box.encrypt(VALUE) != box.encrypt(VALUE)

    def test_none_stays_none(self, box):
        assert box.encrypt(None) is None
        assert box.decrypt(None) is None

    def test_plaintext_is_read_back(self, box):
        """Values stored before encryption was enabled still work."""
        assert box.decrypt(VALUE) == VALUE

    def test_bytes_from_redis_accepted(self, box):
        assert box.decrypt(box.encrypt(VALUE).encode("utf-8")) == VALUE

    def test_wrong_secret_rejected(self, box):
        other = SecretBox("another-passphrase")

        with pytest.raises(SecretDecryptionError, match="may have changed"):
            other.decrypt(box.encrypt(VALUE))

    def test_fernet_key_used_directly(self):
        key = Fernet.generate_key().decode()
        box = SecretBox(key)

        assert Fernet(key.encode()).decrypt(
            box.encrypt(VALUE)[len(PREFIX):].encode()
        ).decode() == VALUE

    def test_non_string_rejected(self, box):
        with pytest.raises(ValueError, match="value must be a string"):
            box.encrypt(1234)


class TestDisabledBox:
    @pytest.fixture
    def box(self):
        return SecretBox(None)

    def test_disabled(self, box):
        assert box.enabled is False

    def test_stores_plaintext(self, box):
        assert box.encrypt(VALUE) == VALUE
        assert box.decrypt(VALUE) == VALUE

    def test_cannot_read_encrypted_value(self, box):
        stored = SecretBox(SECRET).encrypt(VALUE)

        with pytest.raises(SecretDecryptionError, match="ORCHESTRATOR_SECRET"):
            box.decrypt(stored)

    def test_warns_once_at_construction(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING, logger="services.secrets"):
            SecretBox("")

        assert any(r.__dict__.get("event") == "secrets_disabled" for r in caplog.records)


class TestFromEnv:
    def test_disabled_without_secret(self, monkeypatch):
        monkeypatch.delenv("ORCHESTRATOR_SECRET", raising=False)

        assert SecretBox.from_env().enabled is False

    def test_enabled_with_secret(self, monkeypatch):
        monkeypatch.setenv("ORCHESTRATOR_SECRET", SECRET)
        box = SecretBox.from_env()

        assert box.enabled is True
        assert box.decrypt(box.encrypt(VALUE)) == VALUE
