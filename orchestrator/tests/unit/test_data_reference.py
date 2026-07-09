"""Unit tests for DataReference model."""

import base64

import pytest
from pydantic import ValidationError

from models.data_reference import DataReference, Format, Protocol


class TestProtocolEnum:
    """Tests for Protocol enum."""

    def test_all_protocols_defined(self):
        expected = {"s3", "http", "https", "nfs", "grpc", "mqtt", "villas", "inline", "file"}
        actual = {p.value for p in Protocol}
        assert actual == expected

    def test_protocol_from_string(self):
        assert Protocol("s3") == Protocol.S3
        assert Protocol("http") == Protocol.HTTP
        assert Protocol("file") == Protocol.FILE

    def test_invalid_protocol_raises(self):
        with pytest.raises(ValueError):
            Protocol("invalid")


class TestFormatEnum:
    """Tests for Format enum."""

    def test_all_formats_defined(self):
        expected = {"json", "csv", "parquet", "protobuf", "binary", "xml"}
        actual = {f.value for f in Format}
        assert actual == expected

    def test_format_from_string(self):
        assert Format("json") == Format.JSON
        assert Format("csv") == Format.CSV


class TestDataReferenceCreation:
    """Tests for DataReference instantiation."""

    def test_create_minimal(self):
        ref = DataReference(
            protocol=Protocol.S3,
            uri="s3://bucket/key",
            format=Format.JSON,
        )
        assert ref.protocol == Protocol.S3
        assert ref.uri == "s3://bucket/key"
        assert ref.format == Format.JSON
        assert ref.schema_uri is None
        assert ref.size_bytes is None
        assert ref.checksum is None
        assert ref.metadata == {}

    def test_create_with_all_fields(self):
        ref = DataReference(
            protocol=Protocol.HTTPS,
            uri="https://api.example.com/data",
            format=Format.JSON,
            schema_uri="https://schema.example.com/v1",
            size_bytes=1024,
            checksum="sha256:abc123",
            metadata={"version": "1.0"},
        )
        assert ref.size_bytes == 1024
        assert ref.checksum == "sha256:abc123"
        assert ref.metadata["version"] == "1.0"

    def test_create_with_string_protocol(self):
        ref = DataReference(
            protocol="s3",
            uri="s3://bucket/key",
            format="json",
        )
        assert ref.protocol == Protocol.S3
        assert ref.format == Format.JSON


class TestDataReferenceValidation:
    """Tests for DataReference validation."""

    def test_empty_uri_raises(self):
        with pytest.raises(ValidationError, match="uri must not be empty"):
            DataReference(protocol=Protocol.S3, uri="", format=Format.JSON)

    def test_whitespace_uri_raises(self):
        with pytest.raises(ValidationError, match="uri must not be empty"):
            DataReference(protocol=Protocol.S3, uri="   ", format=Format.JSON)

    def test_s3_uri_must_have_prefix(self):
        with pytest.raises(ValidationError, match="S3 URI must start with"):
            DataReference(protocol=Protocol.S3, uri="bucket/key", format=Format.JSON)

    def test_http_uri_must_have_prefix(self):
        with pytest.raises(ValidationError, match="HTTP URI must start with"):
            DataReference(protocol=Protocol.HTTP, uri="example.com/data", format=Format.JSON)

    def test_https_uri_must_have_prefix(self):
        with pytest.raises(ValidationError, match="HTTPS URI must start with"):
            DataReference(protocol=Protocol.HTTPS, uri="http://example.com", format=Format.JSON)

    def test_nfs_uri_must_have_separator(self):
        with pytest.raises(ValidationError, match="NFS URI must be host:path"):
            DataReference(protocol=Protocol.NFS, uri="path/only", format=Format.JSON)

    def test_mqtt_uri_must_have_prefix(self):
        with pytest.raises(ValidationError, match="MQTT URI must start with"):
            DataReference(protocol=Protocol.MQTT, uri="broker.example.com", format=Format.JSON)

    def test_inline_uri_must_be_valid_base64(self):
        with pytest.raises(ValidationError, match="INLINE uri must be valid base64"):
            DataReference(protocol=Protocol.INLINE, uri="not-valid-base64!!!", format=Format.JSON)

    def test_size_bytes_negative_raises(self):
        with pytest.raises(ValidationError, match="size_bytes must be non-negative"):
            DataReference(
                protocol=Protocol.S3,
                uri="s3://bucket/key",
                format=Format.JSON,
                size_bytes=-1,
            )

    def test_checksum_missing_colon_raises(self):
        with pytest.raises(ValidationError, match="algorithm:value"):
            DataReference(
                protocol=Protocol.S3,
                uri="s3://bucket/key",
                format=Format.JSON,
                checksum="sha256abc123",
            )

    def test_checksum_empty_algorithm_raises(self):
        with pytest.raises(ValidationError, match="algorithm and value required"):
            DataReference(
                protocol=Protocol.S3,
                uri="s3://bucket/key",
                format=Format.JSON,
                checksum=":abc123",
            )

    def test_checksum_empty_value_raises(self):
        with pytest.raises(ValidationError, match="algorithm and value required"):
            DataReference(
                protocol=Protocol.S3,
                uri="s3://bucket/key",
                format=Format.JSON,
                checksum="sha256:",
            )


class TestDataReferenceValidProtocols:
    """Tests for valid protocol-specific URIs."""

    def test_valid_s3_uri(self):
        ref = DataReference(protocol=Protocol.S3, uri="s3://bucket/key", format=Format.JSON)
        assert ref.uri == "s3://bucket/key"

    def test_valid_http_uri(self):
        ref = DataReference(protocol=Protocol.HTTP, uri="http://example.com/data", format=Format.JSON)
        assert ref.uri == "http://example.com/data"

    def test_valid_https_uri(self):
        ref = DataReference(protocol=Protocol.HTTPS, uri="https://example.com/data", format=Format.JSON)
        assert ref.uri == "https://example.com/data"

    def test_valid_nfs_uri(self):
        ref = DataReference(protocol=Protocol.NFS, uri="server:/export/path", format=Format.JSON)
        assert ref.uri == "server:/export/path"

    def test_valid_mqtt_uri(self):
        ref = DataReference(protocol=Protocol.MQTT, uri="mqtt://broker/topic", format=Format.JSON)
        assert ref.uri == "mqtt://broker/topic"

    def test_valid_mqtts_uri(self):
        ref = DataReference(protocol=Protocol.MQTT, uri="mqtts://broker/topic", format=Format.JSON)
        assert ref.uri == "mqtts://broker/topic"

    def test_valid_grpc_uri(self):
        ref = DataReference(protocol=Protocol.GRPC, uri="localhost:50051", format=Format.PROTOBUF)
        assert ref.uri == "localhost:50051"

    def test_valid_villas_uri(self):
        ref = DataReference(protocol=Protocol.VILLAS, uri="villas://node/signal", format=Format.BINARY)
        assert ref.uri == "villas://node/signal"

    def test_valid_inline_uri(self):
        data = b"test data"
        encoded = base64.b64encode(data).decode("ascii")
        ref = DataReference(protocol=Protocol.INLINE, uri=encoded, format=Format.BINARY)
        assert ref.uri == encoded


class TestDataReferenceImmutability:
    """Tests for DataReference immutability."""

    def test_cannot_modify_protocol(self):
        ref = DataReference(protocol=Protocol.S3, uri="s3://bucket/key", format=Format.JSON)
        with pytest.raises(ValidationError):
            ref.protocol = Protocol.HTTP

    def test_cannot_modify_uri(self):
        ref = DataReference(protocol=Protocol.S3, uri="s3://bucket/key", format=Format.JSON)
        with pytest.raises(ValidationError):
            ref.uri = "s3://other/key"


class TestDataReferenceSerialization:
    """Tests for DataReference serialization."""

    def test_model_dump_minimal(self):
        ref = DataReference(protocol=Protocol.S3, uri="s3://bucket/key", format=Format.JSON)
        d = ref.model_dump()
        assert d["protocol"] == Protocol.S3
        assert d["uri"] == "s3://bucket/key"
        assert d["format"] == Format.JSON

    def test_model_dump_full(self):
        ref = DataReference(
            protocol=Protocol.S3,
            uri="s3://bucket/key",
            format=Format.JSON,
            schema_uri="urn:schema:v1",
            size_bytes=1024,
            checksum="sha256:abc",
            metadata={"tag": "test"},
        )
        d = ref.model_dump()
        assert d["schema_uri"] == "urn:schema:v1"
        assert d["size_bytes"] == 1024
        assert d["checksum"] == "sha256:abc"
        assert d["metadata"] == {"tag": "test"}

    def test_model_validate_minimal(self):
        data = {"protocol": "s3", "uri": "s3://bucket/key", "format": "json"}
        ref = DataReference.model_validate(data)
        assert ref.protocol == Protocol.S3
        assert ref.uri == "s3://bucket/key"
        assert ref.format == Format.JSON

    def test_roundtrip(self):
        original = DataReference(
            protocol=Protocol.HTTPS,
            uri="https://api.example.com/data",
            format=Format.PARQUET,
            size_bytes=2048,
            checksum="md5:xyz",
        )
        restored = DataReference.model_validate(original.model_dump())
        assert original == restored

    def test_missing_protocol_raises(self):
        with pytest.raises(ValidationError):
            DataReference.model_validate({"uri": "s3://b/k", "format": "json"})

    def test_invalid_protocol_raises(self):
        with pytest.raises(ValidationError):
            DataReference.model_validate({"protocol": "ftp", "uri": "ftp://host", "format": "json"})


class TestDataReferenceEquality:
    """Tests for DataReference equality."""

    def test_equal_references(self):
        ref1 = DataReference(protocol=Protocol.S3, uri="s3://bucket/key", format=Format.JSON)
        ref2 = DataReference(protocol=Protocol.S3, uri="s3://bucket/key", format=Format.JSON)
        assert ref1 == ref2

    def test_unequal_protocol(self):
        ref1 = DataReference(protocol=Protocol.S3, uri="s3://bucket/key", format=Format.JSON)
        encoded = base64.b64encode(b"data").decode("ascii")
        ref2 = DataReference(protocol=Protocol.INLINE, uri=encoded, format=Format.JSON)
        assert ref1 != ref2

    def test_unequal_uri(self):
        ref1 = DataReference(protocol=Protocol.S3, uri="s3://bucket/key1", format=Format.JSON)
        ref2 = DataReference(protocol=Protocol.S3, uri="s3://bucket/key2", format=Format.JSON)
        assert ref1 != ref2

    def test_not_hashable_with_dict_metadata(self):
        ref = DataReference(protocol=Protocol.S3, uri="s3://bucket/key", format=Format.JSON)
        with pytest.raises(TypeError):
            hash(ref)


class TestDataReferenceInlineData:
    """Tests for inline data handling."""

    def test_from_inline_data(self):
        data = b"test content"
        ref = DataReference.from_inline_data(data, Format.BINARY)
        assert ref.protocol == Protocol.INLINE
        assert ref.format == Format.BINARY
        assert ref.size_bytes == len(data)

    def test_from_inline_data_with_metadata(self):
        data = b"test"
        ref = DataReference.from_inline_data(data, Format.JSON, metadata={"key": "value"})
        assert ref.metadata == {"key": "value"}

    def test_get_inline_data(self):
        original = b"test content"
        ref = DataReference.from_inline_data(original, Format.BINARY)
        retrieved = ref.get_inline_data()
        assert retrieved == original

    def test_get_inline_data_wrong_protocol_raises(self):
        ref = DataReference(protocol=Protocol.S3, uri="s3://bucket/key", format=Format.JSON)
        with pytest.raises(ValueError, match="only valid for INLINE protocol"):
            ref.get_inline_data()

    def test_roundtrip_binary_data(self):
        original = bytes(range(256))
        ref = DataReference.from_inline_data(original, Format.BINARY)
        retrieved = ref.get_inline_data()
        assert retrieved == original
