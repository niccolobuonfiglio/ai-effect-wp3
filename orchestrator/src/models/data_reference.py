"""Data reference model for protocol-agnostic data location."""

import base64
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class Protocol(str, Enum):
    """Supported data transfer protocols."""

    S3 = "s3"
    HTTP = "http"
    HTTPS = "https"
    NFS = "nfs"
    GRPC = "grpc"
    MQTT = "mqtt"
    VILLAS = "villas"
    INLINE = "inline"
    FILE = "file"  # local file path


class Format(str, Enum):
    """Supported data serialization formats."""

    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"
    PROTOBUF = "protobuf"
    BINARY = "binary"
    XML = "xml"


class DataReference(BaseModel):
    """Protocol-agnostic reference to data location."""

    model_config = ConfigDict(frozen=True)

    protocol: Protocol
    uri: str
    format: Format | str  # str allowed for GRPC (protobuf message type name)
    schema_uri: str | None = None
    size_bytes: int | None = None
    checksum: str | None = None
    metadata: dict[str, Any] = {}

    @field_validator("uri")
    @classmethod
    def validate_uri_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("uri must not be empty")
        return v

    @field_validator("size_bytes")
    @classmethod
    def validate_size_bytes(cls, v: int | None) -> int | None:
        if v is not None and v < 0:
            raise ValueError("size_bytes must be non-negative")
        return v

    @field_validator("checksum")
    @classmethod
    def validate_checksum(cls, v: str | None) -> str | None:
        if v is not None:
            if ":" not in v:
                raise ValueError("checksum must be algorithm:value format")
            alg, val = v.split(":", 1)
            if not alg or not val:
                raise ValueError("checksum algorithm and value required")
        return v

    @model_validator(mode="after")
    def validate_format_for_protocol(self) -> "DataReference":
        """Validate format matches protocol requirements."""
        if self.protocol != Protocol.GRPC and isinstance(self.format, str):
            # For non-GRPC protocols, format must be a valid Format enum
            try:
                Format(self.format)
            except ValueError:
                valid = ", ".join(f.value for f in Format)
                raise ValueError(
                    f"format must be one of: {valid} (or any string for GRPC protocol)"
                )
        return self

    @model_validator(mode="after")
    def validate_uri_for_protocol(self) -> "DataReference":
        """Validate URI format matches protocol requirements."""
        uri = self.uri
        protocol = self.protocol

        if protocol == Protocol.S3:
            if not uri.startswith("s3://"):
                raise ValueError("S3 URI must start with s3://")
        elif protocol == Protocol.HTTP:
            if not uri.startswith("http://"):
                raise ValueError("HTTP URI must start with http://")
        elif protocol == Protocol.HTTPS:
            if not uri.startswith("https://"):
                raise ValueError("HTTPS URI must start with https://")
        elif protocol == Protocol.NFS:
            if ":" not in uri:
                raise ValueError("NFS URI must be host:path format")
        elif protocol == Protocol.MQTT:
            if not uri.startswith(("mqtt://", "mqtts://")):
                raise ValueError("MQTT URI must start with mqtt:// or mqtts://")
        elif protocol == Protocol.INLINE:
            try:
                base64.b64decode(uri, validate=True)
            except Exception as e:
                raise ValueError("INLINE uri must be valid base64") from e

        return self

    @classmethod
    def from_inline_data(
        cls, data: bytes, format: Format, **kwargs: Any
    ) -> "DataReference":
        """Create DataReference with inline base64-encoded data."""
        return cls(
            protocol=Protocol.INLINE,
            uri=base64.b64encode(data).decode("ascii"),
            format=format,
            size_bytes=len(data),
            **kwargs,
        )

    def get_inline_data(self) -> bytes:
        """Extract inline data. Only valid for INLINE protocol."""
        if self.protocol != Protocol.INLINE:
            raise ValueError("get_inline_data only valid for INLINE protocol")
        return base64.b64decode(self.uri)
