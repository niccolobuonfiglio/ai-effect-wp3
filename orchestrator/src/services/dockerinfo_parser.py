"""Parser for AI-Effect dockerinfo.json files."""

import json
import os
from pathlib import Path

from pydantic import BaseModel, ConfigDict, field_validator

SCHEMES = ("http", "https")
DEFAULT_SCHEME = "http"


class DockerInfoParseError(Exception):
    """Raised when dockerinfo parsing fails."""

    pass


class ServiceEndpoint(BaseModel):
    """Network endpoint for a service."""

    model_config = ConfigDict(frozen=True)

    address: str
    port: int
    scheme: str = DEFAULT_SCHEME

    @property
    def base_url(self) -> str:
        """URL the orchestrator calls this service on."""
        return f"{self.scheme}://{self.address}:{self.port}"

    @field_validator("scheme")
    @classmethod
    def scheme_supported(cls, v: str) -> str:
        if v not in SCHEMES:
            raise ValueError(f"scheme must be one of: {', '.join(SCHEMES)}")
        return v

    @field_validator("address")
    @classmethod
    def address_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("address is required")
        return v

    @field_validator("port")
    @classmethod
    def port_valid(cls, v: int) -> int:
        if v <= 0 or v > 65535:
            raise ValueError("port must be between 1 and 65535")
        return v


class DockerInfoEntry(BaseModel):
    """Entry in docker_info_list.

    Unknown fields are ignored rather than rejected: an entry may carry
    metadata the orchestrator does not use, such as ``webui_port`` /
    ``webui_url``, which the portal reads to link to a service's web UI.
    """

    model_config = ConfigDict(extra="ignore")

    container_name: str
    ip_address: str
    port: str
    scheme: str | None = None

    @field_validator("container_name")
    @classmethod
    def container_name_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("container_name is required")
        return v

    @field_validator("ip_address")
    @classmethod
    def ip_address_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("ip_address is required")
        return v

    @field_validator("port")
    @classmethod
    def port_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("port is required")
        return v


class DockerInfoSchema(BaseModel):
    """Top-level dockerinfo.json structure."""

    model_config = ConfigDict(extra="forbid")

    docker_info_list: list[DockerInfoEntry]

    @field_validator("docker_info_list")
    @classmethod
    def docker_info_list_not_empty(cls, v: list) -> list:
        if not v:
            raise ValueError("docker_info_list is required")
        return v


class DockerInfoParser:
    """Parses dockerinfo.json into service endpoint mapping.

    Args:
        default_scheme: Scheme for entries that do not name one. Defaults to
            the SERVICE_SCHEME environment variable, then "http". Set it to
            "https" when the services are served behind TLS.
    """

    def __init__(self, default_scheme: str | None = None):
        scheme = default_scheme or os.environ.get("SERVICE_SCHEME") or DEFAULT_SCHEME
        if scheme not in SCHEMES:
            raise ValueError(f"default_scheme must be one of: {', '.join(SCHEMES)}")
        self._default_scheme = scheme

    def parse_file(self, path: str) -> dict[str, ServiceEndpoint]:
        """Parse dockerinfo from JSON file."""
        if not path:
            raise ValueError("path is required")

        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(f"DockerInfo file not found: {path}")

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise DockerInfoParseError(f"Invalid JSON: {e}")

        return self.parse_json(data)

    def parse_json(self, data: dict) -> dict[str, ServiceEndpoint]:
        """Parse dockerinfo from JSON dict."""
        if data is None:
            raise ValueError("data is required")

        try:
            schema = DockerInfoSchema.model_validate(data)
        except Exception as e:
            raise DockerInfoParseError(f"Invalid dockerinfo structure: {e}")

        endpoints: dict[str, ServiceEndpoint] = {}

        for entry in schema.docker_info_list:
            try:
                port = int(entry.port)
            except ValueError:
                raise DockerInfoParseError(
                    f"Invalid port for {entry.container_name}: {entry.port}"
                )

            try:
                endpoint = ServiceEndpoint(
                    address=entry.ip_address,
                    port=port,
                    scheme=entry.scheme or self._default_scheme,
                )
            except ValueError as e:
                raise DockerInfoParseError(
                    f"Invalid endpoint for {entry.container_name}: {e}"
                )
            endpoints[entry.container_name] = endpoint

        return endpoints
