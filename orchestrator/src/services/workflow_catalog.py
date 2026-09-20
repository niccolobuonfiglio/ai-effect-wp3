"""Catalog of submitted workflows, for listing them and drawing their graph.

The engine keeps each workflow's state and tasks but has no way to enumerate
workflows, and it does not keep the blueprint (only a node-to-task mapping).
The dashboard needs both, so the API records them here at submission time.
"""

import json

from redis import Redis

WORKFLOWS_KEY = "workflows"


class WorkflowCatalog:
    """Workflow ids and their blueprints, stored in Redis."""

    def __init__(self, redis_client: Redis):
        if redis_client is None:
            raise ValueError("redis_client is required")
        self._redis = redis_client

    @staticmethod
    def _blueprint_key(workflow_id: str) -> str:
        return f"blueprint:{workflow_id}"

    @staticmethod
    def _require(workflow_id: str) -> None:
        if not workflow_id or not workflow_id.strip():
            raise ValueError("workflow_id is required")

    def register(self, workflow_id: str, blueprint: dict) -> None:
        """Record a workflow and the blueprint it was submitted with."""
        self._require(workflow_id)
        if blueprint is None:
            raise ValueError("blueprint is required")
        self._redis.set(self._blueprint_key(workflow_id), json.dumps(blueprint))
        self._redis.sadd(WORKFLOWS_KEY, workflow_id)

    def forget(self, workflow_id: str) -> None:
        """Remove a workflow from the catalog."""
        self._require(workflow_id)
        self._redis.delete(self._blueprint_key(workflow_id))
        self._redis.srem(WORKFLOWS_KEY, workflow_id)

    def ids(self) -> list[str]:
        """All recorded workflow ids (unordered)."""
        return [
            wid.decode("utf-8") if isinstance(wid, bytes) else wid
            for wid in self._redis.smembers(WORKFLOWS_KEY) or set()
        ]

    def blueprint(self, workflow_id: str) -> dict | None:
        """The stored blueprint, or None if missing or unreadable."""
        self._require(workflow_id)
        raw = self._redis.get(self._blueprint_key(workflow_id))
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            data = json.loads(raw)
        except (ValueError, TypeError):
            return None
        return data if isinstance(data, dict) else None
