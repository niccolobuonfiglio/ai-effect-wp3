"""Unit tests for the workflow catalog."""

import fakeredis
import pytest

from services.workflow_catalog import WORKFLOWS_KEY, WorkflowCatalog

BLUEPRINT = {"name": "Demo", "nodes": [{"container_name": "a"}]}


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def catalog(redis_client):
    return WorkflowCatalog(redis_client)


class TestWorkflowCatalog:
    def test_register_and_read_back(self, catalog):
        catalog.register("wf-1", BLUEPRINT)

        assert catalog.ids() == ["wf-1"]
        assert catalog.blueprint("wf-1") == BLUEPRINT

    def test_ids_lists_every_workflow(self, catalog):
        for wf in ("wf-1", "wf-2", "wf-3"):
            catalog.register(wf, BLUEPRINT)

        assert sorted(catalog.ids()) == ["wf-1", "wf-2", "wf-3"]

    def test_forget_removes_id_and_blueprint(self, catalog, redis_client):
        catalog.register("wf-1", BLUEPRINT)

        catalog.forget("wf-1")

        assert catalog.ids() == []
        assert catalog.blueprint("wf-1") is None
        assert redis_client.get("blueprint:wf-1") is None

    def test_unknown_blueprint_is_none(self, catalog):
        assert catalog.blueprint("wf-missing") is None

    def test_corrupt_blueprint_is_none(self, catalog, redis_client):
        redis_client.set("blueprint:wf-bad", "not json")
        assert catalog.blueprint("wf-bad") is None

    def test_non_object_blueprint_is_none(self, catalog, redis_client):
        redis_client.set("blueprint:wf-list", "[1, 2]")
        assert catalog.blueprint("wf-list") is None

    def test_works_with_bytes_client(self):
        catalog = WorkflowCatalog(fakeredis.FakeRedis())
        catalog.register("wf-1", BLUEPRINT)

        assert catalog.ids() == ["wf-1"]
        assert catalog.blueprint("wf-1") == BLUEPRINT

    def test_uses_documented_key(self, catalog, redis_client):
        catalog.register("wf-1", BLUEPRINT)
        assert redis_client.sismember(WORKFLOWS_KEY, "wf-1")

    def test_validation(self, catalog):
        with pytest.raises(ValueError, match="redis_client"):
            WorkflowCatalog(None)
        with pytest.raises(ValueError, match="workflow_id"):
            catalog.register("", BLUEPRINT)
        with pytest.raises(ValueError, match="blueprint"):
            catalog.register("wf-1", None)
        with pytest.raises(ValueError, match="workflow_id"):
            catalog.forget(" ")
