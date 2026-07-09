"""Integration tests for BlueprintParser."""

import json
import tempfile
from pathlib import Path

import pytest
from redis import Redis
from testcontainers.redis import RedisContainer

from services.blueprint_parser import BlueprintParser
from services.state_store import RedisStateStore
from services.task_queue import RedisTaskQueue
from services.workflow_engine import WorkflowEngine


def create_test_blueprint() -> dict:
    """Create a test blueprint similar to real AI-Effect format."""
    return {
        "name": "Test Energy Pipeline",
        "pipeline_id": "test-pipeline-123",
        "creation_date": "2025-01-01 12:00:00",
        "type": "pipeline-topology/v2",
        "version": "2.0",
        "nodes": [
            {
                "container_name": "data_generator1",
                "proto_uri": "microservice/data_generator1.proto",
                "image": "energy-pipeline-data-generator:latest",
                "node_type": "MLModel",
                "operation_signature_list": [
                    {
                        "operation_signature": {
                            "operation_name": "GenerateData",
                            "input_message_name": "GenerateDataRequest",
                            "output_message_name": "GenerateDataResponse",
                            "input_message_stream": False,
                            "output_message_stream": False,
                        },
                        "connected_to": [
                            {
                                "container_name": "data_analyzer1",
                                "operation_signature": {
                                    "operation_name": "AnalyzeData"
                                },
                            }
                        ],
                    }
                ],
            },
            {
                "container_name": "data_analyzer1",
                "proto_uri": "microservice/data_analyzer1.proto",
                "image": "energy-pipeline-data-analyzer:latest",
                "node_type": "MLModel",
                "operation_signature_list": [
                    {
                        "operation_signature": {
                            "operation_name": "AnalyzeData",
                            "input_message_name": "AnalyzeDataRequest",
                            "output_message_name": "AnalyzeDataResponse",
                            "input_message_stream": False,
                            "output_message_stream": False,
                        },
                        "connected_to": [
                            {
                                "container_name": "report_generator1",
                                "operation_signature": {
                                    "operation_name": "GenerateReport"
                                },
                            }
                        ],
                    }
                ],
            },
            {
                "container_name": "report_generator1",
                "proto_uri": "microservice/report_generator1.proto",
                "image": "energy-pipeline-report-generator:latest",
                "node_type": "MLModel",
                "operation_signature_list": [
                    {
                        "operation_signature": {
                            "operation_name": "GenerateReport",
                            "input_message_name": "GenerateReportRequest",
                            "output_message_name": "GenerateReportResponse",
                            "input_message_stream": False,
                            "output_message_stream": False,
                        },
                        "connected_to": [],
                    }
                ],
            },
        ],
    }


@pytest.fixture
def parser():
    return BlueprintParser()


@pytest.fixture(scope="module")
def redis_container():
    with RedisContainer() as container:
        yield container


@pytest.fixture
def redis_client(redis_container):
    client = Redis(
        host=redis_container.get_container_host_ip(),
        port=redis_container.get_exposed_port(6379),
        decode_responses=False,
    )
    yield client
    client.flushdb()
    client.close()


@pytest.fixture
def engine(redis_client):
    state_store = RedisStateStore(redis_client)
    task_queue = RedisTaskQueue(redis_client)
    return WorkflowEngine(state_store, task_queue, redis_client)


class TestParseRealBlueprint:
    """Tests for parsing real blueprint files."""

    def test_parse_test_blueprint(self, parser):
        """Parse test blueprint matching AI-Effect format."""
        data = create_test_blueprint()
        graph = parser.parse_json(data)

        assert len(graph.all_nodes) == 3
        assert len(graph.start_nodes) == 1
        assert graph.start_nodes[0].key == "data_generator1:GenerateData"

        # Verify chain structure
        gen = graph.all_nodes["data_generator1:GenerateData"]
        ana = graph.all_nodes["data_analyzer1:AnalyzeData"]
        rep = graph.all_nodes["report_generator1:GenerateReport"]

        assert gen.next_nodes[0].key == "data_analyzer1:AnalyzeData"
        assert ana.next_nodes[0].key == "report_generator1:GenerateReport"
        assert len(rep.next_nodes) == 0

    def test_parse_blueprint_from_file(self, parser):
        """Parse blueprint from temporary file."""
        data = create_test_blueprint()

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(data, f)
            temp_path = f.name

        try:
            graph = parser.parse_file(temp_path)
            assert len(graph.all_nodes) == 3
        finally:
            Path(temp_path).unlink()


class TestWorkflowEngineIntegration:
    """Tests for integration with WorkflowEngine."""

    def test_parsed_graph_works_with_engine(self, parser, engine):
        """Parsed graph can be used with WorkflowEngine."""
        data = create_test_blueprint()
        graph = parser.parse_json(data)

        # Initialize and start workflow
        engine.initialize_workflow("wf-blueprint-test", graph)
        engine.start_workflow("wf-blueprint-test")

        # Execute all tasks in order
        task1 = engine.claim_task("wf-blueprint-test", timeout=1)
        assert task1.node_key == "data_generator1:GenerateData"
        engine.complete_task("wf-blueprint-test", task1.task_id)

        task2 = engine.claim_task("wf-blueprint-test", timeout=1)
        assert task2.node_key == "data_analyzer1:AnalyzeData"
        engine.complete_task("wf-blueprint-test", task2.task_id)

        task3 = engine.claim_task("wf-blueprint-test", timeout=1)
        assert task3.node_key == "report_generator1:GenerateReport"
        engine.complete_task("wf-blueprint-test", task3.task_id)

        assert engine.is_workflow_complete("wf-blueprint-test")

    def test_complex_blueprint_with_engine(self, parser, engine):
        """Complex blueprint with fan-out/fan-in works with engine."""
        data = {
            "name": "Complex Pipeline",
            "pipeline_id": "complex-123",
            "creation_date": "2025-01-01",
            "type": "pipeline-topology/v2",
            "version": "2.0",
            "nodes": [
                {
                    "container_name": "start",
                    "proto_uri": "start.proto",
                    "image": "start:latest",
                    "node_type": "MLModel",
                    "operation_signature_list": [
                        {
                            "operation_signature": {
                                "operation_name": "Start",
                                "input_message_name": "Input",
                                "output_message_name": "Output",
                            },
                            "connected_to": [
                                {
                                    "container_name": "branch-a",
                                    "operation_signature": {"operation_name": "ProcessA"},
                                },
                                {
                                    "container_name": "branch-b",
                                    "operation_signature": {"operation_name": "ProcessB"},
                                },
                            ],
                        }
                    ],
                },
                {
                    "container_name": "branch-a",
                    "proto_uri": "a.proto",
                    "image": "a:latest",
                    "node_type": "MLModel",
                    "operation_signature_list": [
                        {
                            "operation_signature": {
                                "operation_name": "ProcessA",
                                "input_message_name": "Input",
                                "output_message_name": "Output",
                            },
                            "connected_to": [
                                {
                                    "container_name": "end",
                                    "operation_signature": {"operation_name": "End"},
                                }
                            ],
                        }
                    ],
                },
                {
                    "container_name": "branch-b",
                    "proto_uri": "b.proto",
                    "image": "b:latest",
                    "node_type": "MLModel",
                    "operation_signature_list": [
                        {
                            "operation_signature": {
                                "operation_name": "ProcessB",
                                "input_message_name": "Input",
                                "output_message_name": "Output",
                            },
                            "connected_to": [
                                {
                                    "container_name": "end",
                                    "operation_signature": {"operation_name": "End"},
                                }
                            ],
                        }
                    ],
                },
                {
                    "container_name": "end",
                    "proto_uri": "end.proto",
                    "image": "end:latest",
                    "node_type": "MLModel",
                    "operation_signature_list": [
                        {
                            "operation_signature": {
                                "operation_name": "End",
                                "input_message_name": "Input",
                                "output_message_name": "Output",
                            },
                            "connected_to": [],
                        }
                    ],
                },
            ],
        }

        graph = parser.parse_json(data)
        engine.initialize_workflow("wf-complex", graph)
        engine.start_workflow("wf-complex")

        # Start task
        task_start = engine.claim_task("wf-complex", timeout=1)
        assert task_start.node_key == "start:Start"
        engine.complete_task("wf-complex", task_start.task_id)

        # Both branches now ready
        task_a = engine.claim_task("wf-complex", timeout=1)
        task_b = engine.claim_task("wf-complex", timeout=1)
        engine.complete_task("wf-complex", task_a.task_id)
        engine.complete_task("wf-complex", task_b.task_id)

        # End task ready after both branches
        task_end = engine.claim_task("wf-complex", timeout=1)
        assert task_end.node_key == "end:End"
        engine.complete_task("wf-complex", task_end.task_id)

        assert engine.is_workflow_complete("wf-complex")
