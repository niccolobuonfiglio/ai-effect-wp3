"""
Run with any pytest: ``python -m pytest evaluation/test_sweeps.py``.
"""

import base64
import hashlib
import importlib.util
import json
import sys
import types
from argparse import Namespace
from pathlib import Path

import pytest

from runner import inline_config_input, with_inline_size, with_records

PROVIDER = (Path(__file__).resolve().parent.parent / "use-cases" / "file_based_energy_pipeline"
            / "services" / "input_provider" / "service.py")


def decoded(ref: dict) -> bytes:
    return base64.b64decode(ref["uri"])


@pytest.mark.parametrize("size", [64, 1024, 65536, 65537, 1 << 20])
def test_inline_input_has_exactly_the_requested_size_and_a_correct_checksum(size):
    ref = inline_config_input({"num_records": 10, "output_format": "csv"}, size)
    data = decoded(ref)
    assert len(data) == size == ref["size_bytes"]
    assert ref["checksum"] == "sha256:" + hashlib.sha256(data).hexdigest()
    assert json.loads(data)["num_records"] == 10


def test_inline_input_smaller_than_its_config_is_refused():
    with pytest.raises(ValueError):
        inline_config_input({"num_records": 10, "output_format": "csv"}, 10)


def test_size_zero_submits_no_input():
    payload = {"blueprint": {}, "dockerinfo": {}}
    assert "inputs" not in with_inline_size(payload, None, Namespace(size=0))
    assert len(with_inline_size(payload, None, Namespace(size=2048))["inputs"]) == 1


def test_records_input_asks_for_that_many_records():
    payload = {"blueprint": {}, "dockerinfo": {}}
    (ref,) = with_records(payload, None, Namespace(records=1000))["inputs"]
    assert json.loads(decoded(ref)) == {"num_records": 1000, "output_format": "csv"}


@pytest.fixture(scope="module")
def provider():
    common = types.ModuleType("common")
    sequential = types.ModuleType("common.sequential")
    for name in ("DataReference", "ExecuteRequest", "ExecuteResponse"):
        setattr(sequential, name, lambda **kw: types.SimpleNamespace(**kw))
    sequential.run = lambda module: None
    sys.modules.update({"common": common, "common.sequential": sequential})
    spec = importlib.util.spec_from_file_location("input_provider_service", PROVIDER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    yield module
    for name in ("common", "common.sequential"):
        sys.modules.pop(name, None)


def configured(provider, inputs) -> dict:
    response = provider.execute_GetConfiguration(types.SimpleNamespace(inputs=inputs))
    return json.loads(base64.b64decode(response.output.uri))


def test_provider_without_input_is_unchanged(provider):
    assert configured(provider, []) == {"num_records": 10, "output_format": "csv"}


def test_provider_honours_num_records(provider):
    ref = inline_config_input({"num_records": 1000, "output_format": "csv"})
    assert configured(provider, [ref])["num_records"] == 1000


def test_provider_ignores_padding_and_other_inputs(provider):
    padded = inline_config_input({"num_records": 10, "output_format": "csv"}, 4096)
    file_ref = {"protocol": "file", "uri": "/tmp/x.csv", "format": "csv"}
    assert configured(provider, [file_ref, padded])["num_records"] == 10
    assert configured(provider, [file_ref])["num_records"] == 10


@pytest.mark.parametrize("value", [0, -5, 10**7, "100", True, None])
def test_provider_falls_back_to_the_default_on_invalid_values(provider, value):
    ref = inline_config_input({"num_records": value})
    assert configured(provider, [ref])["num_records"] == 10
