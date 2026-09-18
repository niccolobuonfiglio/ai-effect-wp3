"""System tests run against the live Docker stacks and are opt-in.

They start and stop real containers, so they only run when
RUN_SYSTEM_TESTS=1 is set (``./run-tests-system.sh`` does this). Everywhere
else - ``pytest tests/``, ``./run-tests.sh`` - they are reported as skipped.
"""

import os

import pytest


def pytest_collection_modifyitems(config, items):
    if os.environ.get("RUN_SYSTEM_TESTS") == "1":
        return
    skip = pytest.mark.skip(reason="system test: set RUN_SYSTEM_TESTS=1 or use ./run-tests-system.sh")
    for item in items:
        if "tests/system" in str(item.fspath).replace("\\", "/"):
            item.add_marker(skip)
