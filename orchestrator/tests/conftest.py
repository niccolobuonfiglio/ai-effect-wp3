"""Shared pytest configuration.

Puts ``src`` on the import path so tests import ``services.*`` and ``api.*``
the same way the application does.
"""

import os
import sys

import pytest

SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)


@pytest.fixture(autouse=True)
def _no_api_key_from_shell(request, monkeypatch):
    """Ignore an ORCHESTRATOR_API_KEY exported in the developer's shell.

    The in-process API would otherwise answer 401 to tests that send no token.
    System tests run against a live stack and keep the key.
    """
    if "system" not in request.node.path.parts:
        monkeypatch.delenv("ORCHESTRATOR_API_KEY", raising=False)
