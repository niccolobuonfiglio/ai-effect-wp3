"""Tests for the test-service helpers themselves.

The readiness check must reject a *different* server on the port; before this,
a use-case container publishing the same port silently answered the tests.
"""

import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "fixtures"))
from test_service import (  # noqa: E402
    ControlServiceRunner,
    TEST_SERVICE_MARKER,
    free_port,
    health_body,
    wait_until_ready,
)


class _ForeignHandler(BaseHTTPRequestHandler):
    """Stands in for another service: answers /health without the marker."""

    def do_GET(self):  # noqa: N802
        body = b'{"status": "ok"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def foreign_server():
    server = HTTPServer(("127.0.0.1", 0), _ForeignHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()


def test_free_port_is_bindable():
    import socket

    port = free_port()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", port))


def test_health_body_carries_marker():
    assert health_body()["service"] == TEST_SERVICE_MARKER


def test_readiness_rejects_a_foreign_server(foreign_server):
    port = foreign_server.server_address[1]

    with pytest.raises(RuntimeError, match="different server"):
        wait_until_ready(f"http://127.0.0.1:{port}", timeout=2)


# The runner's server thread fails to bind the taken port; that failure is the
# scenario under test, so its thread-exception warning is expected here.
@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_runner_on_a_taken_port_fails_loudly(foreign_server):
    runner = ControlServiceRunner(port=foreign_server.server_address[1])

    with pytest.raises(RuntimeError, match="different server"):
        runner.start()


def test_readiness_times_out_when_nothing_answers():
    with pytest.raises(RuntimeError, match="did not start"):
        wait_until_ready(f"http://127.0.0.1:{free_port()}", timeout=0.5)


def test_runner_default_uses_a_free_port():
    runner = ControlServiceRunner()
    runner.start()
    try:
        assert runner.port not in (18080, 18081, 18082)
    finally:
        runner.stop()
