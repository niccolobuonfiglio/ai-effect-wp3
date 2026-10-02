"""The engine state machine of ``tests/property``, on a real Redis.

Same steps and same properties; what differs is the server that decides
whether a transaction's ``WATCH`` was violated. fakeredis aborts a transaction
when a watched key was merely written to (``SADD`` of a member already there);
Redis aborts only when the key changed. On fakeredis the race steps are
therefore protected by more watches than the engine really has, and a missing
``WATCH`` can go unnoticed. Here it cannot.
"""

import pytest
from hypothesis.stateful import run_state_machine_as_test
from redis import Redis
from testcontainers.redis import RedisContainer

from tests.property.test_engine_state_machine import SETTINGS, EngineMachine


@pytest.fixture(scope="module")
def redis_url():
    with RedisContainer() as container:
        host, port = container.get_container_host_ip(), container.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"


def test_engine_state_machine_on_real_redis(redis_url):
    class OnRealRedis(EngineMachine):
        def __init__(self):
            self._connections = []
            super().__init__()
            self.redis.flushdb()

        def connect(self) -> Redis:
            self._connections.append(Redis.from_url(redis_url, decode_responses=True))
            return self._connections[-1]

        def teardown(self):
            try:
                super().teardown()
            finally:
                for connection in self._connections:
                    connection.close()

    run_state_machine_as_test(OnRealRedis, settings=SETTINGS)
