"""The Redis benchmark itself, run small on fakeredis.

The numbers mean nothing here; what is checked is that every scenario runs,
that its outcome checks hold, and that retries are counted - so the benchmark
keeps working as the engine changes.
"""

import fakeredis
import pytest
from redis import WatchError

from tests.benchmark.redis_transactions import (
    Settings,
    TransactionStats,
    count_transactions,
    main,
    render,
    run,
)

SMALL = Settings(
    rounds=4, sizes=(2, 5), widths=(2, 4), workers=(1, 3), workflows=3, claim_tasks=6
)


@pytest.fixture
def connect():
    server = fakeredis.FakeServer()
    return lambda: fakeredis.FakeRedis(server=server)


def test_every_scenario_runs_and_leaves_the_database_empty(connect):
    report = run(connect, SMALL)

    assert set(report["transitions"]) == {"2", "5"}
    for transitions in report["transitions"].values():
        assert set(transitions) == {
            "initialize", "start", "claim", "renew lease", "poll, nothing due", "defer",
            "release due", "fail", "resume", "complete", "complete (last task)",
        }
        assert all(row["n"] >= SMALL.rounds for row in transitions.values())

    # one completion per predecessor, one claim per task, in every round
    assert [row["transactions"] for row in report["fan_in"]] == [2, 4]
    # a claim on the emptied queue ends each worker's loop
    assert [row["transactions"] for row in report["claim_race"]] == [6 + 1, 6 + 3]
    for row in report["worker_pool"]:
        assert row["tasks"] == 3 * SMALL.chain_size
        assert row["transactions"]["complete"]["transactions"] == row["tasks"]

    assert connect().dbsize() == 0
    assert "Fan-in" in render(report)


def test_a_database_with_data_is_refused_and_left_alone(connect):
    connect().set("workflow:wf-1", "somebody's workflow")

    with pytest.raises(RuntimeError, match="not empty"):
        run(connect, SMALL)

    assert connect().get("workflow:wf-1") == b"somebody's workflow"


def test_retries_are_counted_per_transaction(connect):
    stats = TransactionStats()
    client = count_transactions(connect(), stats)
    calls = []

    def flaky(pipe):
        calls.append(1)
        if len(calls) < 3:
            raise WatchError()
        pipe.multi()
        pipe.set("k", "v")

    client.transaction(flaky, "k")
    client.transaction(lambda pipe: None)

    assert stats.summary()["flaky"] == {
        "transactions": 1, "retries": 2, "retry_rate": 2.0, "worst_attempts": 3,
    }
    assert stats.summary()["<lambda>"]["retries"] == 0


def test_persistence_cannot_be_chosen_for_a_given_redis():
    with pytest.raises(SystemExit):
        main(["--redis-url", "redis://localhost:1/0", "--persistence", "always"])
