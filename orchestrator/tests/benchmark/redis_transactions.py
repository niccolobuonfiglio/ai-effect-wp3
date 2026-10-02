"""Micro-benchmark of the orchestrator's Redis transactions.

Every state transition is one ``WATCH``/``MULTI``/``EXEC`` transaction
(docs/SCHEDULING.md section 11). This script measures what that costs and how
often a transaction has to be retried because another worker changed a watched
key first:

* **cost**: latency and number of Redis commands of each transition, run by a
  single client, for workflows of several sizes;
* **fan-in**: all predecessors of a join complete at the same instant;
* **claim race**: several workers claim from one queue;
* **workers**: several workers drive many workflows to completion the way the
  worker daemon does, without the service calls in between - the most Redis
  contention the orchestrator can produce for that number of workers.

The contention scenarios also check their outcome (a join queued once with all
its inputs, every task claimed once, every workflow completed), so a retry that
lost an update fails the run instead of going unnoticed.

Run it with ``./run-benchmark-redis.sh`` (see ``--help``). By default it starts
a throwaway Redis with the production persistence settings; ``--persistence``
selects others to compare. Workers are threads, each with its own connection;
they spend their time waiting for Redis, so they contend like processes do.
"""

import argparse
import json
import sys
import threading
import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from redis import Redis
from redis.exceptions import ResponseError

SRC = Path(__file__).resolve().parents[2] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from models.data_reference import DataReference, Format, Protocol
from models.graph import ExecutionGraph, GraphNode
from models.node import Node, OperationSignature, OperationSignatureList
from models.state import WorkflowStatus
from services.state_store import RedisStateStore
from services.task_queue import RedisTaskQueue
from services.workflow_engine import WorkflowEngine

REDIS_IMAGE = "redis:7-alpine"
# Redis command per persistence mode; "everysec" is what docker-compose.yml runs.
PERSISTENCE = {
    "none": None,  # image defaults: snapshots only
    "everysec": "redis-server --appendonly yes --appendfsync everysec",
    "always": "redis-server --appendonly yes --appendfsync always",
}
# How the transactions of services/ are named in the report.
TRANSACTION_LABELS = {"take": "claim", "move": "release/reclaim"}
THREAD_TIMEOUT = 120

OUTPUT = DataReference(protocol=Protocol.S3, uri="s3://bench/output", format=Format.JSON)

Connect = Callable[[], Redis]


class InvariantViolated(AssertionError):
    """A contention scenario ended in a state the transactions must prevent."""


# ----------------------------------------------------------------------
# counting transactions and their retries
# ----------------------------------------------------------------------
class TransactionStats:
    """Attempts per transaction, by name, from any number of threads."""

    def __init__(self):
        self._lock = threading.Lock()
        self.transactions: dict[str, int] = defaultdict(int)
        self.attempts: dict[str, int] = defaultdict(int)
        self.worst: dict[str, int] = defaultdict(int)

    def record(self, name: str, attempts: int) -> None:
        with self._lock:
            self.transactions[name] += 1
            self.attempts[name] += attempts
            self.worst[name] = max(self.worst[name], attempts)

    def summary(self) -> dict[str, dict]:
        """Per transaction: how many ran, how many retries, the worst case."""
        return {
            name: {
                "transactions": count,
                "retries": self.attempts[name] - count,
                "retry_rate": round((self.attempts[name] - count) / count, 4),
                "worst_attempts": self.worst[name],
            }
            for name, count in sorted(self.transactions.items())
        }


def count_transactions(client: Redis, stats: TransactionStats) -> Redis:
    """Make ``client.transaction`` report its attempts to ``stats``.

    redis-py calls the transaction body again after every ``WatchError``, at
    once and without limit, so the calls of the body are the attempts.
    """
    transaction = client.transaction

    def counting(func, *watches, **kwargs):
        attempts = 0

        def body(pipe):
            nonlocal attempts
            attempts += 1
            return func(pipe)

        try:
            return transaction(body, *watches, **kwargs)
        finally:
            name = func.__name__
            stats.record(TRANSACTION_LABELS.get(name, name), attempts)

    client.transaction = counting
    return client


# ----------------------------------------------------------------------
# workflows and workers
# ----------------------------------------------------------------------
def _graph_node(name: str) -> GraphNode:
    node = Node(
        container_name=name,
        proto_uri=f"{name}.proto",
        image=f"{name}:latest",
        node_type="microservice",
        operation_signature_list=[
            OperationSignatureList(
                operation_signature=OperationSignature(
                    operation_name="Process",
                    input_message_name="Input",
                    output_message_name="Output",
                )
            )
        ],
    )
    return GraphNode(node=node, operation=node.operation_signature_list[0])


def _link(before: GraphNode, after: GraphNode) -> None:
    before.next_nodes.append(after)
    after.dependencies.append(before)


def chain(size: int) -> ExecutionGraph:
    """``size`` tasks one after the other, like the use-case pipelines."""
    graph = ExecutionGraph()
    nodes = [_graph_node(f"step-{i}") for i in range(size)]
    for before, after in zip(nodes, nodes[1:]):
        _link(before, after)
    for node in nodes:
        graph.add_node(node)
    graph.start_nodes.append(nodes[0])
    return graph


def fan_in(width: int) -> ExecutionGraph:
    """``width`` start tasks feeding one join."""
    graph = ExecutionGraph()
    join = _graph_node("join")
    for i in range(width):
        node = _graph_node(f"branch-{i}")
        _link(node, join)
        graph.add_node(node)
        graph.start_nodes.append(node)
    graph.add_node(join)
    return graph


def parallel(count: int) -> ExecutionGraph:
    """``count`` independent tasks, all ready at once."""
    graph = ExecutionGraph()
    for i in range(count):
        node = _graph_node(f"task-{i}")
        graph.add_node(node)
        graph.start_nodes.append(node)
    return graph


@dataclass
class Rig:
    """One process' view of the orchestrator: a connection and what uses it."""

    client: Redis
    store: RedisStateStore
    queue: RedisTaskQueue
    engine: WorkflowEngine


def rig(connect: Connect, stats: TransactionStats | None = None) -> Rig:
    client = connect()
    if stats is not None:
        count_transactions(client, stats)
    store, queue = RedisStateStore(client), RedisTaskQueue(client)
    return Rig(client, store, queue, WorkflowEngine(store, queue, client))


@contextmanager
def rigs(connect: Connect, count: int, stats: TransactionStats):
    """``count`` counted workers, closed afterwards."""
    workers = [rig(connect, stats) for _ in range(count)]
    try:
        yield workers
    finally:
        for worker in workers:
            worker.client.close()


def new_workflow_id() -> str:
    return f"bench-{uuid.uuid4().hex[:12]}"


def run_together(bodies: list[Callable[[], None]]) -> float:
    """Run the bodies in threads released at the same instant; seconds taken."""
    barrier = threading.Barrier(len(bodies) + 1)
    errors: list[BaseException] = []

    def guarded(body):
        try:
            barrier.wait(THREAD_TIMEOUT)
            body()
        except BaseException as e:  # reported by the caller's thread
            errors.append(e)

    threads = [threading.Thread(target=guarded, args=(body,)) for body in bodies]
    for thread in threads:
        thread.start()
    barrier.wait(THREAD_TIMEOUT)
    started = time.perf_counter()
    for thread in threads:
        thread.join(THREAD_TIMEOUT)
    elapsed = time.perf_counter() - started
    if errors:
        raise errors[0]
    if any(thread.is_alive() for thread in threads):
        raise TimeoutError(f"workers still running after {THREAD_TIMEOUT}s")
    return elapsed


def check(condition: bool, message: str) -> None:
    if not condition:
        raise InvariantViolated(message)


# ----------------------------------------------------------------------
# statistics
# ----------------------------------------------------------------------
def percentile(samples: list[float], fraction: float) -> float:
    ordered = sorted(samples)
    return ordered[min(len(ordered) - 1, int(fraction * len(ordered)))]


def latency(samples: list[float]) -> dict:
    """Summary of latencies given in milliseconds."""
    return {
        "n": len(samples),
        "mean_ms": round(sum(samples) / len(samples), 3),
        "p50_ms": round(percentile(samples, 0.50), 3),
        "p95_ms": round(percentile(samples, 0.95), 3),
        "p99_ms": round(percentile(samples, 0.99), 3),
    }


def commands_processed(client: Redis) -> int | None:
    """Commands the server has run so far, not counting this question."""
    try:
        stats = client.info("commandstats")
    except ResponseError:  # fakeredis has no INFO
        return None
    return sum(v["calls"] for k, v in stats.items() if k != "cmdstat_info")


# ----------------------------------------------------------------------
# cost of each transition, without contention
# ----------------------------------------------------------------------
def lifecycle(engine: WorkflowEngine, workflow_id: str, size: int, measure) -> None:
    """One chain workflow through every transition.

    ``measure(label, call)`` runs ``call`` and returns its result. The first
    task is also deferred, released, failed and resumed before it completes.
    """
    measure("initialize", lambda: engine.initialize_workflow(workflow_id, chain(size)))
    measure("start", lambda: engine.start_workflow(workflow_id))
    for position in range(size):
        task = measure("claim", lambda: engine.claim_task(workflow_id))
        measure("renew lease", lambda: engine.renew_lease(workflow_id, task.task_id))
        if position == 0:
            measure("poll, nothing due", lambda: engine.release_due_tasks(workflow_id))
            measure("defer", lambda: engine.defer_task(workflow_id, task.task_id, "benchmark", 0))
            measure("release due", lambda: engine.release_due_tasks(workflow_id))
            task = engine.claim_task(workflow_id)
            measure("fail", lambda: engine.fail_task(workflow_id, task.task_id, "benchmark"))
            measure("resume", lambda: engine.resume_workflow(workflow_id))
            task = engine.claim_task(workflow_id)
        label = "complete (last task)" if position == size - 1 else "complete"
        measure(label, lambda: engine.complete_task(workflow_id, task.task_id, [OUTPUT]))


def transition_cost(connect: Connect, size: int, rounds: int) -> dict[str, dict]:
    """Latency and command count of each transition for ``size``-task workflows."""
    worker = rig(connect)
    try:
        samples: dict[str, list[float]] = defaultdict(list)

        def timed(label, call):
            started = time.perf_counter()
            result = call()
            samples[label].append((time.perf_counter() - started) * 1000)
            return result

        commands: dict[str, int | None] = {}

        def counted(label, call):
            before = commands_processed(worker.client)
            result = call()
            after = commands_processed(worker.client)
            # the last call with a label wins, e.g. "complete" of the task
            # before the last one, which has a dependent to hand over to
            commands[label] = None if before is None else after - before
            return result

        for _ in range(rounds):
            lifecycle(worker.engine, new_workflow_id(), size, timed)
        lifecycle(worker.engine, new_workflow_id(), size, counted)
        return {
            label: latency(values) | {"commands": commands[label]}
            for label, values in samples.items()
        }
    finally:
        worker.client.close()


def round_trip(connect: Connect, rounds: int) -> dict:
    """One command and its answer: the unit the transitions are made of."""
    client = connect()
    try:
        samples = []
        for _ in range(rounds):
            started = time.perf_counter()
            client.get("bench:missing")
            samples.append((time.perf_counter() - started) * 1000)
        return latency(samples)
    finally:
        client.close()


# ----------------------------------------------------------------------
# retries under contention
# ----------------------------------------------------------------------
def fan_in_contention(connect: Connect, width: int, rounds: int) -> dict:
    """All ``width`` predecessors of a join complete at the same instant."""
    stats = TransactionStats()
    setup = rig(connect)
    elapsed = []
    try:
        with rigs(connect, width, stats) as workers:
            for _ in range(rounds):
                wf = new_workflow_id()
                setup.engine.initialize_workflow(wf, fan_in(width))
                setup.engine.start_workflow(wf)
                claimed = [setup.engine.claim_task(wf) for _ in range(width)]

                elapsed.append(1000 * run_together([
                    lambda w=worker, t=task: w.engine.complete_task(wf, t.task_id, [OUTPUT])
                    for worker, task in zip(workers, claimed)
                ]))

                check(setup.queue.queue_length(wf) == 1, f"{wf}: join not queued exactly once")
                join = setup.engine.claim_task(wf)
                check(
                    len(join.input_refs) == width,
                    f"{wf}: join has {len(join.input_refs)} of {width} inputs",
                )
                setup.engine.complete_task(wf, join.task_id)
                check(
                    setup.store.get_workflow(wf).status == WorkflowStatus.COMPLETED,
                    f"{wf}: not completed",
                )
    finally:
        setup.client.close()
    return {"width": width, "rounds": rounds, "round": latency(elapsed)} | stats.summary()["complete"]


def claim_race(connect: Connect, workers: int, tasks: int, rounds: int) -> dict:
    """``workers`` workers empty one queue of ``tasks`` ready tasks."""
    stats = TransactionStats()
    setup = rig(connect)
    elapsed = []
    try:
        with rigs(connect, workers, stats) as pool:
            for _ in range(rounds):
                wf = new_workflow_id()
                setup.engine.initialize_workflow(wf, parallel(tasks))
                setup.engine.start_workflow(wf)
                claimed: list[list[str]] = [[] for _ in pool]

                def drain(worker: Rig, mine: list[str]) -> None:
                    while (task := worker.engine.claim_task(wf)) is not None:
                        mine.append(task.task_id)

                elapsed.append(1000 * run_together([
                    lambda w=worker, m=mine: drain(w, m) for worker, mine in zip(pool, claimed)
                ]))

                everything = [task_id for mine in claimed for task_id in mine]
                check(
                    len(everything) == tasks and len(set(everything)) == tasks,
                    f"{wf}: {len(everything)} claims of {len(set(everything))} "
                    f"distinct tasks, expected {tasks} of {tasks}",
                )
                check(
                    sorted(setup.queue.leased_tasks(wf)) == sorted(everything),
                    f"{wf}: leases do not match the claimed tasks",
                )
    finally:
        setup.client.close()
    return {
        "workers": workers,
        "tasks": tasks,
        "rounds": rounds,
        "round": latency(elapsed),
    } | stats.summary()["claim"]


def worker_pool(connect: Connect, workers: int, workflows: int, size: int) -> dict:
    """``workers`` workers run ``workflows`` chain workflows to completion.

    Each loops as the worker daemon does - over the running workflows:
    release what is due, claim, complete - but the service call between claim
    and complete takes no time, so the workers meet in Redis as often as they
    possibly can.
    """
    stats = TransactionStats()
    setup = rig(connect)
    try:
        ids = [new_workflow_id() for _ in range(workflows)]
        for wf in ids:
            setup.engine.initialize_workflow(wf, chain(size))
            setup.engine.start_workflow(wf)

        with rigs(connect, workers, stats) as pool:
            completed = [0] * workers

            def work(index: int, worker: Rig) -> None:
                while running := worker.store.running_workflow_ids():
                    for wf in running:
                        worker.engine.release_due_tasks(wf)
                        task = worker.engine.claim_task(wf)
                        if task is not None:
                            worker.engine.complete_task(wf, task.task_id, [OUTPUT])
                            completed[index] += 1

            seconds = run_together([
                lambda i=index, w=worker: work(i, w) for index, worker in enumerate(pool)
            ])

        check(
            sum(completed) == workflows * size,
            f"{sum(completed)} task executions for {workflows * size} tasks",
        )
        for wf in ids:
            check(
                setup.store.get_workflow(wf).status == WorkflowStatus.COMPLETED,
                f"{wf}: not completed",
            )
    finally:
        setup.client.close()
    return {
        "workers": workers,
        "workflows": workflows,
        "tasks": workflows * size,
        "seconds": round(seconds, 3),
        "tasks_per_second": round(workflows * size / seconds, 1),
        "transactions": stats.summary(),
    }


# ----------------------------------------------------------------------
# the whole benchmark
# ----------------------------------------------------------------------
@dataclass
class Settings:
    rounds: int = 200
    sizes: tuple[int, ...] = (4, 16, 64)
    widths: tuple[int, ...] = (2, 4, 8, 16)
    workers: tuple[int, ...] = (1, 3, 8)
    workflows: int = 50
    claim_tasks: int = 32
    chain_size: int = 4


def server_settings(client: Redis) -> dict:
    try:
        return {
            "version": client.info("server")["redis_version"],
            "appendonly": client.config_get("appendonly")["appendonly"],
            "appendfsync": client.config_get("appendfsync")["appendfsync"],
        }
    except ResponseError:  # fakeredis
        return {}


def run(connect: Connect, settings: Settings) -> dict:
    """Run every scenario on an empty database, which is emptied again at the end."""
    admin = connect()
    try:
        if admin.dbsize() != 0:
            raise RuntimeError(
                "the Redis database is not empty: the benchmark only runs on a "
                "throwaway database, because it empties it"
            )
        contended = max(1, settings.rounds // 4)
        try:
            report = {"redis": server_settings(admin), "settings": vars(settings).copy()}
            report["round_trip"] = round_trip(connect, settings.rounds)
            report["transitions"] = {}
            for size in settings.sizes:
                report["transitions"][str(size)] = transition_cost(connect, size, settings.rounds)
                admin.flushdb()
            report["fan_in"] = [
                fan_in_contention(connect, width, contended) for width in settings.widths
            ]
            admin.flushdb()
            report["claim_race"] = [
                claim_race(connect, workers, settings.claim_tasks, contended)
                for workers in settings.workers
            ]
            admin.flushdb()
            report["worker_pool"] = []
            for workers in settings.workers:
                report["worker_pool"].append(
                    worker_pool(connect, workers, settings.workflows, settings.chain_size)
                )
                admin.flushdb()
            return report
        finally:
            admin.flushdb()
    finally:
        admin.close()


def render(report: dict) -> str:
    """The report as text, one block per scenario."""
    lines = []
    redis_info = report["redis"]
    if redis_info:
        lines.append(
            f"Redis {redis_info['version']}, appendonly={redis_info['appendonly']}, "
            f"appendfsync={redis_info['appendfsync']}"
        )
    trip = report["round_trip"]
    lines.append(f"One round trip (GET): p50 {trip['p50_ms']} ms, p95 {trip['p95_ms']} ms")

    for size, transitions in report["transitions"].items():
        lines += ["", f"Transitions, one client, workflow of {size} tasks (n={report['settings']['rounds']})"]
        lines.append(f"  {'transition':<22}{'commands':>9}{'p50 ms':>9}{'p95 ms':>9}{'p99 ms':>9}")
        for label, row in transitions.items():
            commands = "-" if row["commands"] is None else row["commands"]
            lines.append(
                f"  {label:<22}{commands:>9}{row['p50_ms']:>9}{row['p95_ms']:>9}{row['p99_ms']:>9}"
            )

    def retries(row: dict) -> str:
        return (
            f"{row['transactions']:>13}{row['retries']:>9}{row['retry_rate']:>13}"
            f"{row['worst_attempts']:>15}"
        )

    header = f"{'transactions':>13}{'retries':>9}{'retries/txn':>13}{'worst attempts':>15}"
    lines += ["", "Fan-in: all predecessors of a join complete at once"]
    lines.append(f"  {'width':<22}{header}{'p50 ms':>9}")
    for row in report["fan_in"]:
        lines.append(f"  {row['width']:<22}{retries(row)}{row['round']['p50_ms']:>9}")

    tasks = report["settings"]["claim_tasks"]
    lines += ["", f"Claim race: workers empty one queue of {tasks} tasks"]
    lines.append(f"  {'workers':<22}{header}{'p50 ms':>9}")
    for row in report["claim_race"]:
        lines.append(f"  {row['workers']:<22}{retries(row)}{row['round']['p50_ms']:>9}")

    lines += ["", "Worker pool: chain workflows run to completion, no service time"]
    for row in report["worker_pool"]:
        lines.append(
            f"  {row['workers']} worker(s), {row['workflows']} workflows, {row['tasks']} tasks: "
            f"{row['seconds']} s, {row['tasks_per_second']} tasks/s"
        )
        lines.append(f"    {'transaction':<20}{header}")
        for name, stats in row["transactions"].items():
            lines.append(f"    {name:<20}{retries(stats)}")
    return "\n".join(lines)


@contextmanager
def throwaway_redis(persistence: str):
    """URL of a Redis container that lives for the duration of the block."""
    from testcontainers.redis import RedisContainer

    container = RedisContainer(REDIS_IMAGE)
    if PERSISTENCE[persistence] is not None:
        container.with_command(PERSISTENCE[persistence])
    with container:
        host, port = container.get_container_host_ip(), container.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"


def _numbers(text: str) -> tuple[int, ...]:
    return tuple(int(part) for part in text.split(","))


def main(argv: list[str] | None = None) -> int:
    defaults = Settings()
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--persistence",
        action="append",
        choices=sorted(PERSISTENCE),
        help="persistence of the throwaway Redis; repeat to compare (default: everysec, "
        "as in docker-compose.yml)",
    )
    parser.add_argument(
        "--redis-url",
        help="use this Redis instead of starting one; its database must be empty "
        "and is emptied again at the end",
    )
    parser.add_argument("--rounds", type=int, default=defaults.rounds,
                        help="samples per transition; the contention scenarios run a quarter")
    parser.add_argument("--sizes", type=_numbers, default=defaults.sizes,
                        help="tasks per workflow for the cost measurement, e.g. 4,16,64")
    parser.add_argument("--widths", type=_numbers, default=defaults.widths,
                        help="fan-in widths, e.g. 2,4,8,16")
    parser.add_argument("--workers", type=_numbers, default=defaults.workers,
                        help="worker counts, e.g. 1,3,8 (docker-compose.yml runs 3)")
    parser.add_argument("--workflows", type=int, default=defaults.workflows,
                        help="workflows in the worker-pool scenario")
    parser.add_argument("--json", type=Path, help="also write the results to this file")
    args = parser.parse_args(argv)
    if args.redis_url and args.persistence:
        parser.error("--persistence configures the throwaway Redis; it cannot be used with --redis-url")

    settings = Settings(
        rounds=args.rounds,
        sizes=args.sizes,
        widths=args.widths,
        workers=args.workers,
        workflows=args.workflows,
    )
    reports = {}
    if args.redis_url:
        reports["given"] = run(lambda: Redis.from_url(args.redis_url), settings)
    else:
        for persistence in args.persistence or ["everysec"]:
            with throwaway_redis(persistence) as url:
                reports[persistence] = run(lambda: Redis.from_url(url), settings)

    for name, report in reports.items():
        print(f"\n=== {name} ===")
        print(render(report))
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(reports, indent=2) + "\n")
        print(f"\nResults written to {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
