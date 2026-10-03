#!/usr/bin/env python3
"""Fault-injection and workload scenarios, run the same way on OLD and NEW.
"""

import argparse
import base64
import copy
import hashlib
import json
import os
import re
import secrets
import subprocess
import sys
import threading
import time
import traceback
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import httpx

from config import API, API_KEY, USE_CASES, VERSIONS, UseCase, Version
from oracle import EQUIVALENT, check_outputs, sha256_file, workflow_dir

TERMINAL = ("completed", "failed")
EXECUTE_LINE = re.compile(r"Execute: method=(\w+), task=(\w+)")


# small helpers
def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def api(method: str, path: str, **kwargs) -> httpx.Response:
    headers = {"Authorization": f"Bearer {API_KEY}"} if API_KEY else {}
    return httpx.request(method, f"{API}{path}", headers=headers, timeout=15, **kwargs)


def run(*args: str, check: bool = True, env: dict | None = None) -> str:
    result = subprocess.run(
        list(args), capture_output=True, text=True, check=check,
        env={**os.environ, **env} if env else None,
    )
    return result.stdout


def compose(compose_file: Path, *args: str, project: str | None = None, check=True,
            env: dict | None = None) -> str:
    cmd = ["docker", "compose"]
    if project:
        cmd += ["-p", project]
    return run(*cmd, "-f", str(compose_file), *args, check=check, env=env)


def wait_for(condition, timeout: float, interval: float = 0.25):
    """Poll ``condition()`` until truthy; return (value, seconds) or (None, timeout)."""
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        value = condition()
        if value:
            return value, time.monotonic() - start
        time.sleep(interval)
    return None, timeout


def answers(url: str | None) -> bool:
    if url is None:
        return True
    try:
        return httpx.get(url, timeout=2).status_code == 200
    except httpx.HTTPError:
        return False


def parse_ts(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


# the system under test
class Stack:
    def __init__(self, version: Version, use_case: UseCase, payload_override=None, args=None):
        self.version = version
        self.uc = use_case
        export = use_case.folder / "export"
        self.payload = {
            "blueprint": json.loads((export / "blueprint.json").read_text()),
            "dockerinfo": json.loads((export / "dockerinfo.json").read_text()),
        }
        if payload_override:
            self.payload = payload_override(self.payload, use_case, args)

    # --- version check 
    def detect_version(self) -> str:
        """404 on the health API means OLD; anything else means NEW."""
        code = api("GET", "/orchestrator/health/services").status_code
        return "old" if code == 404 else "new"

    # --- orchestrator 
    def orch(self, *args: str, check=True) -> str:
        return compose(
            self.version.compose_file, *args, project=self.version.project, check=check
        )

    def flush_state(self) -> None:
        """Every trial starts from an empty Redis, on both versions."""
        self.orch("exec", "-T", "redis", "redis-cli", "FLUSHALL")

    def brpop_clients(self) -> int:
        """Redis connections currently blocked in BRPOP (the OLD idle-worker hang)."""
        out = self.orch("exec", "-T", "redis", "redis-cli", "CLIENT", "LIST")
        return sum(1 for line in out.splitlines() if "cmd=brpop" in line)

    def running_workers(self) -> int:
        return len(self.orch("ps", "--status", "running", "-q", "worker").split())

    def set_workers(self, n: int) -> None:
        """Scale the worker service to ``n`` replicas (Amendment 4, RQ10).

        ``--no-recreate`` keeps the running workers as they are; only the
        added ones are created, with the shipped defaults. The next
        ``stack.sh`` switch brings the stack back to its configured replicas.
        """
        self.orch("up", "-d", "--no-recreate", "--scale", f"worker={n}", "worker")
        ok, _ = wait_for(lambda: self.running_workers() == n, 60, 0.5)
        if not ok:
            raise RuntimeError(f"expected {n} running workers, found {self.running_workers()}")

    def restart_workers(self) -> None:
        self.orch("kill", "worker")
        self.orch("start", "worker")

    # --- use case 
    def uc_compose(self, *args: str, check=True) -> str:
        return compose(self.uc.compose_file, *args, check=check)

    def prepare_services(self) -> None:
        """All use-case services up, unpaused and answering."""
        for svc in self.uc.recreate_before_trial:
            self.uc_compose("up", "-d", "--force-recreate", svc)
        names = [c for c, _ in self.uc.services.values()]
        self.uc_compose("unpause", *names, check=False)
        self.uc_compose("start", *names)
        for name, (_, url) in self.uc.services.items():
            ok, _ = wait_for(lambda url=url: answers(url), timeout=60, interval=1)
            if not ok:
                raise RuntimeError(f"{name} does not answer at {url}")

    def recreate(self, names: tuple[str, ...], settle: float = 2.0) -> None:
        """Recreate these compose services and wait until they run.

        Used for use-case services without a health URL (e.g. villas-node);
        ``settle`` gives the service's own API time to come up after start.
        """
        for svc in names:
            self.uc_compose("up", "-d", "--force-recreate", svc)
        ok, _ = wait_for(
            lambda: set(names) <= set(self.uc_compose("ps", "--status", "running", "--services").split()),
            timeout=60, interval=0.5,
        )
        if not ok:
            raise RuntimeError(f"{', '.join(names)} not running after recreate")
        time.sleep(settle)

    def recreate_services(self, **env: str) -> None:
        """Recreate every use-case service with this environment (e.g. a key).

        Compose fixes a container's environment when it is created, so
        switching service authentication on or off needs a recreate, not a
        restart.
        """
        names = [c for c, _ in self.uc.services.values()]
        self.uc_compose_env(env, "up", "-d", "--force-recreate", *names)
        for name, (_, url) in self.uc.services.items():
            ok, _ = wait_for(lambda url=url: answers(url), timeout=60, interval=0.5)
            if not ok:
                raise RuntimeError(f"{name} does not answer at {url} after recreate")

    def uc_compose_env(self, env: dict, *args: str) -> str:
        return compose(self.uc.compose_file, *args, env=env)

    def redis_keys(self, pattern: str) -> int:
        out = self.orch("exec", "-T", "redis", "redis-cli", "--scan", "--pattern", pattern)
        return len([line for line in out.splitlines() if line.strip()])

    def stop_service(self, name: str) -> None:
        self.uc_compose("stop", self.uc.compose_name(name))

    def kill_service(self, name: str) -> None:
        self.uc_compose("kill", self.uc.compose_name(name))

    def start_service(self, name: str) -> float:
        self.uc_compose("start", self.uc.compose_name(name))
        _, waited = wait_for(lambda: answers(self.uc.services[name][1]), 60, 0.2)
        return waited

    def executions_since(self, since_iso: str) -> Counter:
        """How often each operation was executed, from the services' own logs.

        Counting on the service side makes re-execution visible for both
        versions, including OLD resubmissions, which get new task ids.
        """
        names = [c for c, _ in self.uc.services.values()]
        logs = self.uc_compose("logs", "--no-log-prefix", "--since", since_iso, *names)
        return Counter(m.group(1) for m in EXECUTE_LINE.finditer(logs))

    # --- workflows 
    def submit(self) -> str:
        r = api("POST", "/workflows", json=self.payload)
        r.raise_for_status()
        return r.json()["workflow_id"]

    def try_submit(self) -> tuple[httpx.Response, float]:
        """Submit and return the response as is, with how long it took."""
        t0 = time.monotonic()
        r = api("POST", "/workflows", json=self.payload)
        return r, time.monotonic() - t0

    def status(self, wf: str) -> str:
        return api("GET", f"/workflows/{wf}").json()["status"]

    def tasks(self, wf: str) -> dict[str, dict]:
        body = api("GET", f"/workflows/{wf}/tasks").json()
        return {t["node_key"].split(":")[0]: t for t in body["tasks"]}

    def finished(self, wf: str):
        s = self.status(wf)
        return s if s in TERMINAL else None

    def task_span(self, wf: str) -> tuple[float, float] | None:
        """Server-side (first task created, last task updated), as epoch seconds."""
        ts = self.tasks(wf).values()
        if not ts:
            return None
        return (min(parse_ts(t["created_at"]) for t in ts),
                max(parse_ts(t["updated_at"]) for t in ts))

    def makespan(self, wf: str) -> float | None:
        """Server-side time from the first task created to the last update."""
        span = self.task_span(wf)
        return span[1] - span[0] if span else None

    def task_status(self, wf: str, service: str):
        return self.tasks(wf).get(service, {}).get("status")

    def health(self, service: str) -> dict | None:
        r = api("GET", f"/orchestrator/health/services/{service}")
        return r.json() if r.status_code == 200 else None

    def alerts_since(self, since_epoch: float, service: str) -> list[dict]:
        r = api("GET", "/orchestrator/alerts?limit=1000")
        if r.status_code != 200:
            return []
        return [
            a
            for a in r.json()["alerts"]
            if a.get("service_name") == service and parse_ts(a["timestamp"]) >= since_epoch
        ]

    def workflow_alerts_since(self, since_epoch: float) -> Counter:
        """NEW's alerts about workflows rather than services (PROTOCOL.md 10.8)."""
        r = api("GET", "/orchestrator/alerts?limit=1000")
        if r.status_code != 200:
            return Counter()
        return Counter(
            a["alert"]
            for a in r.json()["alerts"]
            if a.get("alert", "").startswith("WORKFLOW_") and parse_ts(a["timestamp"]) >= since_epoch
        )

    def recover(self, service: str) -> dict:
        r = api("POST", f"/orchestrator/health/services/{service}/recover")
        r.raise_for_status()
        return r.json()


class StatsSampler(threading.Thread):
    """Samples CPU/memory of the orchestrator containers during a trial."""

    def __init__(self, project: str):
        super().__init__(daemon=True)
        self.project = project
        self.samples: list[dict] = []
        self._halt = threading.Event()

    def run(self):
        while not self._halt.is_set():
            out = run(
                "docker", "stats", "--no-stream", "--format", "{{json .}}", check=False
            )
            t = time.time()
            for line in out.splitlines():
                row = json.loads(line)
                if row["Name"].startswith(self.project + "-"):
                    self.samples.append(
                        {
                            "t": t,
                            "name": row["Name"],
                            "cpu": float(row["CPUPerc"].rstrip("%") or 0),
                            "mem_mib": _mib(row["MemUsage"].split("/")[0].strip()),
                        }
                    )

    def stop(self) -> list[dict]:
        self._halt.set()
        self.join(timeout=10)
        return self.samples


def _mib(text: str) -> float:
    units = {"B": 1 / 2**20, "KiB": 1 / 1024, "MiB": 1, "GiB": 1024, "kB": 1 / 1024}
    m = re.match(r"([\d.]+)\s*([A-Za-z]+)", text)
    return float(m.group(1)) * units.get(m.group(2), 1) if m else 0.0


# operator model
def operator_recover(stack: Stack, wf: str, service: str, rec: dict, limit: float) -> str | None:
    """After the service is back: bring the logical job to completion.

    Returns the id of the workflow that finally carries the job (a new id on
    OLD, where recovery means resubmitting), and records actions and outcome.
    """
    def before_recovery():
        if stack.uc.recreate_before_recovery:
            stack.recreate(stack.uc.recreate_before_recovery)
            rec["f2_recreates"] = rec.get("f2_recreates", 0) + 1

    if stack.version.name == "new":
        status = stack.finished(wf)
        if status is None or status == "failed":
            before_recovery()
            body = stack.recover(service)
            rec["operator_actions"] += 1
            rec["resumed_tasks"] = body.get("resumed_tasks")
            if status == "failed":  # e.g. a blocked-task limit was hit: resume it
                api("POST", f"/workflows/{wf}/resume").raise_for_status()
                rec["operator_actions"] += 1
        final, _ = wait_for(lambda: stack.finished(wf), limit)
        rec["attempts"].append({"workflow_id": wf, "status": final or "timeout"})
        return wf

    # OLD: resubmit until the job completes, at most max_resubmits times
    current = wf
    final, _ = wait_for(lambda: stack.finished(current), limit)
    rec["attempts"].append({"workflow_id": current, "status": final or "timeout"})
    for _ in range(rec["max_resubmits"]):
        if final == "completed":
            break
        before_recovery()
        current = stack.submit()
        rec["operator_actions"] += 1
        final, _ = wait_for(lambda: stack.finished(current), limit)
        rec["attempts"].append({"workflow_id": current, "status": final or "timeout"})
    return current


# scenarios
def sc_nominal(stack: Stack, rec: dict, args) -> None:
    """No fault: one workflow, end-to-end latency (RQ6 overhead)."""
    t0 = time.monotonic()
    wf = stack.submit()
    rec["carriers"] = [wf]
    final, _ = wait_for(lambda: stack.finished(wf), args.timeout, interval=0.05)
    rec["e2e_seconds"] = time.monotonic() - t0
    rec["makespan_seconds"] = stack.makespan(wf)
    rec["final_status"] = final or "timeout"
    rec["job_completed"] = final == "completed"


def sc_burst(stack: Stack, rec: dict, args) -> None:
    """No fault: K workflows at once (throughput) and the idle-worker check (V-A4)."""
    if not stack.uc.concurrent:
        raise SystemExit(f"use case {stack.uc.name} cannot run workflows concurrently")
    k = args.workflows
    t0 = time.monotonic()
    wfs = [stack.submit() for _ in range(k)]
    done: dict[str, float] = {}

    def all_done():
        for wf in wfs:
            if wf not in done and stack.finished(wf):
                done[wf] = time.monotonic() - t0
        return len(done) == k

    wait_for(all_done, args.timeout, interval=0.1)
    statuses = [stack.finished(wf) or "timeout" for wf in wfs]
    rec["workflows"] = k
    rec["jobs"] = k + 1  # the burst plus the late workflow
    rec["completed_workflows"] = statuses.count("completed")
    rec["latencies"] = [done.get(wf) for wf in wfs]
    rec["makespan_seconds"] = max(done.values()) if len(done) == k else None
    rec["throughput_wf_per_min"] = (
        60 * k / rec["makespan_seconds"] if rec["makespan_seconds"] else None
    )
    rec["job_completed"] = rec["completed_workflows"] == k
    _record_failures(stack, rec, wfs)
    # after the burst, idle workers should not sit in a blocking BRPOP
    time.sleep(3)
    rec["brpop_clients_after"] = stack.brpop_clients()
    # a workflow submitted afterwards must still run
    t1 = time.monotonic()
    late = stack.submit()
    final, _ = wait_for(lambda: stack.finished(late), args.timeout)
    rec["late_workflow_status"] = final or "timeout"
    rec["carriers"] = wfs + [late]
    rec["late_workflow_seconds"] = time.monotonic() - t1


def sc_outage(stack: Stack, rec: dict, args) -> None:
    """V-A1: target service down before submission; operator restores it."""
    target = stack.uc.outage_target
    stack.stop_service(target)
    t_fault = time.time()
    wf = stack.submit()

    # Observe the fault for a fixed window, identical for both versions.
    def settled():
        s = stack.status(wf)
        ts = stack.task_status(wf, target)
        return s in TERMINAL or ts in ("blocked", "failed")

    _, seen_after = wait_for(settled, 60)
    rec["first_status"] = stack.status(wf)
    rec["target_task_status_during"] = stack.task_status(wf, target)
    _observe_detection(stack, rec, target, t_fault, window=args.observe)

    restored_at = time.monotonic()
    rec["service_start_seconds"] = stack.start_service(target)
    rec["operator_actions"] += 1  # starting the service: same on both versions
    final_wf = operator_recover(stack, wf, target, rec, args.timeout)
    rec["carriers"] = [final_wf]
    rec["final_status"] = stack.finished(final_wf) or "timeout"
    rec["job_completed"] = rec["final_status"] == "completed"
    rec["recovery_seconds"] = time.monotonic() - restored_at if rec["job_completed"] else None
    _alerts(stack, rec, target, t_fault)


def sc_short_outage(stack: Stack, rec: dict, args) -> None:
    """V-A3: service comes back by itself after a short outage; no operator."""
    target = stack.uc.outage_target
    stack.stop_service(target)
    t_fault = time.time()
    wf = stack.submit()
    wait_for(
        lambda: stack.status(wf) in TERMINAL
        or stack.task_status(wf, target) in ("blocked", "failed"),
        60,
    )
    rec["first_status"] = stack.status(wf)
    rec["carriers"] = [wf]
    time.sleep(args.down_extra)
    restored_at = time.monotonic()
    rec["service_start_seconds"] = stack.start_service(target)
    final, _ = wait_for(lambda: stack.finished(wf), args.timeout)
    rec["final_status"] = final or "timeout"
    rec["job_completed"] = final == "completed"
    rec["recovery_seconds"] = time.monotonic() - restored_at if rec["job_completed"] else None
    _alerts(stack, rec, target, t_fault)


def sc_outage_during_task(stack: Stack, rec: dict, args) -> None:
    """V-A2: the service of a long-running task dies while the task runs."""
    target = stack.uc.long_task
    if not target:
        raise SystemExit(f"use case {stack.uc.name} has no long-running task (use germany)")
    wf = stack.submit()
    running, _ = wait_for(lambda: stack.task_status(wf, target) == "running", 120, 0.2)
    if not running:
        raise RuntimeError(f"{target} never reached 'running'")
    stack.kill_service(target)
    t_fault = time.time()
    wait_for(
        lambda: stack.status(wf) in TERMINAL
        or stack.task_status(wf, target) in ("blocked", "failed"),
        120,
    )
    rec["first_status"] = stack.status(wf)
    rec["target_task_status_during"] = stack.task_status(wf, target)
    _observe_detection(stack, rec, target, t_fault, window=args.observe)
    restored_at = time.monotonic()
    rec["service_start_seconds"] = stack.start_service(target)
    rec["operator_actions"] += 1
    final_wf = operator_recover(stack, wf, target, rec, args.timeout)
    rec["carriers"] = [final_wf]
    rec["final_status"] = stack.finished(final_wf) or "timeout"
    rec["job_completed"] = rec["final_status"] == "completed"
    rec["recovery_seconds"] = time.monotonic() - restored_at if rec["job_completed"] else None
    _alerts(stack, rec, target, t_fault)


def sc_request_error(stack: Stack, rec: dict, args) -> None:
    """V-A7: a request error (unknown operation) must fail cleanly on both.

    Non-regression check: NEW must fail the workflow like OLD, and must not
    count a client-side error against the service's health.
    """
    wf = stack.submit()
    rec["carriers"] = [wf]
    final, secs = wait_for(lambda: stack.finished(wf), args.timeout)
    rec["final_status"] = final or "timeout"
    rec["time_to_terminal"] = secs
    rec["job_completed"] = final == "completed"
    h = stack.health(stack.uc.outage_target) if stack.version.name == "new" else None
    rec["circuit_state_after"] = h.get("circuit_state") if h else None


def sc_worker_death(stack: Stack, rec: dict, args) -> None:
    """V-B12: every worker is killed while holding a task.

    The target service is paused so its task stays claimed; all workers are
    then killed and fresh ones started, and the service is resumed. Nobody
    touches the workflow afterwards.
    """
    target = stack.uc.outage_target
    svc = stack.uc.compose_name(target)
    stack.uc_compose("pause", svc)
    try:
        wf = stack.submit()
        rec["carriers"] = [wf]
        running, _ = wait_for(lambda: stack.task_status(wf, target) == "running", 60)
        if not running:
            raise RuntimeError(f"{target} was never claimed")
        stack.restart_workers()
        t_fault = time.monotonic()
    finally:
        stack.uc_compose("unpause", svc)
    final, _ = wait_for(lambda: stack.finished(wf), args.timeout)
    rec["final_status"] = final or "timeout"
    rec["target_task_status_end"] = stack.task_status(wf, target)
    rec["job_completed"] = final == "completed"
    rec["recovery_seconds"] = time.monotonic() - t_fault if final == "completed" else None


def sc_concurrent_outage(stack: Stack, rec: dict, args) -> None:
    """V-B3: K workflows blocked on one service, then one restoration."""
    if not stack.uc.concurrent:
        raise SystemExit(f"use case {stack.uc.name} cannot run workflows concurrently")
    target = stack.uc.outage_target
    k = args.workflows
    stack.stop_service(target)
    t_fault = time.time()
    wfs = [stack.submit() for _ in range(k)]
    wait_for(
        lambda: all(
            stack.status(w) in TERMINAL or stack.task_status(w, target) in ("blocked", "failed")
            for w in wfs
        ),
        90,
    )
    rec["first_statuses"] = Counter(stack.status(w) for w in wfs)
    restored_at = time.monotonic()
    stack.start_service(target)
    rec["operator_actions"] += 1
    if stack.version.name == "new":
        body = stack.recover(target)
        rec["operator_actions"] += 1
        rec["resumed_tasks"] = body.get("resumed_tasks")
        finals = [wait_for(lambda w=w: stack.finished(w), args.timeout)[0] for w in wfs]
        carriers = wfs
    else:
        finals, carriers = [], []
        for w in wfs:
            f = stack.finished(w)
            if f == "failed":
                w = stack.submit()
                rec["operator_actions"] += 1
            finals.append(wait_for(lambda w=w: stack.finished(w), args.timeout)[0])
            carriers.append(w)
    _record_failures(stack, rec, carriers)
    rec["carriers"] = carriers
    rec["workflows"] = k
    rec["jobs"] = k
    rec["completed_workflows"] = sum(f == "completed" for f in finals)
    rec["job_completed"] = rec["completed_workflows"] == k
    rec["recovery_seconds"] = time.monotonic() - restored_at if rec["job_completed"] else None
    _alerts(stack, rec, target, t_fault)


# Scaling and data size 
def sc_scale(stack: Stack, rec: dict, args) -> None:
    """RQ10: K workflows submitted at once, on the W workers set by --workers.

    Latency and throughput come from the orchestrator's own task timestamps,
    not from the client's polling, so that polling K workflows (which loads the
    API equally on both versions) does not set the resolution. A workflow's
    latency runs from its first task created (at submission) to its last task
    updated; the trial's makespan from the first task of any workflow to the
    last update of any.
    """
    if not stack.uc.concurrent:
        raise SystemExit(f"use case {stack.uc.name} cannot run workflows concurrently")
    k = args.workflows
    rec["workflows"] = rec["jobs"] = k
    rec["workers"] = stack.running_workers()
    t0 = time.monotonic()
    wfs = [stack.submit() for _ in range(k)]
    rec["submit_seconds"] = time.monotonic() - t0
    rec["carriers"] = wfs
    pending = set(wfs)

    def drain():
        for wf in list(pending):
            if stack.finished(wf):
                pending.discard(wf)
        return not pending

    wait_for(drain, args.timeout, interval=0.5)
    rec["client_seconds"] = time.monotonic() - t0
    statuses = {wf: stack.finished(wf) or "timeout" for wf in wfs}
    rec["completed_workflows"] = sum(s == "completed" for s in statuses.values())
    rec["job_completed"] = rec["completed_workflows"] == k
    spans = {wf: stack.task_span(wf) for wf in wfs}
    done = [spans[wf] for wf in wfs if statuses[wf] == "completed" and spans[wf]]
    rec["latencies"] = [end - start for start, end in done]
    if rec["job_completed"] and done:
        rec["makespan_seconds"] = max(end for _, end in done) - min(start for start, _ in done)
        rec["throughput_wf_per_min"] = 60 * k / rec["makespan_seconds"] if rec["makespan_seconds"] > 0 else None
    else:
        rec["makespan_seconds"] = rec["throughput_wf_per_min"] = None
    _record_failures(stack, rec, wfs)


def _single_workflow(stack: Stack, rec: dict, args) -> str | None:
    """Submit once and follow the workflow; a refusal is a result, not an error.

    Returns the workflow id, or None if the submission was refused.
    """
    t0 = time.monotonic()
    r, took = stack.try_submit()
    rec["submit_status"] = r.status_code
    rec["submit_seconds"] = took
    rec["accepted"] = r.status_code == 200
    rec["refused"] = r.status_code == 400
    if not rec["accepted"]:
        rec["refusal"] = r.text[:300]
        rec["final_status"] = "refused" if rec["refused"] else f"http {r.status_code}"
        rec["job_completed"] = False
        return None
    wf = r.json()["workflow_id"]
    final, _ = wait_for(lambda: stack.finished(wf), args.timeout, interval=0.05)
    rec["e2e_seconds"] = time.monotonic() - t0
    rec["makespan_seconds"] = stack.makespan(wf)
    rec["final_status"] = final or "timeout"
    rec["job_completed"] = final == "completed"
    return wf


def sc_inline_size(stack: Stack, rec: dict, args) -> None:
    rec["inline_bytes"] = args.size
    wf = _single_workflow(stack, rec, args)
    if wf:
        rec["carriers"] = [wf]


def sc_data_size(stack: Stack, rec: dict, args) -> None:
    rec["records"] = args.records
    wf = _single_workflow(stack, rec, args)
    if wf and rec["job_completed"]:
        folder = workflow_dir(stack.uc.data_dir, wf)
        names = list(stack.uc.output_reference)
        rec["output_hashes"] = {name: sha256_file(folder / name) for name in names}
        rec["output_bytes"] = {name: (folder / name).stat().st_size if (folder / name).is_file() else None
                               for name in names}


def inline_config_input(config: dict, size: int | None = None) -> dict:
    """A valid inline JSON initial input, padded to exactly ``size`` decoded bytes."""
    data = json.dumps(config).encode()
    if size is not None:
        base = len(json.dumps({**config, "padding": ""}).encode())
        if size < base:
            raise ValueError(f"an inline input of this config needs at least {base} bytes")
        data = json.dumps({**config, "padding": "x" * (size - base)}).encode()
    return {
        "protocol": "inline",
        "uri": base64.b64encode(data).decode(),
        "format": "json",
        "checksum": "sha256:" + hashlib.sha256(data).hexdigest(),
        "size_bytes": len(data),
    }


def with_inline_size(payload: dict, uc: UseCase, args) -> dict:
    p = copy.deepcopy(payload)
    if args.size:
        p["inputs"] = [inline_config_input({"num_records": 10, "output_format": "csv"}, args.size)]
    return p


def with_records(payload: dict, uc: UseCase, args) -> dict:
    p = copy.deepcopy(payload)
    p["inputs"] = [inline_config_input({"num_records": args.records, "output_format": "csv"})]
    return p


def check_job_outputs(stack: Stack, rec: dict) -> None:
    reference = stack.uc.output_reference
    if not reference or "carriers" not in rec:
        return
    outputs = []
    for wf in rec["carriers"]:
        if stack.status(wf) != "completed":
            continue
        result = check_outputs(stack.uc.data_dir, wf, reference)
        result["history"] = api("GET", f"/workflows/{wf}/tasks").json().get("history")
        verify = api("GET", f"/workflows/{wf}/verify")
        result["verified"] = verify.json()["verified"] if verify.status_code == 200 else None
        if result["verified"] is False:
            result["verification_problems"] = [
                f"{c['name']}: {p}" for c in verify.json()["checks"] for p in c["problems"]
            ]
        outputs.append(result)
    rec["outputs"] = outputs
    rec["jobs_checked"] = len(outputs)
    rec["jobs_equivalent"] = sum(o["verdict"] == EQUIVALENT for o in outputs)
    rec["silent_divergences"] = len(outputs) - rec["jobs_equivalent"]


def _record_failures(stack: Stack, rec: dict, wfs: list[str]) -> None:
    """Keep the status and task errors of every workflow that did not complete."""
    out = []
    for wf in wfs:
        status = stack.status(wf)
        if status != "completed":
            out.append({
                "workflow_id": wf,
                "status": status,
                "tasks": {k: {"status": t["status"], "error": t.get("error")}
                          for k, t in stack.tasks(wf).items() if t["status"] != "completed"},
            })
    rec["unfinished_workflows"] = out


def _observe_detection(stack: Stack, rec: dict, target: str, t_fault: float, window: float):
    """Is the outage visible through an API, and how fast (NEW only)?"""
    h = stack.health(target)
    rec["health_api"] = h is not None
    if h is None:
        rec["detection_seconds"] = None
        return
    unhealthy, _ = wait_for(
        lambda: (x := stack.health(target)) and x["status"] == "UNHEALTHY", window, 0.2
    )
    rec["detection_seconds"] = time.time() - t_fault if unhealthy else None


def _alerts(stack: Stack, rec: dict, target: str, t_fault: float):
    alerts = stack.alerts_since(t_fault, target)
    kinds = Counter(a["alert"] for a in alerts)
    rec["alerts"] = dict(kinds)
    down = sorted(parse_ts(a["timestamp"]) for a in alerts if a["alert"] == "SERVICE_DOWN")
    rec["first_alert_seconds"] = down[0] - t_fault if down else None


# Data-exchange contracts 
def _submit_and_observe(stack: Stack, rec: dict, args) -> None:
    """Submit; if accepted, follow the workflow until the client can see it fail.

    ``error_visible_seconds`` is the time from submission to the first error a
    client can observe: the refusal itself, or the workflow turning ``failed``.
    """
    t0 = time.monotonic()
    r, took = stack.try_submit()
    rec["submit_status"] = r.status_code
    rec["accepted"] = r.status_code == 200
    rec["refused"] = r.status_code == 400
    rec["refusal"] = None if rec["accepted"] else r.text[:300]
    if rec["accepted"]:
        wf = r.json()["workflow_id"]
        rec["carriers"] = [wf]
        final, _ = wait_for(lambda: stack.finished(wf), args.timeout, interval=0.05)
        rec["final_status"] = final or "timeout"
        rec["job_completed"] = final == "completed"
        rec["error_visible_seconds"] = time.monotonic() - t0 if final == "failed" else None
        tasks = stack.tasks(wf).values()
        rec["tasks_started"] = sum(t["status"] != "pending" for t in tasks)
        rec["task_errors"] = [t["error"][:200] for t in tasks if t.get("error")]
    else:
        rec["final_status"] = "refused" if rec["refused"] else f"http {r.status_code}"
        rec["job_completed"] = False
        rec["error_visible_seconds"] = took
        rec["tasks_started"] = 0
    rec["residue_services_keys"] = stack.redis_keys("services_key:*")
    rec["residue_endpoints"] = stack.redis_keys("endpoints:*")


def sc_credential_misconfig(stack: Stack, rec: dict, args) -> None:
    """V-C1: the services require a key and the workflow is submitted without one."""
    key = secrets.token_hex(24)
    stack.recreate_services(SERVICE_API_KEY=key)
    try:
        _submit_and_observe(stack, rec, args)
    finally:
        # Back to the unauthenticated services every other scenario uses
        stack.recreate_services(SERVICE_API_KEY="")


def sc_corrupt_inline(stack: Stack, rec: dict, args) -> None:
    """V-C2: an inline input whose checksum does not match its payload."""
    _submit_and_observe(stack, rec, args)


def corrupt_inline_input(payload: dict, uc: UseCase, args=None) -> dict:
    """Add an inline initial input whose stated checksum is not its own."""
    p = copy.deepcopy(payload)
    data = json.dumps({"num_records": 10, "output_format": "csv"}).encode()
    p["inputs"] = [{
        "protocol": "inline",
        "uri": base64.b64encode(data).decode(),
        "format": "json",
        "checksum": "sha256:" + hashlib.sha256(data + b" altered").hexdigest(),
    }]
    return p


def break_operation(payload: dict, uc: UseCase, args=None) -> dict:
    """Ask the outage target for an operation it does not implement.

    The blueprint stays consistent (the node and every link to it use the new
    name), so the orchestrator accepts it and the *service* rejects the call.
    """
    p = copy.deepcopy(payload)
    target = uc.outage_target
    for node in p["blueprint"]["nodes"]:
        for op in node["operation_signature_list"]:
            if node["container_name"] == target:
                op["operation_signature"]["operation_name"] += "DoesNotExist"
            for link in op.get("connected_to", []):
                if link["container_name"] == target:
                    link["operation_signature"]["operation_name"] += "DoesNotExist"
    return p


SCENARIOS = {
    "nominal": (sc_nominal, None),
    "burst": (sc_burst, None),
    "outage": (sc_outage, None),
    "short-outage": (sc_short_outage, None),
    "outage-during-task": (sc_outage_during_task, None),
    "request-error": (sc_request_error, break_operation),
    "worker-death": (sc_worker_death, None),
    "concurrent-outage": (sc_concurrent_outage, None),
    "credential-misconfig": (sc_credential_misconfig, None),
    "corrupt-inline": (sc_corrupt_inline, corrupt_inline_input),
    # Amendment 4 (PROTOCOL.md 11): swept by campaign_sweeps.sh
    "scale": (sc_scale, None),
    "inline-size": (sc_inline_size, with_inline_size),
    "data-size": (sc_data_size, with_records),
}


SWEEPS = {
    "scale": (("workflows", "workflows"), ("workers_requested", "workers")),
    "inline-size": (("inline_bytes", "size"),),
    "data-size": (("records", "records"),),
}


# main
def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--version", choices=VERSIONS, required=True)
    p.add_argument("--scenario", choices=SCENARIOS, required=True)
    p.add_argument("--use-case", choices=USE_CASES, default="file_based")
    p.add_argument("--reps", type=int, default=10)
    p.add_argument("--warmup", type=int, default=0, help="unrecorded trials first")
    p.add_argument("--workflows", type=int, default=10, help="K for burst/concurrent-outage/scale")
    p.add_argument("--workers", type=int, help="scale the worker service to W replicas first (scale)")
    p.add_argument("--size", type=int, default=0, help="inline input size in decoded bytes (inline-size)")
    p.add_argument("--records", type=int, default=10, help="records the pipeline generates (data-size)")
    p.add_argument("--timeout", type=float, default=180, help="per-workflow wait (s)")
    p.add_argument("--observe", type=float, default=30, help="fault observation window (s)")
    p.add_argument("--down-extra", type=float, default=10, help="short-outage extra downtime (s)")
    p.add_argument("--max-resubmits", type=int, default=2, help="OLD operator budget")
    p.add_argument("--stats", action="store_true", help="sample docker stats")
    p.add_argument("--variant", default="default",
                   help="label of the orchestrator configuration, e.g. checks-off (RQ6 attribution)")
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()

    fn, override = SCENARIOS[args.scenario]
    stack = Stack(VERSIONS[args.version], USE_CASES[args.use_case], override, args)
    live = stack.detect_version()
    if live != args.version:
        print(f"refusing: asked for {args.version} but {live.upper()} is live", file=sys.stderr)
        return 2

    if args.workers:
        stack.set_workers(args.workers)

    args.out.mkdir(parents=True, exist_ok=True)
    out = args.out / "trials.jsonl"
    total = args.warmup + args.reps
    for i in range(total):
        warm = i < args.warmup
        rec = {
            "version": args.version,
            "scenario": args.scenario,
            "variant": args.variant,
            "use_case": args.use_case,
            "rep": i - args.warmup,
            "warmup": warm,
            "started_at": now_iso(),
            "operator_actions": 0,
            "attempts": [],
            "max_resubmits": args.max_resubmits,
        }
        if args.scenario in SWEEPS:  # the point of the sweep this trial belongs to
            rec.update({key: getattr(args, attr) for key, attr in SWEEPS[args.scenario]})
        sampler = None
        try:
            stack.prepare_services()
            stack.flush_state()
            since = now_iso()
            since_epoch = time.time()
            if args.stats:
                sampler = StatsSampler(stack.version.project)
                sampler.start()
            fn(stack, rec, args)
            check_job_outputs(stack, rec)
            if stack.version.name == "new":
                rec["workflow_alerts"] = dict(stack.workflow_alerts_since(since_epoch))
            execs = stack.executions_since(since)
            rec["executions"] = dict(execs)
            # every logical job runs each operation once; anything beyond is a re-run
            jobs = rec.get("jobs", 1)
            rec["reexecutions"] = sum(max(0, n - jobs) for n in execs.values())
        except Exception as exc:  # the trial is recorded as an error, not dropped
            rec["error"] = f"{type(exc).__name__}: {exc}"
            rec["traceback"] = traceback.format_exc()
        finally:
            if sampler:
                rec["stats"] = sampler.stop()
            rec["ended_at"] = now_iso()
        if not warm:
            with out.open("a") as f:
                f.write(json.dumps(rec, default=str) + "\n")
        print(
            f"[{args.version} {args.scenario} {'warm' if warm else rec['rep']}] "
            f"completed={rec.get('job_completed')} status={rec.get('final_status')} "
            f"actions={rec['operator_actions']} reexec={rec.get('reexecutions')} "
            f"{'equivalent=' + str(rec['jobs_equivalent']) + '/' + str(rec['jobs_checked']) + ' ' if 'jobs_checked' in rec else ''}"
            f"{'ERROR ' + rec['error'] if 'error' in rec else ''}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
