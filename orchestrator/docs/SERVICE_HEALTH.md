# Service Health Awareness & Manual Recovery

This document describes the changes made to the orchestrator compared with the
previous version of the repository (`oldCode/ai-effect-wp3`). It covers what
was changed, why, how to configure it and how to test it.

The old orchestrator had only a static `GET /health` endpoint returning
`{"status": "ok"}`. It had no notion of whether the services it calls were
reachable, and any service failure immediately failed the whole workflow.

## Summary of improvements

| # | Feature | Result |
|---|---------|--------|
| 1 | **Service health detection** | A Redis-backed circuit breaker tracks every service. When a circuit opens, a structured `SERVICE DOWN: {service_name} at {base_url}` alert is logged and written as JSON to `logs/alerts.jsonl`, ready to be forwarded to TEF monitoring. |
| 2 | **Health status API** | `GET /orchestrator/health/services` returns every registered service as `HEALTHY` / `DEGRADED` / `UNHEALTHY`, with the last failure, timestamp and error message. |
| 3 | **Manual recovery API** | `POST /orchestrator/health/services/{service_name}/recover` resets the circuit breaker and immediately re-queues the tasks waiting on that service. |
| 4 | **Workflows survive outages** | Tasks for an unavailable service are marked `BLOCKED` and retried later instead of failing the workflow. After the service returns, the *same* workflow completes, with no need to resubmit it or restart the orchestrator. |
| 5 | **Idle workers no longer hang** (bug fix) | `dequeue_task(timeout=0)` used Redis `BRPOP`, where `0` means *block forever*. Workers now poll without blocking, so they keep cycling and can retry blocked tasks. |

## How it works

```
                ┌──────────────── Redis (shared state) ────────────────┐
                │ circuit:registry   per-service health (breaker)      │
                │ registry:services  known services + their workflows  │
                │ queue:{wf}         ready tasks                       │
                │ delayed:{wf}       BLOCKED tasks + retry time        │
                └──────────────────────────────────────────────────────┘
                   ▲                                   ▲
  worker replicas  │                                   │  API
  ─────────────────┘                                   └─────────────────
  1. release tasks whose retry time has passed         GET  /orchestrator/health/services
  2. claim a task                                      POST /orchestrator/health/services/{name}/recover
  3. circuit open?  → task BLOCKED, retry later              → reset breaker
  4. call service                                            → re-queue that service's BLOCKED tasks
     - success            → record success, complete task
     - transient failure  → record failure, task BLOCKED
     - other failure      → fail task (as before), breaker untouched
```

Because all state lives in Redis, a failure seen by one worker replica opens
the circuit for all replicas, and a recovery triggered through the API unblocks
all of them at once.

### Circuit breaker states

| Circuit state | Reported status | Meaning |
|---------------|-----------------|---------|
| `closed`, 0 recent failures | `HEALTHY` | Service is working. |
| `closed`, ≥ `CIRCUIT_DEGRADED_THRESHOLD` failures | `DEGRADED` | Service was recently unreachable, but not enough times to be taken out of use. |
| `open` | `UNHEALTHY` | `CIRCUIT_FAILURE_THRESHOLD` consecutive failures. Calls are not attempted. `SERVICE DOWN` alert is emitted once. |
| `half_open` | `UNHEALTHY` | `CIRCUIT_RESET_TIMEOUT` has passed; the next call is a probe. Success closes the circuit (`SERVICE RECOVERED`), failure opens it again. |

### Which failures block a task and which fail it

| Failure | Counts against the breaker | Task outcome |
|---------|:--------------------------:|--------------|
| Connection error, timeout, other request error | yes | `BLOCKED` → retried |
| HTTP 5xx | yes | `BLOCKED` → retried |
| Circuit open (call not attempted) | n/a | `BLOCKED` → retried |
| HTTP 4xx, invalid response | no | `FAILED` (workflow fails) |
| Service replies `status: failed` (on execute or while polling) | no | `FAILED` (workflow fails) |

Only availability failures count against the breaker. A service that answers
at all, even with an error, is reachable. For example, `villas-chronics`
reporting that its `villas-node` dependency crashed must not mark
`villas-chronics` as down (see [RUN_ANALYSIS_GERMANY_NODE.md](RUN_ANALYSIS_GERMANY_NODE.md), run 2).
Such failures also do not raise the `DEGRADED` status.

Transient errors while *polling* a long-running task also block it. Because the
service's own task state is probably lost when it goes down, the task is
executed again from the start after recovery.

Blocking only happens when a circuit breaker is configured (always the case in
the worker daemon). A `Worker` created without a breaker behaves as before.

## Alerts (feature 1)

All alerts go through the dedicated logger `orchestrator.alerts` and share one
schema:

| Field | Description |
|-------|-------------|
| `alert` | `SERVICE_DOWN`, `SERVICE_RECOVERED` or `SERVICE_MANUAL_RECOVERY` |
| `severity` | `critical`, `info` or `warning` respectively |
| `source` | always `orchestrator` |
| `service_name`, `base_url` | the affected service |
| `timestamp` | ISO 8601 UTC time of the event |
| `failure_count`, `failure_threshold`, `last_error` | only on `SERVICE_DOWN` |

Example line from `logs/alerts.jsonl`:

```json
{"timestamp": "2026-09-16T21:25:10.106000+00:00", "level": "ERROR", "logger": "orchestrator.alerts",
 "message": "SERVICE DOWN: databroker at http://databroker:8080", "event": "service_down",
 "alert": "SERVICE_DOWN", "severity": "critical", "source": "orchestrator",
 "service_name": "databroker", "base_url": "http://databroker:8080",
 "failure_count": 1, "failure_threshold": 1, "last_error": "Connection failed: ..."}
```

(Shown on several lines for readability; the file has one JSON object per line.)

Alerts also appear in the normal log (`orchestrator.log` / `worker_daemon.log`
and the console). With `LOG_FORMAT=json` the normal log is JSON too; with the
default `text` it keeps the old format.

`alerts.jsonl` is written by the process that detected the event, which for
`SERVICE_DOWN` is a worker replica. The file lives in `/app/src/logs/` inside
the container.

## API (features 2 and 3)

All routes require the orchestrator API key (`Authorization: Bearer <key>`)
when `ORCHESTRATOR_API_KEY` is set.

### `GET /orchestrator/health/services`

```json
{
  "overall_status": "UNHEALTHY",
  "total": 2,
  "healthy": 1,
  "degraded": 0,
  "unhealthy": 1,
  "services": [
    {
      "service_name": "databroker",
      "base_url": "http://databroker:8080",
      "status": "UNHEALTHY",
      "circuit_state": "open",
      "failure_count": 1,
      "last_failure": "2026-09-16T21:25:10.106000+00:00",
      "last_error": "Connection failed: [Errno 111] Connection refused",
      "last_success": null,
      "opened_at": "2026-09-16T21:25:10.106000+00:00",
      "retry_after": "2026-09-16T21:26:10.106000+00:00",
      "manually_recovered_at": null
    },
    { "service_name": "predictor", "status": "HEALTHY", "...": "..." }
  ]
}
```

`overall_status` is the worst status among all services. Services are
registered when a workflow is submitted, so they appear before any task has
run against them.

### `GET /orchestrator/health/services/{service_name}`

Same object as one entry of `services` above. Returns `404` for unknown services.

### `GET /orchestrator/health/services/{service_name}/endpoint`

Registration details: `address`, `port`, `base_url`, `first_seen`,
`last_seen`, and the `workflow_ids` that used the service (the 20 most recent).

### `POST /orchestrator/health/services/{service_name}/recover`

```json
{
  "service_name": "databroker",
  "status": "HEALTHY",
  "message": "Circuit breaker reset; service marked healthy. 1 blocked task(s) re-queued.",
  "resumed_tasks": 1
}
```

- Closes the circuit, resets the failure count, sets `manually_recovered_at`
  and emits a `SERVICE_MANUAL_RECOVERY` alert.
- Re-queues the `BLOCKED` tasks of this service in every **running** workflow
  that used it. Deleted workflows are skipped.
- Safe to call more than once. Returns `404` for unknown services.

Recovery does not check whether the service is actually back. If it is still
down, the next call fails, and with `CIRCUIT_FAILURE_THRESHOLD=1` the circuit
opens again straight away.

## Configuration

| Variable | Default | Used by | Description |
|----------|---------|---------|-------------|
| `CIRCUIT_FAILURE_THRESHOLD` | `3` (`1` for the worker in `docker-compose.yml`) | API, worker | Consecutive failures before the circuit opens |
| `CIRCUIT_RESET_TIMEOUT` | `60` | API, worker | Seconds before an open circuit allows a probe call |
| `CIRCUIT_DEGRADED_THRESHOLD` | `1` | API, worker | Failures at which a closed circuit reports `DEGRADED` |
| `BLOCKED_TASK_RETRY_DELAY` | `30` | worker | Seconds before a `BLOCKED` task is re-queued automatically |
| `LOG_FORMAT` | `text` | API, worker | `text` or `json` for the main log. `alerts.jsonl` is always JSON. |

The API container currently does not set the `CIRCUIT_*` variables, so it uses
the defaults. Status is still reported correctly (the worker opens the circuit
in the shared Redis state), but for consistency set the same values on both
services.

## Changed files (compared with `oldCode/ai-effect-wp3/orchestrator`)

### New files

| File | Purpose |
|------|---------|
| `src/services/circuit_breaker.py` | Redis-backed circuit breaker, health snapshots, alerts |
| `src/services/service_registry.py` | Known services, overall health summary, recovery and resuming of blocked tasks |
| `src/api/health_routes.py` | Health and recovery routes |
| `docs/SERVICE_HEALTH.md` | This document |
| `tests/unit/test_circuit_breaker.py` | Breaker states, thresholds, alerts, JSON alert output |
| `tests/unit/test_service_registry.py` | Registration, overall health summary, recovery |
| `tests/unit/test_log_service.py` | JSON formatter, `LOG_FORMAT`, alerts file |
| `tests/integration/test_health_api.py` | Health API end to end, auth, recovery resuming a workflow |

### Modified files

| File | Change |
|------|--------|
| `src/services/worker.py` | Circuit breaker check before calling a service; records successes and failures; `BLOCKED` instead of failing when a service is unavailable; re-queues tasks whose retry time has passed before claiming a new one |
| `src/services/workflow_engine.py` | `defer_task`, `release_due_tasks`, `resume_blocked_tasks` |
| `src/services/task_queue.py` | Delayed re-queue (`schedule_task`, `release_due_tasks`, `release_tasks`, `delayed_tasks`). Each task is re-queued only once, even if several processes try at the same time. `clear_queue` also removes delayed tasks. **Fix:** `timeout=0` no longer blocks forever. |
| `src/services/control_client.py` | `ControlClientError.transient` (connection errors, timeouts, 5xx). The `parameters` argument came from an earlier commit. |
| `src/services/log_service.py` | `JsonFormatter`, `LOG_FORMAT`, `orchestrator.alerts` logger and `alerts.jsonl` |
| `src/services/state_store.py` | `clear_error` option in `update_task_status` |
| `src/models/state.py` | `TaskStatus.BLOCKED` |
| `src/api/app.py` | Creates the breaker and registry, registers services when a workflow is submitted, mounts the health routes behind the API key |
| `src/api/models.py` | `ServiceHealthResponse`, `ServicesHealthResponse`, `RecoveryResponse` (with `resumed_tasks`), `RegisteredServiceResponse` |
| `src/main.py` | Builds the breaker and registry from environment variables |
| `src/worker_daemon.py` | Shared breaker; `CIRCUIT_*` and `BLOCKED_TASK_RETRY_DELAY` settings |
| `docker-compose.yml` | `LOG_FORMAT`, `CIRCUIT_FAILURE_THRESHOLD=1` (worker), `BLOCKED_TASK_RETRY_DELAY` |
| `tests/conftest.py` | Adds `src` to the import path |
| `tests/unit/test_worker.py` | Breaker check, blocking vs failing, polling errors, recovery |
| `tests/unit/test_workflow_engine.py` | Deferring, releasing and resuming blocked tasks (only the named service's tasks) |
| `tests/unit/test_task_queue.py` | Non-blocking dequeue, delayed tasks, each task re-queued only once |
| `tests/unit/test_control_client.py` | `transient` flag |

`requirements.txt` (`httpx2`) and `run-tests.sh` (Docker network and socket)
also differ from the old version, but those changes are older and unrelated
to this feature.

## Testing

### Automated tests

From `orchestrator/`:

```bash
# Everything except e2e (Docker based, same as run-tests.sh)
./run-tests.sh tests/unit tests/integration

# Only the tests for these features
./run-tests.sh tests/unit/test_circuit_breaker.py tests/unit/test_service_registry.py \
  tests/unit/test_log_service.py tests/unit/test_worker.py \
  tests/unit/test_workflow_engine.py tests/unit/test_task_queue.py \
  tests/integration/test_health_api.py -v
```

Last run: 483 unit + health API tests and 93 integration tests passed. The e2e
suite (`tests/e2e`) needs the use-case services running and was not run.

The main scenario test is
`tests/integration/test_health_api.py::TestRecoveryResumesWorkflows::test_recover_resumes_blocked_workflow`:
a service goes down → its task is `BLOCKED` → the workflow stays `running` →
`POST /recover` → `resumed_tasks == 1` → the same workflow reaches `completed`.

### Manual end-to-end check

A complete manual run against the `germany-node` use case, with the observed
output and its analysis, is documented in
[RUN_ANALYSIS_GERMANY_NODE.md](RUN_ANALYSIS_GERMANY_NODE.md).

```bash
# Start orchestrator (JSON logs optional) and a use case
cd orchestrator && LOG_FORMAT=json docker compose up -d --build
cd ../use-cases/file_based_energy_pipeline && ./start.sh && ./submit-workflow.sh

AUTH="Authorization: Bearer $ORCHESTRATOR_API_KEY"   # omit if no key is set
API=http://localhost:18000
COMPOSE="docker compose -f ../../orchestrator/docker-compose.yml"

# 1. Take a service down while the workflow is running
docker stop <service-container>

# 2. Detection: alert in logs and in alerts.jsonl
$COMPOSE logs worker | grep "SERVICE DOWN"
for i in 1 2 3; do $COMPOSE exec --index $i worker cat logs/alerts.jsonl; done

# 3. Status: service UNHEALTHY, workflow still running, task blocked
curl -s -H "$AUTH" $API/orchestrator/health/services | jq
curl -s -H "$AUTH" $API/workflows/<workflow_id> | jq

# 4. Recovery: bring the service back and recover it
docker start <service-container>
curl -s -X POST -H "$AUTH" $API/orchestrator/health/services/<service-name>/recover | jq
#    → "resumed_tasks": 1

# 5. The same workflow completes
curl -s -H "$AUTH" $API/workflows/<workflow_id> | jq .status
```

Without step 4 the blocked task is retried automatically every
`BLOCKED_TASK_RETRY_DELAY` seconds. Once `CIRCUIT_RESET_TIMEOUT` has passed,
the first successful retry closes the circuit and logs `SERVICE RECOVERED`.

## Known limitations

- Breaker updates read the state, change it and write it back without a lock.
  With several worker replicas failing at the same moment, a failure count can
  be lost or `SERVICE DOWN` can be logged twice. The fix is atomic updates
  (e.g. a Redis Lua script).
- In the `half_open` state, several replicas may each send a probe call instead
  of just one.
- A workflow waiting on a service that never comes back stays `running`
  indefinitely. There is no maximum number of retries.
- A workflow that already failed (e.g. before this change) is not resumed by
  recovery. It has to be submitted again.
- Alerts are written to log files only. Sending them to TEF (log shipper,
  Redis stream or webhook) still has to be set up.
