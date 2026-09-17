# Run Analysis: Service Health Features on `germany-node`

Manual end-to-end runs of the features described in
[SERVICE_HEALTH.md](SERVICE_HEALTH.md), performed on the live Docker stack with
the `use-cases/germany-node` pipeline. The document records what was run, what
was observed, how it compares with the expected behaviour, and what still has
to be tested to fully verify the improvements over the old code
(`oldCode/ai-effect-wp3`).

**Dates:** runs 1–3 on 2026-09-16, run 4 on 2026-09-17. All times are UTC, as
printed by the containers.
**Result:** ✅ The full scenario (service down → task blocked → workflow kept
running → manual recovery → same workflow completed) was verified in run 3, and
again in run 4 through the browser with the API key enabled. Run 2 found a
design issue, which was fixed afterwards (see [Findings](#6-findings)).
Section 8 measures the same outage on the old code (`486ccc7`) for a direct
comparison: there the workflow fails, there is no health API and no alert, and
the only recovery is to resubmit the whole workflow.

---

## 1. Setup

### Stack

| Component | Details |
|-----------|---------|
| Orchestrator | `orchestrator/docker-compose.yml`, rebuilt with the new code: `redis`, `api`, 3 × `worker` |
| Worker config | `CIRCUIT_FAILURE_THRESHOLD=1`, `CIRCUIT_RESET_TIMEOUT=60` (default), `BLOCKED_TASK_RETRY_DELAY=30` (default) |
| Auth | `ORCHESTRATOR_API_KEY` and `SERVICE_API_KEY` unset |
| Use case | `use-cases/germany-node`: `data-provider`, `villas-chronics`, `villas-node` (VILLASnode engine used by `villas-chronics`), `output-formatter` |

### Pipeline and name mapping

The orchestrator identifies services by the blueprint `container_name`. Docker
Compose uses the service name, which is also the hostname:

| Order | Orchestrator name (health API, `/recover`) | Compose service | URL |
|:-----:|---------------------|-----------------|-----|
| 1 | `data_provider1` | `data-provider` | `http://data-provider:8080` |
| 2 | `villas_chronics1` | `villas-chronics` (calls `villas-node` `/api/v2/restart`) | `http://villas-chronics:8080` |
| 3 | `output_formatter1` | `output-formatter` | `http://output-formatter:8080` |

`output-formatter` was chosen as the service to take down: it runs last, so
steps 1–2 complete first, and it has no dependency on `villas-node`.

### Terminals

| Terminal | Command | Purpose |
|----------|---------|---------|
| T1 | `docker compose logs -f --since 1s worker \| grep -E "SERVICE\|blocked\|half-open\|Processed\|Error"` (in `orchestrator/`) | Workers detect outages and emit `SERVICE DOWN` |
| T2 | `docker compose logs -f --since 1s api \| grep -vE "GET /orchestrator/health\|GET /workflows"` (in `orchestrator/`) | Recovery runs in the API process |
| T3 | `watch -n 2 "curl -s localhost:18000/orchestrator/health/services \| jq ..."` | Live health view |
| T4 | `watch -n 2 "curl ... /workflows/<WF> ...; curl ... /workflows/<WF>/tasks ..."` | Live workflow and task view |
| T5 | `use-cases/germany-node/` | Commands that change things |

---

## 2. Run 1: baseline, all services up

**Goal:** confirm the normal path still works with the new code.

```bash
./submit-workflow.sh          # → wf-705e62103e4f
```

**Observed**

```
worker-2 | 22:27:33,878 - INFO - worker_daemon - Processed task for workflow wf-705e62103e4f
worker-2 | 22:27:38,971 - INFO - worker_daemon - Processed task for workflow wf-705e62103e4f
worker-2 | 22:27:39,022 - INFO - worker_daemon - Processed task for workflow wf-705e62103e4f
```

- Workflow `completed` with no error. All 3 tasks `completed`.
- Health: `overall_status: HEALTHY`, `total: 3`, and all services `HEALTHY` / `closed` / `failure_count: 0`.
- T2: only `POST /workflows 200`.

**Verdict:** ✅ As expected. The new code does not change the normal path. The
services are registered at submission time and appear in the health API.

---

## 3. Run 2: `output-formatter` stopped, but `villas-node` crashed first

**Goal:** stop `output-formatter`, submit, and watch its task become `BLOCKED`.

```bash
docker compose stop output-formatter
./submit-workflow.sh          # → wf-ec4dd690d052
```

**Observed**

```
worker-3 | 22:35:05,997 - INFO  - worker_daemon - Processed task for workflow wf-ec4dd690d052
worker-3 | 22:35:11,054 - ERROR - orchestrator.alerts - SERVICE DOWN: villas_chronics1 at http://villas-chronics:8080
worker-3 | 22:35:11,055 - INFO  - worker_daemon - Processed task for workflow wf-ec4dd690d052
```

```
{"status":"failed","error":"Task task_91277dc9 failed: ('Connection broken: IncompleteRead(0 bytes read, 81 more expected)', ...)"}
{"node_key":"data_provider1:ProvideData","status":"completed","error":null}
{"node_key":"output_formatter1:FormatOutput","status":"pending","error":null}
{"node_key":"villas_chronics1:GenerateChronics","status":"failed","error":"('Connection broken: IncompleteRead(...)')"}
```

`villas-node` logs and container state:

```
22:35:06 info api:request:restart Restarting to /shared/wf-ec4dd690d052/villas_config.conf
22:35:06 info node             Received Terminated signal. Terminating...
22:35:06 info node:file        Stopping node file_node(file)
germany-node-villas-node-1   Exited (139)      restart=no
```

After `docker compose start output-formatter` and the recovery call:

```json
{"service_name": "output_formatter1", "status": "HEALTHY",
 "message": "Circuit breaker reset; service marked healthy. 0 blocked task(s) re-queued.",
 "resumed_tasks": 0}
```

T2: `SERVICE MANUAL RECOVERY: output_formatter1 at http://output-formatter:8080`.
The API `alerts.jsonl` contained the matching `SERVICE_MANUAL_RECOVERY` line.
The worker `alerts.jsonl` loop printed nothing.

### Analysis

1. `data_provider1` completed.
2. `villas_chronics1` asked `villas-node` to restart with the new workflow's
   config. `villas-node` **segfaulted** (exit code 139) during that restart. It
   has no restart policy, so it stayed down. Its first restart in run 1 had worked.
   This is a problem in the use case, not in the orchestrator.
3. `villas-chronics` itself was up. It received a truncated HTTP response and
   reported its task as `failed`, with the `IncompleteRead` error.
4. The code at that time counted a failure reported by a service against the
   breaker. With threshold 1, the circuit of `villas_chronics1` opened, and
   `SERVICE DOWN` was emitted **for a service that was actually reachable**.
   The task failed, so the workflow failed.
5. `output_formatter1` was never called, so it stayed `pending`. `resumed_tasks: 0`
   was therefore correct.
6. The worker `alerts.jsonl` looked empty only because the loop used
   `-f ../../orchestrator/docker-compose.yml` from inside `orchestrator/`,
   where that path does not exist. `2>/dev/null` hid the error. Run correctly,
   worker 3's file contained:

   ```json
   {"timestamp":"2026-09-16T22:35:11.053636+00:00","alert":"SERVICE_DOWN","service_name":"villas_chronics1",
    "failure_count":1,"failure_threshold":1,"last_error":"('Connection broken: IncompleteRead(0 bytes read, 81 more expected)', ...)"}
   ```

**Verdict:** ⚠️ The intended scenario was not reached. The run still confirmed:

- **detection:** `SERVICE DOWN` in the log and as JSON in `alerts.jsonl`;
- **health API:** `villas_chronics1` reported `UNHEALTHY` / `open` with
  `last_error` and `opened_at`;
- **manual recovery:** breaker reset, `manually_recovered_at` set,
  `SERVICE_MANUAL_RECOVERY` alert emitted.

It also exposed findings F1 and F2 below. Workflow `wf-ec4dd690d052` cannot be
resumed, because only `running` workflows are resumed.

### Remediation before run 3

```bash
docker compose up -d --force-recreate villas-node                                    # new villas-node container
curl -s -X POST localhost:18000/orchestrator/health/services/villas_chronics1/recover   # 22:45:37, clears the stale open circuit
```

---

## 4. Run 3: full scenario ✅

```bash
docker compose stop output-formatter
./submit-workflow.sh                         # → wf-99549bd89a68
# ... waited ~45 s ...
docker compose start output-formatter && sleep 3
curl -s -X POST localhost:18000/orchestrator/health/services/output_formatter1/recover | jq
```

### Observed

T1 (workers):

```
worker-1 | 22:46:01,832 - INFO    - worker_daemon - Processed task for workflow wf-99549bd89a68
worker-1 | 22:46:06,904 - INFO    - worker_daemon - Processed task for workflow wf-99549bd89a68
worker-1 | 22:46:06,936 - ERROR   - orchestrator.alerts - SERVICE DOWN: output_formatter1 at http://output-formatter:8080
worker-1 | 22:46:06,938 - WARNING - services.worker - Task task_2b4e324b of workflow wf-99549bd89a68 blocked, retry in 30.0s: Service 'output_formatter1' unavailable: Connection failed: [Errno 111] Connection refused. Task will be retried; recover via POST /orchestrator/health/services/output_formatter1/recover
worker-1 | 22:46:06,939 - INFO    - worker_daemon - Processed task for workflow wf-99549bd89a68
worker-2 | 22:46:36,958 - WARNING - services.worker - Task task_2b4e324b of workflow wf-99549bd89a68 blocked, retry in 30.0s: Circuit open for service 'output_formatter1' at http://output-formatter:8080 - service marked UNHEALTHY, call not attempted. Recover via POST /orchestrator/health/services/output_formatter1/recover
worker-2 | 22:46:36,962 - INFO    - worker_daemon - Processed task for workflow wf-99549bd89a68
worker-2 | 22:46:53,068 - INFO    - worker_daemon - Processed task for workflow wf-99549bd89a68
```

T2 (API):

```
api-1 | 22:46:52,659 - WARNING - orchestrator.alerts - SERVICE MANUAL RECOVERY: output_formatter1 at http://output-formatter:8080
api-1 | 22:46:52,663 - INFO    - services.service_registry - Resumed 1 blocked task(s) for service output_formatter1
api-1 | "POST /orchestrator/health/services/output_formatter1/recover HTTP/1.1" 200 OK
```

Recovery response:

```json
{"service_name": "output_formatter1", "status": "HEALTHY",
 "message": "Circuit breaker reset; service marked healthy. 1 blocked task(s) re-queued.",
 "resumed_tasks": 1}
```

Final state, queried from the API afterwards:

```
{"status":"completed","error":null,"created_at":"2026-09-16T22:46:01.619970Z","updated_at":"2026-09-16T22:46:53.067685Z"}
{"node_key":"data_provider1:ProvideData","status":"completed","error":null}
{"node_key":"output_formatter1:FormatOutput","status":"completed","error":null}
{"node_key":"villas_chronics1:GenerateChronics","status":"completed","error":null}

overall_status: HEALTHY
output_formatter1  HEALTHY closed failure_count=0  manually_recovered_at=22:46:52.659  last_success=22:46:53.064
villas_chronics1   HEALTHY closed failure_count=0  manually_recovered_at=22:45:37.930  last_success=22:46:06.901
data_provider1     HEALTHY closed failure_count=0                                      last_success=22:46:01.829
```

Worker 1 `alerts.jsonl`:

```json
{"timestamp":"2026-09-16T22:46:06.936397+00:00","alert":"SERVICE_DOWN","service_name":"output_formatter1","last_error":"Connection failed: [Errno 111] Connection refused"}
```

### Timeline

| Time | Event | Feature |
|------|-------|---------|
| 22:46:01.6 | Workflow submitted, services registered | 2 |
| 22:46:01.8 | `data_provider1` completed | — |
| 22:46:06.9 | `villas_chronics1` completed (the recreated `villas-node` worked) | — |
| 22:46:06.936 | `output_formatter1` call → `Connection refused` (transient) → circuit opened → `SERVICE DOWN` log line and JSON alert | 1 |
| 22:46:06.938 | Task `BLOCKED`, retry in 30 s; **workflow stays `running`** | 4 |
| 22:46:36.958 | Automatic retry (a different replica, worker 2). Circuit still open (60 s reset timeout not over), so no call; task blocked again. No duplicate alert. | 4 |
| 22:46:52.659 | Operator recovery: breaker reset, `SERVICE_MANUAL_RECOVERY` alert | 3 |
| 22:46:52.663 | 1 blocked task re-queued (4 ms after the reset) | 3, 4 |
| 22:46:53.068 | `output_formatter1` executed by worker 2; **the same workflow completed** | 4 |

### Checks

| Expected behaviour | Evidence | Result |
|--------------------|----------|:------:|
| An unreachable service is detected and a structured `SERVICE DOWN: {service} at {url}` alert is emitted | T1 22:46:06.936, worker 1 `alerts.jsonl` | ✅ |
| Health API reports the down service as `UNHEALTHY` with error details | T3 during the outage, `last_error: Connection refused` | ✅ |
| The task is `BLOCKED` instead of failed; the workflow stays `running` | T1 22:46:06.938, T4 | ✅ |
| While the circuit is open, retries skip the call and don't repeat the alert | T1 22:46:36.958 | ✅ |
| Blocked tasks are shared across replicas (a different worker retried) | worker 1 blocked, worker 2 retried and completed | ✅ |
| Recovery resets the breaker, records `manually_recovered_at` and emits an alert | T2, recovery response, final health | ✅ |
| Recovery re-queues the blocked task immediately | `resumed_tasks: 1`, 4 ms | ✅ |
| The same workflow completes without being resubmitted or restarting the orchestrator | `wf-99549bd89a68` `completed`; `last_success` 22:46:53 | ✅ |
| Already completed tasks are not executed again | `data_provider1` and `villas_chronics1` ran once each (T1) | ✅ |
| Service is `HEALTHY` again after recovery | final health view | ✅ |

**Verdict:** ✅ All four features work on the live stack.

- **Time from service restart to workflow completion:** ≈ 4 s (3 s `sleep` + 0.4 s).
- **Operator actions:** one API call.
- **Without the change:** the workflow would have failed at 22:46:06. It would
  then have needed a full resubmission, re-running `data_provider1` and
  `villas_chronics1` and restarting `villas-node` again, which is exactly what
  triggered the crash in run 2.

---

## 5. Run 4: same scenario through the browser, API key enabled ✅

**Goal:** repeat run 3 on the rebuilt image, with orchestrator authentication
turned on. This time the outage is observed and the recovery triggered through
the interactive API page (`/docs`) instead of reading logs and using curl.

### Setup differences from runs 1–3

| Item | Run 4 |
|------|-------|
| Orchestrator image | Rebuilt on 2026-09-17 by Task 3.3 German script `01-Start-Core-And-Germany-Services.ps1 -Build` from commit `8dd92c1`. It includes the F1 fix. |
| Code check | The API container contains `src/api/health_routes.py`, the compose working directory is `…/Thesis/ai-effect-wp3/orchestrator`, and `GET /orchestrator/health/services` without a key returns `401` (the old code would return `404`). |
| Orchestrator auth | `ORCHESTRATOR_API_KEY` set. Every call except `/health` needs `Authorization: Bearer <key>`. In `/docs` the key is entered once with **Authorize**. |
| Service auth | None. At 14:27:39 the germany-node services were recreated by a foreground `docker compose up --build` in a shell without `SERVICE_API_KEY` (see F8). |
| Submission | `submit-workflow.sh` sends no API key (F9), so the workflow was submitted with curl and the `Authorization` header. |
| `villas-node` | Recreated before the run (F2). |

### Steps

```bash
# terminal in use-cases/germany-node, with ORCHESTRATOR_API_KEY exported
docker compose up -d --force-recreate villas-node
docker compose stop output-formatter
PAYLOAD=$(jq -n --argjson b "$(cat export/blueprint.json)" --argjson d "$(cat export/dockerinfo.json)" \
  '{blueprint:$b, dockerinfo:$d}')
curl -s -X POST localhost:18000/workflows -H "Authorization: Bearer $ORCHESTRATOR_API_KEY" \
  -H 'Content-Type: application/json' -d "$PAYLOAD" | jq          # → wf-100f8649e062
# browser (/docs, authorized): GET /orchestrator/health/services, GET /workflows/{id}, GET /workflows/{id}/tasks
docker compose start output-formatter && sleep 3
# browser: POST /orchestrator/health/services/output_formatter1/recover, then the three GETs again
```

The first submission attempt printed `null` as the workflow id. It ran in a
terminal where `ORCHESTRATOR_API_KEY` was not exported, so the API correctly
answered `401 {"detail":"Unauthorized"}` and no workflow was created.

### Observed

Worker log:

```
worker-3 | 14:31:07,604 - INFO    - worker_daemon - Processed task for workflow wf-100f8649e062
worker-3 | 14:31:12,659 - INFO    - worker_daemon - Processed task for workflow wf-100f8649e062
worker-3 | 14:31:12,680 - ERROR   - orchestrator.alerts - SERVICE DOWN: output_formatter1 at http://output-formatter:8080
worker-3 | 14:31:12,681 - WARNING - services.worker - Task task_2b4e324b of workflow wf-100f8649e062 blocked, retry in 30.0s: Service 'output_formatter1' unavailable: Connection failed: [Errno 111] Connection refused. ...
worker-3 | 14:31:42,785 - WARNING - services.worker - Task task_2b4e324b of workflow wf-100f8649e062 blocked, retry in 30.0s: Circuit open for service 'output_formatter1' at http://output-formatter:8080 - service marked UNHEALTHY, call not attempted. ...
```

Service logs (foreground compose): `data-provider` executed at 14:31:07.595.
`villas-chronics` restarted `villas-node` with the workflow config at 14:31:07.63,
and all 5 output files were present at 14:31:09.63. This first restart of the
recreated container did not crash.

**During the outage**, `GET /orchestrator/health/services`:

```json
{"overall_status": "UNHEALTHY", "total": 3, "healthy": 2, "degraded": 0, "unhealthy": 1,
 "services": [
  {"service_name": "output_formatter1", "status": "UNHEALTHY", "circuit_state": "open",
   "failure_count": 4, "last_failure": "2026-09-17T14:34:13.190043+00:00",
   "last_error": "Connection failed: [Errno 111] Connection refused",
   "opened_at": "2026-09-17T14:34:13.190043+00:00", "retry_after": "2026-09-17T14:35:13.190056+00:00", "...": "..."},
  {"service_name": "data_provider1", "status": "HEALTHY", "last_success": "2026-09-17T14:31:07.602512+00:00", "...": "..."},
  {"service_name": "villas_chronics1", "status": "HEALTHY", "last_success": "2026-09-17T14:31:12.657521+00:00", "...": "..."}]}
```

`GET /workflows/wf-100f8649e062` → `"status": "running"`, `"error": null`.

`GET /workflows/wf-100f8649e062/tasks` (queried later than the health view):

| Task | Status | Updated | Error |
|------|--------|---------|-------|
| `data_provider1:ProvideData` | `completed` | 14:31:07.60 | — |
| `villas_chronics1:GenerateChronics` | `completed` | 14:31:12.66 | — |
| `output_formatter1:FormatOutput` | `blocked` | 14:38:13.80 | `Service 'output_formatter1' unavailable: Connection failed: [Errno 111] Connection refused. Task will be retried; recover via POST /orchestrator/health/services/output_formatter1/recover` |

**Recovery**, `POST /orchestrator/health/services/output_formatter1/recover` at 14:40:56:

```json
{"service_name": "output_formatter1", "status": "HEALTHY",
 "message": "Circuit breaker reset; service marked healthy. 1 blocked task(s) re-queued.",
 "resumed_tasks": 1}
```

**After recovery:**

- `GET /workflows/wf-100f8649e062` → `"status": "completed"`, `"updated_at": "2026-09-17T14:40:57.698426Z"`, `"error": null`.
- All three tasks `completed`. `output_formatter1` updated at 14:40:57.70, `error: null`, output `/shared/wf-100f8649e062/final_output` (csv).
- Health: `overall_status: HEALTHY`, 3/3 healthy. `output_formatter1`: `closed`, `failure_count: 0`, `manually_recovered_at` 14:40:56.834770, `last_success` 14:40:57.697324.
- The same response was returned by `curl` in a terminal with the `Authorization` header.

The `404` and `422` blocks shown under each endpoint in `/docs` are Swagger's
documentation of *possible* responses, with placeholder values. They are not
responses that were received. Every call in this run returned `200`, except the
unauthenticated submission noted above (`401`).

### Timeline

| Time | Event | Feature |
|------|-------|---------|
| 14:31:06.68 | Workflow submitted with the API key; services registered | 2, auth |
| 14:31:07.60 | `data_provider1` completed | — |
| 14:31:12.66 | `villas_chronics1` completed | — |
| 14:31:12.680 | `output_formatter1` → `Connection refused` → circuit opened → `SERVICE DOWN` | 1 |
| 14:31:12.681 | Task `BLOCKED`, workflow stays `running` | 4 |
| 14:31:42.785 | Retry: circuit still open, no call, no new alert | 4 |
| until 14:34:13.19 | Test calls after each 60 s reset timeout failed; `failure_count` reached 4 and the circuit re-opened each time (`opened_at` moved to 14:34:13) | 1 |
| 14:38:13.80 | Task retried again, still unavailable, blocked again | 4 |
| 14:40:56.83 | Operator recovery through `/docs` → breaker reset, 1 task re-queued | 3 |
| 14:40:57.70 | `output_formatter1` completed; **same workflow completed** | 4 |

### Checks

| Expected behaviour | Evidence | Result |
|--------------------|----------|:------:|
| Health and workflow endpoints require the API key; `/health` and `/docs` stay open | `401` without key (submission, and the code check), `200` after **Authorize** | ✅ |
| Outage detected and alerted | worker log 14:31:12.680 | ✅ |
| Outage visible without reading logs | `/docs` health view: `UNHEALTHY`, `open`, `last_error`, `opened_at`, `retry_after` | ✅ |
| Workflow survives a ~10 min outage | `running` from 14:31:06 to 14:40:56; task `blocked` with an explanatory error | ✅ |
| Automatic test calls continue while the service is down | `failure_count: 4`, `opened_at` updated to 14:34:13, task updated at 14:38:13 | ✅ (see F10) |
| Recovery from the browser resumes the task | `resumed_tasks: 1` | ✅ |
| Same workflow completes, finished tasks not re-run | `completed` at 14:40:57.70; tasks 1–2 keep their 14:31 timestamps | ✅ |
| Browser and terminal show the same state | identical `GET /orchestrator/health/services` responses | ✅ |

**Verdict:** ✅ Same outcome as run 3, now with authentication enabled and
without reading any logs.

- **Recovery call to workflow completion:** 0.87 s.
- **Outage length:** ≈ 9 min 44 s (first failure 14:31:12 → recovery 14:40:56).
- **Operator actions:** one API call.
- **Tasks re-executed:** 0.
- **Not exercised here:** F1 (no service reported a failure), and alert delivery
  outside the containers (F3).

---

## 6. Findings

| ID | Finding | Status |
|----|---------|--------|
| **F1** | A failure *reported by the service* (`status: failed`) counted against the breaker. In run 2 this marked the reachable `villas_chronics1` as `UNHEALTHY` and blocked future calls to it for up to 60 s. | **Fixed after the run.** Only transient errors (connection failure, timeout, HTTP 5xx) now count against the breaker. Service-reported failures, 4xx and invalid responses fail the task without touching the breaker (`src/services/worker.py`, with new tests in `tests/unit/test_worker.py`). Included in the image rebuilt on 2026-09-17 (commit `8dd92c1`, used in run 4), but not yet exercised live (see V-A6). |
| **F2** | `villas-node` segfaults (exit 139) when asked to restart a second time, and has `restart: no`. | Use-case issue. Workaround: `docker compose up -d --force-recreate villas-node` before each run. Better: `restart: unless-stopped` in `use-cases/germany-node/docker-compose.yml`, and investigate the crash. |
| **F3** | `alerts.jsonl` is written inside each container. It is split across replicas and lost when containers are recreated. | Open. A delivery path to TEF has to be chosen (see V-B9). |
| **F4** | `Processed task for workflow …` is also logged when a task was only blocked, which is misleading in the logs. | Open, cosmetic. The daemon could log the outcome instead. |
| **F5** | The API container does not set `CIRCUIT_*`, so it uses threshold 3 while the workers use 1. Not visible in these runs. | Open. Set the same values on both services. |
| **F6** | A workflow that already failed (`wf-ec4dd690d052`) cannot be resumed by recovery. | Known limitation (see SERVICE_HEALTH.md). |
| **F7** | Manual check instructions referenced a relative compose path that only works from `use-cases/<name>/`. | Documentation. From `orchestrator/`, use `docker compose exec --index N worker ...` without `-f`. |
| **F8** | Recreating the use-case services from a shell without `SERVICE_API_KEY` (run 4: foreground `docker compose up --build`) silently turns off orchestrator-to-service authentication. | Operational. Recreate services only with the key exported; before further Task 3.3 scripts, rerun German script 01. |
| **F9** | `use-cases/germany-node/submit-workflow.sh` sends neither the orchestrator key nor `services_api_key`, so it cannot submit once `ORCHESTRATOR_API_KEY` is set. | Open. Add optional `Authorization` header and `services_api_key` from the environment. |
| **F10** | During a long outage, every failed test call re-opens the circuit and emits another `SERVICE DOWN` (run 4: 4 failures in ~3 min before the operator acted). | Expected with the current design; input for V-B1 (alert throttling). |

---

## 7. Further testing for a definitive verification

Runs 1–4 prove the features work in the main scenario on the new code. They
do not yet prove:

- **(a)** how the old code behaves in the same situations. This is needed to
  claim an improvement with evidence rather than by reasoning.
- **(b)** how the new code behaves in less favourable situations. This decides
  which further modifications are needed.

### 7.0 Prerequisites

1. **Rebuild the new stack** so that F1 is included:
   `cd orchestrator && docker compose up -d --build`. Done on 2026-09-17 (run 4).
2. **Old stack side by side.** Done on 2026-09-17 (§8). The old and new orchestrators both use the compose
   project name `orchestrator`, so they would share and overwrite
   container and image names. Use a separate project name for the old one, and
   never run both at once, because both bind port 18000:

   ```bash
   # switch to OLD
   (cd ~/University/Thesis/ai-effect-wp3/orchestrator && docker compose down)
   (cd ~/University/Thesis/oldCode/ai-effect-wp3/orchestrator && docker compose -p orch-old up -d --build)
   # switch back to NEW
   (cd ~/University/Thesis/oldCode/ai-effect-wp3/orchestrator && docker compose -p orch-old down)
   (cd ~/University/Thesis/ai-effect-wp3/orchestrator && docker compose up -d)
   ```

   Each `down` removes that stack's Redis container, so every switch starts
   from empty state.
3. **Clean use-case state before each scenario:** recreate `villas-node` (F2).
   Use `file_based_energy_pipeline` for scenarios that need many workflows at
   once, because the single `villas-node` instance cannot serve concurrent
   workflows.
4. **Record for every scenario:**
   - final workflow status;
   - number of operator actions;
   - time from service restoration to workflow completion;
   - number of tasks executed more than once;
   - alerts emitted (count and type);
   - whether the outage was visible without reading raw logs.

### 7.A Old vs new comparison

| ID | Scenario | How | Expected OLD | Expected NEW | Proves |
|----|----------|-----|--------------|--------------|--------|
| **V-A1** ✅ done (§8) | Service down before submission (= runs 3, 4) | stop `output-formatter`, submit, restart it, (NEW: recover) | Workflow `failed` at step 3; outage only visible in the task error; recovery = resubmit, which re-runs steps 1–2 | Runs 3–4 result | Features 1–4 |
| **V-A2** | Service dies **during** a long-running task | submit; while `villas_chronics1` is `running` (it polls for output files), `docker compose kill villas-chronics`; then start it | Workflow `failed` (polling error) | Task `BLOCKED` → re-executed from the start after recovery → `completed` | Blocking of in-flight tasks; **safe re-execution** (see V-B8) |
| **V-A3** | Short outage, no operator | stop `output-formatter`, submit, start it again within ~20 s, do **not** call recover | `failed` | Retry every 30 s. After the 60 s reset timeout, a test call closes the circuit (`SERVICE RECOVERED` alert) → `completed` within ≈ 60–90 s | Automatic recovery |
| **V-A4** 🟡 partly done (§8) | Idle-worker hang (fix 5) | all services up; submit 5–10 workflows one after another, waiting for each to finish; then check `docker compose exec redis redis-cli CLIENT LIST` for connections blocked on `brpop` | Hypothesis: workers stay blocked on the empty queue of a finished workflow; later workflows stay `running`/`pending` once all 3 replicas are stuck | All workflows complete; no worker stuck in `brpop` | Fix 5 (confirms the bug existed) |
| **V-A5** ✅ done (§8) | Visibility | `curl localhost:18000/orchestrator/health/services`, check the logs during V-A1 | `404`; no alert; only the workflow error | Health report and alerts | Features 1–2 |
| **V-A6** | Service-reported failure (= run 2) | do **not** recreate `villas-node` after a run; submit | `failed` | `failed`; after F1, `villas_chronics1` stays `HEALTHY` / `closed`, no `SERVICE DOWN` | No regression; F1 fix |
| **V-A7** | Request error (4xx) | submit a blueprint with a wrong operation name for one service | `failed` | `failed`; breaker untouched | No behaviour change for request errors |

V-A4 was the only one whose OLD result was a hypothesis. §8 confirms the
underlying bug (`Timeout reading from socket` every ~6 s on OLD, absent on NEW);
what remains is the controlled variant that also shows later workflows stalling.

### 7.B New-code robustness (decides further modifications)

| ID | Scenario | How | What to observe | Possible modification if it fails |
|----|----------|-----|-----------------|-----------------------------------|
| **V-B1** 🟡 partly done (run 4, F10) | Long outage (≥ 5 min), no recovery | stop a service, submit, wait | Number of `SERVICE DOWN` alerts (expected one every ~60–90 s, one per failed test call); the workflow stays `running` forever | Alert de-duplication ("still down" throttling); a maximum blocked time after which the task fails (`BLOCKED_TASK_MAX_AGE`) |
| **V-B2** | Recover while the service is still down | call `/recover` without starting the service | Circuit re-opens on the next call, new `SERVICE DOWN`, task blocked again, workflow not failed | Optionally check the service's `/health` before accepting the recovery |
| **V-B3** | Concurrency: many workflows blocked on one service | file-based pipeline, stop `data-analyzer`, submit 10 workflows quickly, recover | Exactly one `SERVICE DOWN` alert? `failure_count` consistent? `resumed_tasks == 10`? | **Atomic breaker updates** (Redis Lua, plan step 2) if duplicates or lost counts appear |
| **V-B4** | More than 20 workflows blocked | as V-B3 with 25 workflows | `resumed_tasks` (the registry keeps only the last 20 workflow ids, so expected 20; the rest resume on their next retry) | Index blocked tasks per service instead of via the registry's workflow list |
| **V-B5** | Worker restart during an outage | with a task blocked: `docker compose restart worker` | Task is still retried and resumed after recovery (state is in Redis) | — |
| **V-B6** | API restart during an outage | `docker compose restart api` | Health state is preserved; recover still works | — |
| **V-B7** | Redis restart during an outage | `docker compose restart redis` | All workflow and health state is lost, because Redis has no volume (same for OLD) | Redis persistence (AOF plus a volume) |
| **V-B8** | Re-execution safety (with V-A2) | inspect the shared volume and output files after V-A2 | Duplicate or partial outputs? `villas-node` restarted twice → F2 crash? | Service-side idempotency per `task_id`; restart policy for `villas-node` |
| **V-B9** | Alert delivery to TEF | mount `/app/src/logs` on a host volume for all workers; cause an outage | 3 replicas writing and rotating the same `alerts.jsonl` → interleaved lines, broken rotation | Per-replica file names (hostname), or deliver alerts via stdout JSON, a Redis stream or a webhook |
| **V-B10** 🟡 partly done (run 4) | Authentication | set `ORCHESTRATOR_API_KEY`, restart | `401` on all `/orchestrator/health/...` routes without the header, `200` with it. **Partly done in run 4** (`GET` health list and `POST /workflows`); the per-service `GET` and `recover` without a key are still to check. | — |
| **V-B11** | Test calls after the timeout with several replicas | V-B3 setup; wait > 60 s without starting the service; count requests in the service logs | More than one test call per timeout window | Single-probe lock (part of plan step 2) |
| **V-B12** | Worker killed mid-task | `docker kill orchestrator-worker-1` while it runs `villas_chronics1` | Task stays `running` forever (same in OLD; not addressed by this change) | Task lease / heartbeat with re-queue (separate improvement) |
| **V-B13** | Config consistency (F5) | set `CIRCUIT_*` on `api` as well; repeat V-A1 | `retry_after` and `DEGRADED` match the workers' view | Add the variables to the `api` service in `docker-compose.yml` |
| **V-B14** | Overhead | 20 workflows, all services up, OLD vs NEW total time | Extra Redis calls per task should not add noticeable time | — |
| **V-B15** ✅ done (483 unit + health API, 93 integration, 2026-09-16) | Automated suites | `./run-tests.sh tests/unit tests/integration`; `tests/e2e` with the use-case services running | All green | — |

### 7.C Suggested order

1. ~~**V-B15** after rebuilding, to confirm the F1 change on the live image.~~ ✅ done
2. **V-A4** on OLD, then NEW (confirms or refutes fix 5). 🟡 evidence from §8; the 5-workflow file-based variant is still open.
3. ~~**V-A1, V-A5** on OLD.~~ ✅ done (§8). **V-A6, V-A7** on OLD still open.
4. **V-A2, V-A3, V-A6, V-A7** on NEW.
5. **V-B1, V-B2, V-B3, V-B11** together (one outage, many workflows) to decide on plan step 2 and alert throttling.
6. **V-B5 – V-B9, V-B12** (restarts, persistence, delivery).
7. **V-B4, V-B10, V-B13, V-B14.**

### 7.D Expected outcome

| Improvement | Evidence so far | Needed to call it verified |
|-------------|-----------------|---------------------------|
| 1. Service health detection and TEF-ready alerts | Runs 2–4; OLD baseline §8 (no alert at all) | V-B1 (alert volume), V-B9 (delivery) |
| 2. Health status API | Runs 1–4 (run 4 with auth); OLD returns `404` §8 | V-B10 (per-service routes) |
| 3. Manual recovery API | Runs 2–4; §8 shows the OLD alternative (resubmit) | V-B2, V-B3/V-B4 (several workflows) |
| 4. Workflows survive outages | Runs 3–4; **§8 measured against OLD** | V-A2, V-A3, V-B5 |
| 5. Idle workers no longer hang | Test suite; OLD `Timeout reading from socket` loop observed §8 | V-A4 in the controlled 5-workflow form |
| F1. Only transient errors count against the breaker | Unit tests; in the running image since run 4 | V-A6 on the rebuilt NEW stack |

---

## 8. Old versus new, measured (2026-09-17)

The same outage was run against the frozen baseline `486ccc7`
(`~/University/Thesis/oldCode/ai-effect-wp3`, which is exactly the commit the
Task 3.3 suites pin) and compared with run 4 on `8dd92c1`. Same machine, same
`germany-node` services, same stopped service (`output-formatter`).

### Method

```bash
# OLD stack, separate compose project, same port 18000
(cd ~/University/Thesis/ai-effect-wp3/orchestrator && docker compose down)
(cd ~/University/Thesis/oldCode/ai-effect-wp3/orchestrator && docker compose -p orch-old up -d --build)

# services, outage, submission (no API key on this stack)
cd ~/University/Thesis/ai-effect-wp3/use-cases/germany-node
docker compose up -d && docker compose up -d --force-recreate villas-node
docker compose stop output-formatter
PAYLOAD=$(jq -n --argjson b "$(cat export/blueprint.json)" --argjson d "$(cat export/dockerinfo.json)" \
  '{blueprint:$b, dockerinfo:$d}')
curl -s -X POST localhost:18000/workflows -H 'Content-Type: application/json' -d "$PAYLOAD" | jq -r .workflow_id
```

Which code is live is verified with `GET /orchestrator/health/services`:
`404` on OLD, `401`/`200` on NEW.

### OLD: first workflow `wf-21e7b1509412`

| Time | Event |
|------|-------|
| 18:29:40.05 | Submitted |
| 18:29:40.25 | `data_provider1` completed |
| 18:29:45.37 | `villas_chronics1` completed |
| 18:29:45.44 | `output_formatter1` **failed**: `Connection failed: [Errno 111] Connection refused` → **workflow `failed`** after 5.4 s |

```json
{"workflow_id":"wf-21e7b1509412","status":"failed",
 "error":"Task task_2b4e324b failed: Connection failed: [Errno 111] Connection refused"}
```

- `GET /orchestrator/health/services` → **404** (the route does not exist).
- No `SERVICE DOWN` line anywhere in the worker logs.
- The two completed tasks and their outputs are lost for this workflow.

### OLD: recovery means resubmitting — `wf-cbc47cf728eb`

`output-formatter` was started again, and the same payload resubmitted (a new
workflow id, as the old code cannot resume a failed workflow).

| Time | Event |
|------|-------|
| 18:32:00.29 | Resubmitted |
| 18:32:00.59 | `data_provider1` **executed a second time** |
| 18:34:01.04 | `villas_chronics1` **failed**: `Timeout waiting for VILLASnode output. Missing: ['load_p.csv', 'load_q.csv', 'prod_p.csv', 'prod_q.csv', 'prod_v.csv']` → workflow `failed` again |

The cause is F2: re-running the workflow made `villas-chronics` restart
VILLASnode a second time, and the container segfaulted (`Exited (139)`). So on
the old code the outage of *one* service produced a *second*, unrelated failure
through the recovery procedure itself. `output_formatter1`, the service that had
actually been down, never even ran.

### OLD: stuck workers (fix 5)

While `wf-cbc47cf728eb` was running, two of the three workers logged, every
~6 seconds from 18:32:05 to 18:34:05:

```
worker-1 | 18:32:05,633 - ERROR - worker_daemon - Error processing workflow wf-cbc47cf728eb: Timeout reading from socket
worker-2 | 18:32:05,864 - ERROR - worker_daemon - Error processing workflow wf-cbc47cf728eb: Timeout reading from socket
```

That is `dequeue_task(timeout=0)` → Redis `BRPOP` with "wait forever", hitting
the client socket timeout and retrying. Only worker 3 did any work. These
messages do not occur on the new stack, where an empty queue returns
immediately (`RPOP`).

### Side by side

| | OLD `486ccc7` | NEW `8dd92c1` (run 4) |
|---|---|---|
| Workflow while the service is down | **`failed`** after 5.4 s | **`running`**, task `blocked` |
| Task state | `output_formatter1` `failed`; finished work lost | `output_formatter1` `blocked`, error names the recover endpoint |
| Outage visibility | task error only; health API **404** | `UNHEALTHY`, `open`, `last_error`, `opened_at`, `retry_after` |
| Alert for monitoring | none | `SERVICE DOWN …` in the log and as a JSON line |
| Automatic retry | none | every 30 s, plus a test call after each 60 s timeout |
| Recovery | resubmit the whole workflow (new id) | one `POST …/recover`, same workflow id |
| Tasks re-executed | 2 (`data_provider1`, `villas_chronics1`) | **0** |
| Outcome after recovery | **failed again** (VILLASnode crash caused by the re-run) | `completed` **0.87 s** after the recover call |
| Operator actions | start service, resubmit, diagnose, recreate `villas-node`, resubmit again | start service, one API call |
| Worker log noise | `Timeout reading from socket` every ~6 s from 2 of 3 workers | none |

### Verdict

- **V-A1 (outage handling)** ✅ The old code fails the workflow; the new code
  keeps it running and finishes it after one call.
- **V-A5 (visibility)** ✅ The old code has no health API (`404`) and emits no
  alert; the outage is only visible as a task error after the fact.
- **V-A4 (stuck workers)** 🟡 The `Timeout reading from socket` loop is direct
  evidence of the blocking `BRPOP` on the old code, and it is absent on the new
  one. The controlled 5-workflow variant (counting `brpop` clients and checking
  that later workflows still start) has not been run; when sampled afterwards
  the count was 0, because all workflows had ended.

### Caveats

- The old stack ran **without `ORCHESTRATOR_API_KEY`** (started in a shell
  without it). The old code supports the key, so this is a setup difference,
  not a missing feature.
- The second old-code failure depends on F2 (the VILLASnode restart crash),
  which is a use-case defect. It is reported here because resubmission is what
  triggers it, and resubmission is the old code's only recovery path.
- Both stacks use compose project names that bind port 18000, so only one can
  run at a time. Every switch also replaces the Redis container, so each side
  started from empty state.
