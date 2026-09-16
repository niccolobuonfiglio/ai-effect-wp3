# Run Analysis: Service Health Features on `germany-node`

Manual end-to-end runs of the features described in
[SERVICE_HEALTH.md](SERVICE_HEALTH.md), performed on the live Docker stack with
the `use-cases/germany-node` pipeline. The document records what was run, what
was observed, how it compares with the expected behaviour, and what still has
to be tested to fully verify the improvements over the old code
(`oldCode/ai-effect-wp3`).

**Date:** 2026-09-16. All times are UTC, as printed by the containers.
**Result:** ✅ The full scenario (service down → task blocked → workflow kept
running → manual recovery → same workflow completed) was verified in run 3. Run 2
found a design issue, which was fixed afterwards (see [Findings](#findings)).

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

## 5. Findings

| ID | Finding | Status |
|----|---------|--------|
| **F1** | A failure *reported by the service* (`status: failed`) counted against the breaker. In run 2 this marked the reachable `villas_chronics1` as `UNHEALTHY` and blocked future calls to it for up to 60 s. | **Fixed after the run.** Only transient errors (connection failure, timeout, HTTP 5xx) now count against the breaker. Service-reported failures, 4xx and invalid responses fail the task without touching the breaker (`src/services/worker.py`, with new tests in `tests/unit/test_worker.py`). **The running stack must be rebuilt** (`docker compose up -d --build` in `orchestrator/`) to pick this up. |
| **F2** | `villas-node` segfaults (exit 139) when asked to restart a second time, and has `restart: no`. | Use-case issue. Workaround: `docker compose up -d --force-recreate villas-node` before each run. Better: `restart: unless-stopped` in `use-cases/germany-node/docker-compose.yml`, and investigate the crash. |
| **F3** | `alerts.jsonl` is written inside each container. It is split across replicas and lost when containers are recreated. | Open. A delivery path to TEF has to be chosen (see V-B9). |
| **F4** | `Processed task for workflow …` is also logged when a task was only blocked, which is misleading in the logs. | Open, cosmetic. The daemon could log the outcome instead. |
| **F5** | The API container does not set `CIRCUIT_*`, so it uses threshold 3 while the workers use 1. Not visible in these runs. | Open. Set the same values on both services. |
| **F6** | A workflow that already failed (`wf-ec4dd690d052`) cannot be resumed by recovery. | Known limitation (see SERVICE_HEALTH.md). |
| **F7** | Manual check instructions referenced a relative compose path that only works from `use-cases/<name>/`. | Documentation. From `orchestrator/`, use `docker compose exec --index N worker ...` without `-f`. |

---

## 6. Further testing for a definitive verification

Runs 1–3 prove the features work in the main scenario on the new code. They
do not yet prove:

- **(a)** how the old code behaves in the same situations. This is needed to
  claim an improvement with evidence rather than by reasoning.
- **(b)** how the new code behaves in less favourable situations. This decides
  which further modifications are needed.

### 6.0 Prerequisites

1. **Rebuild the new stack** so that F1 is included:
   `cd orchestrator && docker compose up -d --build`.
2. **Old stack side by side.** The old and new orchestrators both use the compose
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

### 6.A Old vs new comparison

| ID | Scenario | How | Expected OLD | Expected NEW | Proves |
|----|----------|-----|--------------|--------------|--------|
| **V-A1** | Service down before submission (= run 3) | stop `output-formatter`, submit, restart it, (NEW: recover) | Workflow `failed` at step 3; outage only visible in the task error; recovery = resubmit, which re-runs steps 1–2 | Run 3 result | Features 1–4 |
| **V-A2** | Service dies **during** a long-running task | submit; while `villas_chronics1` is `running` (it polls for output files), `docker compose kill villas-chronics`; then start it | Workflow `failed` (polling error) | Task `BLOCKED` → re-executed from the start after recovery → `completed` | Blocking of in-flight tasks; **safe re-execution** (see V-B8) |
| **V-A3** | Short outage, no operator | stop `output-formatter`, submit, start it again within ~20 s, do **not** call recover | `failed` | Retry every 30 s. After the 60 s reset timeout, a test call closes the circuit (`SERVICE RECOVERED` alert) → `completed` within ≈ 60–90 s | Automatic recovery |
| **V-A4** | Idle-worker hang (fix 5) | all services up; submit 5–10 workflows one after another, waiting for each to finish; then check `docker compose exec redis redis-cli CLIENT LIST` for connections blocked on `brpop` | Hypothesis: workers stay blocked on the empty queue of a finished workflow; later workflows stay `running`/`pending` once all 3 replicas are stuck | All workflows complete; no worker stuck in `brpop` | Fix 5 (confirms the bug existed) |
| **V-A5** | Visibility | `curl localhost:18000/orchestrator/health/services`, check the logs during V-A1 | `404`; no alert; only the workflow error | Health report and alerts | Features 1–2 |
| **V-A6** | Service-reported failure (= run 2) | do **not** recreate `villas-node` after a run; submit | `failed` | `failed`; after F1, `villas_chronics1` stays `HEALTHY` / `closed`, no `SERVICE DOWN` | No regression; F1 fix |
| **V-A7** | Request error (4xx) | submit a blueprint with a wrong operation name for one service | `failed` | `failed`; breaker untouched | No behaviour change for request errors |

V-A4 is the only one whose OLD result is a hypothesis rather than an
expectation. It should be run first, because a positive result is an
additional, independent improvement.

### 6.B New-code robustness (decides further modifications)

| ID | Scenario | How | What to observe | Possible modification if it fails |
|----|----------|-----|-----------------|-----------------------------------|
| **V-B1** | Long outage (≥ 5 min), no recovery | stop a service, submit, wait | Number of `SERVICE DOWN` alerts (expected one every ~60–90 s, one per failed test call); the workflow stays `running` forever | Alert de-duplication ("still down" throttling); a maximum blocked time after which the task fails (`BLOCKED_TASK_MAX_AGE`) |
| **V-B2** | Recover while the service is still down | call `/recover` without starting the service | Circuit re-opens on the next call, new `SERVICE DOWN`, task blocked again, workflow not failed | Optionally check the service's `/health` before accepting the recovery |
| **V-B3** | Concurrency: many workflows blocked on one service | file-based pipeline, stop `data-analyzer`, submit 10 workflows quickly, recover | Exactly one `SERVICE DOWN` alert? `failure_count` consistent? `resumed_tasks == 10`? | **Atomic breaker updates** (Redis Lua, plan step 2) if duplicates or lost counts appear |
| **V-B4** | More than 20 workflows blocked | as V-B3 with 25 workflows | `resumed_tasks` (the registry keeps only the last 20 workflow ids, so expected 20; the rest resume on their next retry) | Index blocked tasks per service instead of via the registry's workflow list |
| **V-B5** | Worker restart during an outage | with a task blocked: `docker compose restart worker` | Task is still retried and resumed after recovery (state is in Redis) | — |
| **V-B6** | API restart during an outage | `docker compose restart api` | Health state is preserved; recover still works | — |
| **V-B7** | Redis restart during an outage | `docker compose restart redis` | All workflow and health state is lost, because Redis has no volume (same for OLD) | Redis persistence (AOF plus a volume) |
| **V-B8** | Re-execution safety (with V-A2) | inspect the shared volume and output files after V-A2 | Duplicate or partial outputs? `villas-node` restarted twice → F2 crash? | Service-side idempotency per `task_id`; restart policy for `villas-node` |
| **V-B9** | Alert delivery to TEF | mount `/app/src/logs` on a host volume for all workers; cause an outage | 3 replicas writing and rotating the same `alerts.jsonl` → interleaved lines, broken rotation | Per-replica file names (hostname), or deliver alerts via stdout JSON, a Redis stream or a webhook |
| **V-B10** | Authentication | set `ORCHESTRATOR_API_KEY`, restart | `401` on all `/orchestrator/health/...` routes without the header, `200` with it | — |
| **V-B11** | Test calls after the timeout with several replicas | V-B3 setup; wait > 60 s without starting the service; count requests in the service logs | More than one test call per timeout window | Single-probe lock (part of plan step 2) |
| **V-B12** | Worker killed mid-task | `docker kill orchestrator-worker-1` while it runs `villas_chronics1` | Task stays `running` forever (same in OLD; not addressed by this change) | Task lease / heartbeat with re-queue (separate improvement) |
| **V-B13** | Config consistency (F5) | set `CIRCUIT_*` on `api` as well; repeat V-A1 | `retry_after` and `DEGRADED` match the workers' view | Add the variables to the `api` service in `docker-compose.yml` |
| **V-B14** | Overhead | 20 workflows, all services up, OLD vs NEW total time | Extra Redis calls per task should not add noticeable time | — |
| **V-B15** | Automated suites | `./run-tests.sh tests/unit tests/integration`; `tests/e2e` with the use-case services running | All green | — |

### 6.C Suggested order

1. **V-B15** after rebuilding, to confirm the F1 change on the live image.
2. **V-A4** on OLD, then NEW (confirms or refutes fix 5).
3. **V-A1, V-A5, V-A6, V-A7** on OLD. The NEW results for A1 and A5 are already in this document.
4. **V-A2, V-A3, V-A6, V-A7** on NEW.
5. **V-B1, V-B2, V-B3, V-B11** together (one outage, many workflows) to decide on plan step 2 and alert throttling.
6. **V-B5 – V-B9, V-B12** (restarts, persistence, delivery).
7. **V-B4, V-B10, V-B13, V-B14.**

### 6.D Expected outcome

| Improvement | Evidence so far | Needed to call it verified |
|-------------|-----------------|---------------------------|
| 1. Service health detection and TEF-ready alerts | Runs 2–3 | V-A5 (OLD baseline), V-B1 (alert volume), V-B9 (delivery) |
| 2. Health status API | Runs 1–3 | V-A5, V-B10 |
| 3. Manual recovery API | Runs 2–3 | V-B2, V-B3/V-B4 (several workflows) |
| 4. Workflows survive outages | Run 3 | V-A1 on OLD, V-A2, V-A3, V-B5 |
| 5. Idle workers no longer hang | Test suite only | V-A4 on OLD and NEW |
| F1. Only transient errors count against the breaker | Unit tests | V-A6 on the rebuilt NEW stack |
