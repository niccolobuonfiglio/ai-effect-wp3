# Evaluation protocol: original vs modified orchestrator

This is the method behind the thesis chapter "Experimental evaluation". It
fixes the setup, the questions, the scenarios, the metrics and the analysis
*before* the measurements, so that the comparison is not tuned after the fact.
Every number in the chapter must come from a file produced by the scripts in
this folder.

## 1. Object of study and design

| | OLD (baseline) | NEW (this work) |
|---|---|---|
| Code | `oldCode/ai-effect-wp3` @ `486ccc7` | `ai-effect-wp3` @ `bf1d841` (or later, recorded in `env.json`) |
| Why this baseline | the exact commit pinned by the German and Portugal Task 3.3 suites | |
| Compose project | `orch-old` | `orchestrator` |

**Design.** A controlled before/after comparison with one independent
variable, the orchestrator version. The following are held constant and are
recorded in `results/<run>/env.json` by `env_snapshot.sh`:

- host (CPU, RAM, kernel), Docker/Compose, Python and pwsh versions;
- use-case code and images: `file_based_energy_pipeline` and `germany-node` are
  byte-identical in both trees, and Portugal uses the OLD overlay for both
  versions (NEW's copy differs, see `t33/portugal-new.psd1`);
- workload inputs (SHA-256 of blueprints, dockerinfo and data);
- the orchestrator configuration: each version runs with its own shipped
  `docker-compose.yml` defaults. The one exception is `LEASE_SECONDS=20` for
  NEW in the worker-death scenario only (default 300 s), so that a trial ends
  in minutes. OLD has no lease mechanism at all.

**Trial independence.** Before every trial, Redis is flushed (`FLUSHALL`) and
all use-case services are restarted, unpaused and probed. Germany trials also
recreate `villas-node`, which crashes on its second restart (use-case defect
F2 in `RUN_ANALYSIS_GERMANY_NODE.md`).

**Order effects.** `campaign.sh` runs blocks in alternating order (OLD, NEW,
then NEW, OLD, …) so that drift over time does not line up with one version.

## 2. Research questions and hypotheses

Every H1 is tested against H0 "no difference between OLD and NEW".

| RQ | Question | H1 | Evidence layer |
|---|---|---|---|
| RQ1 Reliability | Do workflows survive transient service faults? | NEW completion rate under fault > OLD | 3 |
| RQ2 Recoverability | Is recovery cheaper? | NEW needs fewer re-executed tasks and operator actions, and less time from restoration to completion | 3 |
| RQ3 Observability | Are faults detected and reported? | NEW detects an outage via API and alert within a bounded time; OLD cannot | 3 |
| RQ4 Scheduling correctness | Are the concurrency and liveness defects fixed? | Fan-in race, idle-worker hang, orphaned tasks and never-ending tasks occur in OLD and not in NEW | 2, 3 |
| RQ5 Conformance | Do the independent T3.3 findings improve? | T3.3 verdicts move DEFECT/BLOCKED → PASS | 1 |
| RQ6 Cost | Do the features slow the fault-free path? | Latency of NEW is within ±5 % of OLD (equivalence) | 3 |
| RQ7 Non-regression | Does NEW still satisfy OLD's contract? | OLD's tests pass on NEW, and every T3.3 PASS on OLD stays PASS | 1, 2 |

## 3. Evidence layers

### Layer 1: independent conformance (Task 3.3 suites)

The Task 3.3 suites were written by others, before this work, for the
official node assessment. That makes them the strongest defence against the
author choosing tests that favour their own code.

- `run_t33.sh <node> <old|new> <out>` runs each node's non-interactive scripts
  in the order given in `START-HERE.md` and records the final `RESULT:` verdict
  into `t33.csv`.
- Following the suite's own rule (DEVELOPER-NOTES, "Frozen baselines"), NEW is
  a *new package version*: the same scripts, with `ExpectedCommit` set to NEW's
  commit. The published OLD evidence is not overwritten.
- The test scripts are not modified. Local portability patches (PowerShell 7
  on Linux) apply equally to both versions; `run_t33.sh` saves them as
  `t33-harness.diff` and they must be listed in the chapter.
- **German node.** The VILLASnode image that blocked the official closure run
  is now present locally. Its digest (in `env.json`) must be reported, since
  the suite forbids silently substituting images.
- **Portugal node.** This needs the external TEF checkout (`TefServicesRoot`,
  commit `7ba677f…`), which is not on this machine. Until it is, Portugal is
  reported as *not run*.
- **Dutch node.** This pins the `paulban` fork @ `22fbb6e`, whose orchestrator
  is **not** `486ccc7`: it has its own changes to `app.py`, `models.py`,
  `control_client.py` and `worker_daemon.py`. `run_t33.sh prepare-dutch`
  builds two trees:
  - `dutch-old` is the fork as pinned;
  - `dutch-new` is the fork with `orchestrator/` replaced by NEW.

  Dutch results are therefore a *fork-baseline* comparison. The fork's own
  orchestrator changes, lost in `dutch-new`, are listed in
  `dutch-orchestrator-swap.stat` and must be discussed as a confound.

- **Clean tree.** The suites refuse a dirty repository. Commit `evaluation/`
  before running NEW. The resulting commit differs from the measured
  orchestrator commit only by this folder, so record both in the chapter.

**Reported as** a per-script verdict table (OLD | NEW) and a transition matrix
(PASS→PASS, DEFECT→PASS, BLOCKED→PASS, PASS→FAIL…). Any PASS→non-PASS is a
regression and is reported individually.

### Layer 2: own suites, crossed with both versions (`run_own_suites.sh`)

| Cell | Tests | Code | Purpose |
|---|---|---|---|
| old-on-old | OLD | OLD | baseline size, pass rate, line/branch coverage |
| new-on-new | NEW | NEW | the same for NEW |
| old-on-new | OLD | NEW | RQ7: NEW keeps OLD's tested contract. Any failure is listed with a justification (intended API change) or counted as a regression |
| diff-on-old | differential | OLD | expected to **fail**: the defect exists |
| diff-on-new | differential | NEW | expected to **pass**: the defect is fixed |

**Precondition:** ports 18081–18084 must be free, so stop the use-case stacks
first. OLD's integration tests bind 18081/18082 at fixed ports (NEW picks free
ports), and would otherwise talk to the real pipeline and fail for reasons
unrelated to the code. The script refuses to start otherwise.

The differential tests (`differential/test_differential.py`) use only the API
both versions share. A test that fails on OLD and passes on NEW shows that the
test detects a real defect, and not just code that OLD does not have. The fan-in
race is repeated `DIFF_TRIALS` times, with 0–2 ms of random latency injected
before each Redis command. This jitter injection widens the race window the
way network latency does in production, so the result is an **empirical
failure rate** and not an anecdote.

Test counts and coverage (layer 2) describe the verification effort. They are
**not** evidence that the system itself improved, and the chapter must not
present them as such.

### Layer 3: fault-injection experiments (`runner.py`, `campaign.sh`)

Black-box trials against the live stacks, using the same code for both
versions: public HTTP API, Docker Compose, and the use-case services' own logs.

**Operator model** (fixed in advance; the only version-specific behaviour):

- *Both:* after a fault, the operator restarts the failed service. This counts as 1 action.
- *OLD:* no health API and no resume. If the workflow failed, the operator
  resubmits the whole workflow, at most `--max-resubmits` (2) times. Each
  resubmission is 1 action.
- *NEW:* one `POST /orchestrator/health/services/<svc>/recover` (1 action). If
  the workflow is `failed`, also `POST /workflows/<id>/resume` (1 action).

| ID | Scenario (`--scenario`) | Fault injection | Oracle | Main metrics | n per version |
|---|---|---|---|---|---|
| V-A1 | `outage` | stop target service, submit, observe 30 s, restart, operator acts | job completed | completion, recovery time, re-executions, operator actions, detection latency, alerts | 10 |
| V-A3 | `short-outage` | stop, submit, restart after +10 s, **no operator** | job completed | completion, recovery time | 10 |
| V-A2 | `outage-during-task` (germany) | kill the service of the running long task | job completed | as V-A1 | 10 |
| V-A7 | `request-error` | blueprint asks the target for an operation it lacks | workflow fails cleanly; NEW breaker stays closed | status, time to terminal state, circuit state | 10 |
| V-B3 | `concurrent-outage` | stop target, submit K=10, restore once | all K complete | completed/K, operator actions, SERVICE_DOWN alert count | 10 |
| V-B12 | `worker-death` | pause target, kill all workers while holding the task, start new workers, unpause | job completed with no human action | completion, time to completion | 10 |
| V-A4 | `burst` | none: K=10 at once, then one more workflow | all complete; no worker blocked in BRPOP | throughput, completed, `brpop_clients_after`, late workflow status | 10 |
| V-B14 | `nominal` | none | completes | end-to-end latency, server makespan | 30 (+2 warm-up) |

**Re-executions** are counted on the service side: the `Execute: method=…`
lines in the use-case logs during the trial. This works for both versions,
including OLD's resubmissions, which get new task ids.

**Detection latency** is the time from the fault to `UNHEALTHY` in the health
API, and to the first `SERVICE_DOWN` alert. OLD has neither, so it is
reported as *n/s* (not supported), never as 0 or ∞.

A trial that raises is kept, with its `error` field, and is reported
separately. It is never silently dropped.

## 4. Metrics summary

| Metric | Type | RQ |
|---|---|---|
| Job completion (final workflow `completed`) | binary | 1 |
| Recovery time: service restored → job completed | seconds | 2 |
| Re-executed tasks (service executions − 1, per operation) | count | 2 |
| Operator actions | count | 2 |
| Detection latency; first-alert latency; SERVICE_DOWN count per outage | seconds / count | 3 |
| Fan-in violation rate; empty-queue claim returns; orphan reclaimed; stuck service bounded | rate / binary | 4 |
| T3.3 verdict per script | categorical | 5, 7 |
| End-to-end latency, makespan, throughput, CPU/RAM (`docker stats`, burst only) | continuous | 6 |
| Pass rate, line and branch coverage per cell | ratio | 7 |

## 5. Analysis (`analyze.py`)

- **Binary outcomes** (completion): Fisher's exact test, two-sided.
- **Continuous and count outcomes:** Mann-Whitney U, two-sided. Effect size is
  Cliff's δ (|δ| < 0.147 negligible, < 0.33 small, < 0.474 medium, else large),
  with a 95 % bootstrap CI (10 000 resamples) of the difference in medians.
- **Overhead (RQ6):** equivalence rather than difference. TOST on log latency
  with a ±5 % margin: NEW is "equivalent" if both one-sided p < 0.05.
- α = 0.05. With several scenarios, the chapter reports effect sizes and CIs
  alongside p-values and does not rest any claim on a single borderline p.
- Descriptives: median and IQR. Figures are strip plots with a median tick
  (small n: show every trial) and completion bars.

Outputs go to `results/<run>/thesis/`: `table_trials.tex`, `table_t33.tex`,
`table_suites.tex`, `fig_completion.pdf`, `fig_recovery.pdf`,
`fig_latency.pdf` and `summary.json`.

## 6. Procedure

```bash
cd ai-effect-wp3
python3 -m venv evaluation/.venv && evaluation/.venv/bin/pip install -r evaluation/requirements.txt
RUN=evaluation/results/$(date +%F)

evaluation/env_snapshot.sh $RUN                       # 1. environment
evaluation/run_own_suites.sh $RUN                     # 2. layer 2 (~5 min)
export ORCHESTRATOR_API_KEY=<key> SERVICE_API_KEY=<key>  # T3.3 auth tests need both, set
                                                      # before the stack starts (stack.sh passes them on)
evaluation/run_t33.sh german old $RUN                 # 3. layer 1, per node and version
evaluation/run_t33.sh german new $RUN
evaluation/run_t33.sh prepare-dutch                   #    once; then dutch old/new
evaluation/campaign.sh $RUN 2 5                       # 4. layer 3 (~3-4 h)
evaluation/.venv/bin/python evaluation/analyze.py $RUN   # 5. tables and figures
```

Single scenarios can be run on their own, for example
`evaluation/stack.sh old && evaluation/.venv/bin/python evaluation/runner.py --version old --scenario outage --reps 1 --out $RUN`.

## 7. Threats to validity (to be discussed in the chapter)

- **Internal.**
  - The author wrote NEW and its tests. This is mitigated by layer 1, which is
    independent, and by differential tests that must *fail* on OLD.
  - Use-case defects (F2 VILLAS crash, Portugal healthchecks) can hurt OLD
    through the resubmission path. Germany results are reported separately from
    the file-based ones.
  - The operator model is a simplification, but it is fixed in advance and
    symmetric: service restart is counted on both sides.
- **Construct.** Operator actions and recovery time are proxies for
  operational cost. Coverage and test counts measure testing effort, not
  quality.
- **External.**
  - One machine, small workloads, three worker replicas. Following the T3.3
    README, none of this is a scalability or SLA claim.
  - The Dutch comparison uses a swapped orchestrator on a fork.
  - Portugal depends on an external checkout.
- **Conclusion.**
  - n = 10 per cell detects only large effects. This is why effect sizes and
    CIs are reported.
  - A deterministic outcome (for example 0/10 vs 10/10) is significant at
    Fisher p < 0.001, and that is the expected shape for most fault scenarios.
- **Configuration.** NEW's lease is shortened in one scenario (reported).
  Jitter injection in the fan-in test is an artificial stressor: it shows the
  race is *possible*, not how often it happens in production.
