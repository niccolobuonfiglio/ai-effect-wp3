# Evaluation harness: summary of the work (2026-09-25)

Nothing under `orchestrator/` was changed in this work. Everything described
here is the new `evaluation/` folder: a way to measure, with evidence, how the
modified orchestrator (NEW, `bf1d841`) compares with the original (OLD,
`486ccc7`, the commit the Task 3.3 suites pin). The method itself is in
[PROTOCOL.md](PROTOCOL.md).

## 1. What was built

| File | What it does | Why it matters for the thesis |
|---|---|---|
| `PROTOCOL.md` | Sets out the method before any measuring: the fixed setup, 7 research questions with hypotheses, the scenarios, metrics, statistical tests and threats to validity. | This is the chapter's method section. Fixing the rules in advance means the results can't be tuned afterwards to favour NEW. |
| `config.py` | Holds everything that must be identical for both versions: paths, stack names, use-case services, which service is taken down. | Guarantees the only thing that differs between OLD and NEW trials is the orchestrator code. |
| `stack.sh` | Switches the live orchestrator between OLD and NEW and checks which one is actually running. | Stops you measuring the wrong version by accident. |
| `env_snapshot.sh` | Records both commits, whether the working trees are clean, image hashes, tool versions, hardware and input-file hashes. | Makes the experimental setup documented and reproducible. |
| `runner.py` | Runs 8 scenarios the same way on both versions. It uses only the public API, Docker and the services' own logs. One record per trial. | The quantitative core. Because it measures from the outside, the same code measures both versions fairly. |
| `campaign.sh` | Runs every scenario on both versions, repeated, alternating which version goes first in each block. | Enough repetitions for statistics, and slow drift over time can't favour one version. |
| `differential/test_differential.py` | 4 behavioural tests that only use code both versions share. They should fail on OLD and pass on NEW. | Shows each fix addresses a real defect in OLD, not just that NEW has more code. |
| `run_own_suites.sh` | Crosses the test suites with the code: OLD tests on OLD, NEW on NEW, OLD tests on NEW, and the differential tests on each. | Measures testing effort and non-regression, i.e. whether NEW still does everything OLD's tests check. |
| `run_t33.sh` + `t33/*.psd1` | Runs the independent Task 3.3 test suites on both versions and records each script's verdict. It never edits the test scripts. | Evidence written by other people, which answers "the author wrote their own tests". |
| `analyze.py` | Turns the raw results into statistical tests, LaTeX tables and PDF figures. | Every number in the chapter traces back to a file. |

## 2. Fixes made to the harness while testing it

Each of these would have made the comparison unfair or wrong:

1. **The fan-in race couldn't be observed.** Without extra stress the race
   window is too narrow to hit, so the test passed on OLD too. Small random
   delays before each Redis call (jitter injection) now turn it into a
   measurable failure rate on OLD.
2. **OLD's tests failed for the wrong reason.** They bind fixed ports
   (18081/18082) that the running pipeline already held, so they talked to the
   wrong service. The script now refuses to start while those ports are busy.
   The result went from 10 misleading failures to 1 genuine OLD bug.
3. **OLD could have been tested with NEW's code.** The Task 3.3 suites start
   the orchestrator themselves, and both folders are called `orchestrator`, so
   OLD would have reused NEW's containers and images. Each tree now gets its
   own project name via an `.env` file, hidden from git so the suites'
   clean-repository check still passes, and removed afterwards.
4. **Re-executions were overcounted** when several workflows ran at once. Only
   executions beyond one per logical job are counted now.
5. **The worker-death scenario reset NEW's lease to 300 s.** Bringing workers
   back with `up` re-applied the default, so NEW repetitions after the first
   would have timed out and been counted as NEW failures. The runner now
   restarts the same containers with `start`.
6. **Measuring resource use disturbed the timings,** because `docker stats`
   itself uses CPU. It now runs in a separate run, never during latency or
   throughput measurement.
7. **Use cases now match between versions.** Portugal's files differ between
   the two trees, so both versions use OLD's copy.
8. **API keys leaked into the measurements.** A key still exported in the
   shell made both versions answer `401` to every test expecting an open API
   (about 30 false failures per cell). `run_own_suites.sh` and `campaign.sh`
   now clear the keys themselves.
9. **The T3.3 scripts couldn't find OLD's containers.** They look them up by
   the name `orchestrator-*`, so OLD's renamed project made scripts 01 and 50
   stop without a result. Both versions now run as project `orchestrator`,
   with the images rebuilt from the tree under test and NEW's restored
   afterwards.
10. **T3.3 script 50 passed without testing anything.** It inspects only the
    service keys left in Redis, and a fresh stack has none. One workflow with
    a service key now runs first, on both versions. OLD then shows the
    published DEFECT CONFIRMED.
11. **Preconditions are written into the protocol:** the API keys for the
   authentication tests, committing before Task 3.3 runs on NEW, and freeing
   the ports.

## 3. Results so far

### Deterministic results (they will repeat in every trial)

- **Non-regression:** all 351 of OLD's tests pass on NEW, so NEW keeps
  everything OLD's tests check.
- **A real OLD defect:** OLD fails 1 of its own tests (`execute()` has no
  `parameters` argument). NEW passes all 841 of its tests.
- **Differential tests:** all 4 fail on OLD and pass on NEW.
  - **Fan-in race:** OLD lost one branch's input in 111 of 200 trials and
    queued the join twice in 78. NEW had neither.
  - **Idle worker hang:** an OLD worker blocks forever on an empty queue; NEW
    returns at once.
  - **Worker death:** a task whose worker dies stays `running` forever on OLD;
    NEW picks it up again.
  - **Stuck service:** a service that never finishes holds an OLD worker
    forever; NEW gives up after a time limit.
- **Behaviour in the scenarios:**

  | Scenario | OLD | NEW |
  |---|---|---|
  | Short outage, no operator | workflow fails | completes by itself |
  | Worker killed mid-task | task stuck forever | recovers in about 19 s |
  | Service down before submission | fails; the resubmit re-runs 2 finished tasks | task waits, outage detected in about 6 s, nothing re-run |
  | Outage visibility | no health API, no alert | health status plus a SERVICE_DOWN alert |

- **Independent suite so far:** the Task 3.3 German suite runs on both
  versions. With the fixed driver, scripts 00, 02, 40 and 99 give PASS, 11
  gives DEFECT CONFIRMED (a packaged-input defect in the use case, not the
  orchestrator) and 60 gives READY on both. The final verdicts come from the
  rerun with the fixed driver.

### Finding: the service-key defect is not fixed in NEW

T3.3 reports that workflow service keys persist in Redis without expiry. In
NEW, a key submitted with a workflow is still in Redis after the workflow
completes (`TTL -1`):

- With the shipped defaults (`ORCHESTRATOR_SECRET` unset) it is stored in
  plain text, exactly as in OLD.
- With `ORCHESTRATOR_SECRET` set it is encrypted at rest, but it still never
  expires, so T3.3 script 50 still reports DEFECT CONFIRMED.

NEW improves confidentiality only when it is configured to. It does not change
the key's lifecycle.

### Limits to keep in mind

- All the timings (latency, throughput, recovery seconds) are single smoke
  runs, and they flipped direction between runs. The overhead question stays
  open until the campaign has run.
- Operator actions tie in the single-outage scenario. NEW saves actions only
  with several workflows at once (2 vs 4 with 3 workflows).
- NEW can run a task twice after a worker dies: the paused service still
  finishes the old request, and then the reclaimed task runs again. This is
  at-least-once behaviour and should be reported as a trade-off.

## 4. Still to do

1. Commit `evaluation/`.
2. Run the Task 3.3 suites on both versions (`run_t33.sh german old|new <run>`,
   with `ORCHESTRATOR_API_KEY` and `SERVICE_API_KEY` exported).
3. Run `campaign.sh <run> 2 5` (about 3–4 hours).

After that, the tables and figures in `results/<run>/thesis/` can go straight
into the chapter.
