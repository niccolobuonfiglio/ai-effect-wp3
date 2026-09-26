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
11. **The T3.3 script 50 preparation hung on VILLASnode.** Script 01 restarts
    VILLASnode, which crashes on its second restart (use-case defect F2). The
    preparation now recreates it first and prints its progress.
12. **The campaign wrote its trials to the wrong folder.** It changed into
    `evaluation/` before starting the runner but passed a relative results
    path, which created `evaluation/evaluation/results/...`. The path is now
    made absolute.
13. **Unexplained failures could not be diagnosed.** Trial records for burst
    and concurrent-outage now keep the status and task errors of every
    workflow that did not complete.
14. **Preconditions are written into the protocol:** the API keys for the
    authentication tests, committing before Task 3.3 runs on NEW, and freeing
    the ports.

## 3. Results

All numbers below come from the final run in
`results/final-2026-09-25/` (NEW commit `1c42faa`, OLD `486ccc7`). How that
folder came about is described in its `NOTES.md`. The `results/` folder is not
under version control.

### Layer 2: own suites and differential tests

- **Non-regression:** all 351 of OLD's tests pass on NEW, so NEW keeps
  everything OLD's tests check.
- **A real OLD defect:** OLD fails 1 of its own tests (`execute()` has no
  `parameters` argument). NEW passes all 841 of its tests.
- **Differential tests:** all 4 fail on OLD and pass on NEW.
  - **Fan-in race (300 trials, 2 ms jitter):** OLD lost one branch's input in
    165 trials and queued the join more or less than once in 149. NEW had
    neither.
  - **Idle worker hang:** an OLD worker blocks forever on an empty queue; NEW
    returns at once.
  - **Worker death:** a task whose worker dies stays `running` forever on OLD;
    NEW picks it up again.
  - **Stuck service:** a service that never finishes holds an OLD worker
    forever; NEW gives up after a time limit.

### Layer 1: Task 3.3 German suite

Every script gives the same verdict on both versions: PASS for 00, 02, 40 and
99; RECORDED for 01; DEFECT CONFIRMED for 11 (a data problem in the use case)
and 50 (see below); READY for 60.

- This is strong evidence of **non-regression**: an independent suite finds
  nothing that NEW broke.
- It is **not evidence of improvement**: these scripts test authentication,
  input validation and service keys, not outages, recovery or scheduling.
- Script 60 gives READY because the VILLASnode image, which blocked the
  official closure run, is present on this machine (digest in `env.json`).
  That is not a result of NEW.

### Layer 3: fault-injection campaign (n = 10 per version, 30 for nominal)

| Scenario | Metric | OLD | NEW | Test |
|---|---|---|---|---|
| short-outage, no operator | jobs completed | 0/10 | **10/10** | Fisher p < 0.001 |
| worker-death | jobs completed | 0/10 | **10/10** (about 19 s) | Fisher p < 0.001 |
| outage | re-executed tasks (median) | 2 | **0** | MWU p < 0.001, δ = −1 |
| outage | recovery time (median) | 1.9 s | 1.2 s | p = 0.24, not significant |
| outage | detection / first alert | not supported | about 7 s | exists only in NEW |
| concurrent-outage, 10 workflows | operator actions | 11 | **2** | p < 0.001, δ = −1 |
| concurrent-outage | re-executed tasks | 20 | **0** | p < 0.001, δ = −1 |
| concurrent-outage | recovery time | 9.6 s | **1.8 s** | p < 0.001, δ = −1 |
| burst | idle workers stuck in BRPOP (median) | 1 | 0 | the OLD hang, seen live |
| nominal | end-to-end latency (median) | 0.70 s | 0.70 s | TOST ±5 %: ratio 1.007, p = 0.0498 |
| request-error | jobs completed | 0/10 | 0/10 | same behaviour |

How to read it:

- **Overhead:** NEW's fault-free latency is equivalent to OLD's within ±5 %,
  but p = 0.0498 only just passes. Report it as "no measurable overhead, at
  the margin of the test", not as proven equivalence.
- **Recovery time for a single outage** does not differ significantly. The
  gain there is that no finished work is repeated and the workflow keeps its
  identity, not speed.
- **Operator actions** tie at 2 for a single outage. NEW saves actions only
  when several workflows are hit (2 vs 11 with 10 workflows).
- **At-least-once:** NEW re-executed exactly 1 task in every worker-death
  trial. The paused service still finishes the orphaned request, and then the
  reclaimed task runs again. Report this as a trade-off.

### Not usable as orchestrator results: burst and concurrent-outage completion

Burst completion (6/10 on both versions) and concurrent-outage completion
(OLD 10/10, NEW 8/10, Fisher p = 0.47) are distorted by a defect in the test
pipeline, not in either orchestrator.

The `file_based_energy_pipeline` services read and write fixed file names
shared by every workflow (for example `data/analyzed_energy.csv` and
`data/energy_report.csv`). When workflows run at the same time, one can read a
file another is still writing, and fails with `No columns to parse from file`.
This was reproduced with the runner on OLD: 5 of 20 bursts had a failed
workflow, all with that error (`diagnostics/burst-race-old-20reps.jsonl` in the
results folder).

The asymmetry in concurrent-outage follows from the operator model. NEW's
recovery re-queues all 10 blocked workflows at once, so they overlap and can
collide. OLD's operator resubmits them one at a time, so they never overlap.
Burst throughput (OLD 561, NEW 707 workflows/min, p = 0.003) is affected by
the same failures. OLD's idle worker stuck in BRPOP is a genuine factor in
that gap too, but the two cannot be separated in this data.

### Finding (superseded 2026-09-26): the service-key defect

On 2026-09-25, T3.3 German script 50 reported DEFECT CONFIRMED on both
versions: a key submitted with a workflow stayed in Redis after it completed.
NEW now deletes the key when the workflow completes or is deleted (see
`orchestrator/docs/PROVENANCE_AND_CREDENTIALS.md`). Rerun on 2026-09-26
(`results/t33-2026-09-26`), script 50 gives **DEFECT CONFIRMED on OLD and
PASS on NEW**, the only T3.3 verdict that improves. Caveats for the chapter:
a failed workflow keeps its key (needed for resume), and without
`ORCHESTRATOR_SECRET` it is stored in plain text, as in OLD.

The same day the Dutch suite was run on both versions for the first time,
after four harness fixes (PROTOCOL.md, Layer 1). All verdicts are identical
(non-regression). HAI scripts 29–36 are blocked by the environment on both,
because the supplied simulator can no longer be built on Debian 11, and they
never use the orchestrator.

## 4. Amendment 1 (2026-09-26) and what is left

The protocol was amended before new measurements (PROTOCOL.md §8):

- **RQ8, data-exchange contracts**, with 5 more differential tests (checked:
  all 9 fail on OLD and pass on NEW, each for the behaviour it names) and two
  scenarios, `credential-misconfig` and `corrupt-inline`. In a smoke run OLD
  accepted both, completing the workflow on the corrupted input and failing
  the misconfigured one 0.7 s later with a key left in Redis; NEW refused both
  at submission in under 40 ms, leaving nothing behind.
- **Overhead attribution**: a nominal arm with NEW's submission checks off
  (`--variant checks-off`), so their cost is measured rather than hidden in
  RQ6.
- **The pipeline race is fixed** (decision (a) above): the file-based services
  write under `data/<workflow_id>/`. Check on OLD: 10/10 bursts completed with
  no file-race errors (previously 5 of 20 failed).

Left to do:

1. Commit, then rerun the whole protocol on the final commit: `env_snapshot.sh`,
   `run_own_suites.sh`, `campaign.sh $RUN 2 5` (about 4–5 h), `analyze.py`.
   T3.3 can be cited from `results/t33-2026-09-26` if the orchestrator code
   does not change again.
2. Replace the results in section 3 with the rerun's, including the new tables
   `table_overhead.tex` and `table_differential.tex`.
3. Optional: Portugal (needs the partner's TEF source).
