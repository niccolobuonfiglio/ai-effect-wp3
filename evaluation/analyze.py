#!/usr/bin/env python3

import csv
import json
import math
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
from scipy import stats

RNG = np.random.default_rng(20260925)
MARGIN = 0.05  # equivalence margin for overhead
SWEEP_SCENARIOS = ("scale", "inline-size", "data-size")
COST_SCENARIOS = ("service-delay", "hung-submit", "running-cost")  # Amendment 5, PROTOCOL.md 12
DIAGNOSTIC_SCENARIOS = ("pickup",)  # Amendment 6
VERSIONS = ("old", "new")
COLOR = {"old": "#eb6834", "new": "#2a78d6"}
HATCH = {"old": "////", "new": ""}
INK, MUTED, GRID = "#1f1f1e", "#6b6a64", "#e4e3dd"


# statistics
def cliffs_delta(a, b) -> float:
    """P(a > b) - P(a < b); |d| < .147 negligible, < .33 small, < .474 medium."""
    a, b = np.asarray(a), np.asarray(b)
    if not len(a) or not len(b):
        return float("nan")
    diff = a[:, None] - b[None, :]
    return float((np.sum(diff > 0) - np.sum(diff < 0)) / diff.size)


def magnitude(d: float) -> str:
    if math.isnan(d):
        return ""
    d = abs(d)
    return "negligible" if d < 0.147 else "small" if d < 0.33 else "medium" if d < 0.474 else "large"


def bootstrap_median_diff(a, b, n=10_000):
    """95% percentile CI of median(b) - median(a)."""
    a, b = np.asarray(a), np.asarray(b)
    if len(a) < 2 or len(b) < 2:
        return (float("nan"), float("nan"))
    ia = RNG.integers(0, len(a), (n, len(a)))
    ib = RNG.integers(0, len(b), (n, len(b)))
    d = np.median(b[ib], axis=1) - np.median(a[ia], axis=1)
    return tuple(float(x) for x in np.percentile(d, [2.5, 97.5]))


def compare_continuous(old, new) -> dict:
    out = {
        "n_old": len(old), "n_new": len(new),
        "median_old": _med(old), "median_new": _med(new),
        "iqr_old": _iqr(old), "iqr_new": _iqr(new),
    }
    if len(old) and len(new):
        out["mwu_p"] = float(stats.mannwhitneyu(old, new, alternative="two-sided").pvalue)
        out["cliffs_delta"] = cliffs_delta(new, old)
        out["effect"] = magnitude(out["cliffs_delta"])
        out["ci_median_diff"] = bootstrap_median_diff(old, new)
    return out


def compare_binary(k_old, n_old, k_new, n_new) -> dict:
    p = float("nan")
    if n_old and n_new:
        p = float(stats.fisher_exact([[k_old, n_old - k_old], [k_new, n_new - k_new]]).pvalue)
    return {"k_old": k_old, "n_old": n_old, "k_new": k_new, "n_new": n_new, "fisher_p": p}


def tost_log(old, new, margin=MARGIN) -> dict:
    """Equivalence of mean log latency within +/- margin (Welch, two one-sided tests)."""
    a, b = np.log(np.asarray(old)), np.log(np.asarray(new))
    if len(a) < 3 or len(b) < 3:
        return {}
    lo, hi = math.log(1 - margin), math.log(1 + margin)
    d = b.mean() - a.mean()
    se = math.sqrt(a.var(ddof=1) / len(a) + b.var(ddof=1) / len(b))
    df = se**4 / (
        (a.var(ddof=1) / len(a)) ** 2 / (len(a) - 1) + (b.var(ddof=1) / len(b)) ** 2 / (len(b) - 1)
    )
    p_lo = 1 - stats.t.cdf((d - lo) / se, df)
    p_hi = stats.t.cdf((d - hi) / se, df)
    return {
        "ratio_new_over_old": math.exp(d),
        "margin": margin,
        "p_tost": float(max(p_lo, p_hi)),
        "equivalent": bool(max(p_lo, p_hi) < 0.05),
    }


def _med(x):
    return float(np.median(x)) if len(x) else None


def _iqr(x):
    return tuple(float(v) for v in np.percentile(x, [25, 75])) if len(x) else None


# layer 3: trials
def load_trials(run: Path) -> list[dict]:
    path = run / "trials.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def values(trials, key) -> list[float]:
    return [t[key] for t in trials if t.get(key) is not None]


def analyse_trials(trials: list[dict]) -> dict:
    groups: dict[tuple, dict[str, list]] = defaultdict(lambda: {"old": [], "new": []})
    for t in trials:
        # Only the shipped configuration of each version is compared here; the
        # checks-off arm is analysed separately (analyse_overhead), and so are
        # the sweeps, whose points must not be pooled (analyse_sweeps).
        if (not t.get("warmup") and t.get("variant", "default") == "default"
                and t["scenario"] not in SWEEP_SCENARIOS + COST_SCENARIOS + DIAGNOSTIC_SCENARIOS):
            groups[(t["scenario"], t["use_case"])][t["version"]].append(t)

    result = {}
    for (scenario, uc), by in sorted(groups.items()):
        old, new = by["old"], by["new"]
        valid = {v: [t for t in by[v] if "error" not in t] for v in VERSIONS}
        r = {
            "errors": {v: sum("error" in t for t in by[v]) for v in VERSIONS},
            "completion": compare_binary(
                sum(bool(t.get("job_completed")) for t in valid["old"]), len(valid["old"]),
                sum(bool(t.get("job_completed")) for t in valid["new"]), len(valid["new"]),
            ),
        }
        if any("refused" in t for t in valid["old"] + valid["new"]):  
            r["refused"] = compare_binary(
                sum(bool(t.get("refused")) for t in valid["old"]), len(valid["old"]),
                sum(bool(t.get("refused")) for t in valid["new"]), len(valid["new"]),
            )
        for key in ("recovery_seconds", "reexecutions", "operator_actions", "e2e_seconds",
                    "makespan_seconds", "throughput_wf_per_min", "brpop_clients_after",
                    "error_visible_seconds", "tasks_started", "residue_services_keys",
                    "residue_endpoints"):
            o, n = values(valid["old"], key), values(valid["new"], key)
            if o or n:
                r[key] = compare_continuous(o, n)
        for key in ("detection_seconds", "first_alert_seconds"):
            n = values(valid["new"], key)
            if n:
                r[key] = {"new": compare_continuous([], n), "old": "n/s"}
        down = [t.get("alerts", {}).get("SERVICE_DOWN", 0) for t in valid["new"] if "alerts" in t]
        if down:
            r["service_down_alerts_new"] = {"median": _med(down), "max": max(down)}
        if any("workflow_alerts" in t for t in valid["new"]):
            raised = Counter()
            for t in valid["new"]:
                raised.update(t.get("workflow_alerts", {}))
            r["workflow_alerts_new"] = dict(raised)
        if scenario == "nominal":
            r["tost_e2e"] = tost_log(values(valid["old"], "e2e_seconds"), values(valid["new"], "e2e_seconds"))
        result[f"{scenario}/{uc}"] = r
    return result


def analyse_overhead(trials: list[dict]) -> dict:
    """RQ6 with attribution (PROTOCOL.md 8.4): what do NEW's submission checks cost?

    File-based only: the checks-off arm exists only there, and latencies of
    different use cases are never pooled (PROTOCOL.md 9.4)."""
    def e2e(version, variant):
        return [t["e2e_seconds"] for t in trials
                if t["scenario"] == "nominal" and t.get("use_case", "file_based") == "file_based"
                and not t.get("warmup") and "error" not in t
                and t["version"] == version and t.get("variant", "default") == variant
                and t.get("e2e_seconds") is not None]

    old, new, off = e2e("old", "default"), e2e("new", "default"), e2e("new", "checks-off")
    if not off:
        return {}
    cost = compare_continuous(off, new) 
    return {
        "a_old_vs_new_default": tost_log(old, new),
        "b_old_vs_new_checks_off": tost_log(old, off),
        "c_checks_cost": {
            "n_checks_off": len(off), "n_default": len(new),
            "median_checks_off": _med(off), "median_default": _med(new),
            "median_diff_seconds": (_med(new) - _med(off)) if new and off else None,
            "ci_median_diff": cost.get("ci_median_diff"),
            "mwu_p": cost.get("mwu_p"), "cliffs_delta": cost.get("cliffs_delta"),
        },
    }


def nominal_latencies(trials: list[dict]) -> dict[tuple, list[float]]:
    """(use case, version, variant) -> end-to-end latencies of the nominal trials."""
    out: dict[tuple, list[float]] = defaultdict(list)
    for t in trials:
        if (t["scenario"] == "nominal" and not t.get("warmup") and "error" not in t
                and t.get("e2e_seconds") is not None):
            out[(t.get("use_case", "file_based"), t["version"], t.get("variant", "default"))].append(t["e2e_seconds"])
    return out


def describe_spread(x) -> dict:
    x = np.asarray(x)
    p = np.percentile(x, [0, 5, 25, 50, 75, 95, 100])
    sd = float(x.std(ddof=1)) if len(x) > 1 else float("nan")
    return {
        "n": len(x), "min": float(p[0]), "p5": float(p[1]), "q1": float(p[2]), "median": float(p[3]),
        "q3": float(p[4]), "p95": float(p[5]), "max": float(p[6]), "iqr": float(p[4] - p[2]),
        "sd": sd, "cv": sd / float(x.mean()),
    }


def analyse_dispersion(trials: list[dict]) -> dict:
    """Exploratory, not pre-registered: is nominal latency equally spread on OLD and NEW?
    """
    lat = nominal_latencies(trials)
    result = {}
    for uc in sorted({k[0] for k in lat}):
        order = lambda k: (VERSIONS.index(k[1]), k[2] != "default", k[2])  
        cells = {f"{v}/{var}": describe_spread(lat[(u, v, var)])
                 for (u, v, var) in sorted(lat, key=order) if u == uc and len(lat[(u, v, var)]) > 1}
        old, new = lat.get((uc, "old", "default"), []), lat.get((uc, "new", "default"), [])
        r = {"cells": cells}
        if len(old) > 1 and len(new) > 1:
            r["brown_forsythe_p"] = float(stats.levene(old, new, center="median").pvalue)
            r["fligner_p"] = float(stats.fligner(old, new).pvalue)
        result[uc] = r
    return result


def analyse_pickup(trials: list[dict]) -> dict:
    """Diagnostic, not pre-registered (Amendment 6): where nominal latency goes.

    Cells are (use case, start jitter, version). Within each use case and
    jitter, OLD and NEW are compared on pickup and end-to-end latency, and on
    the spread of the latter, as in analyse_dispersion.
    """
    cells: dict[tuple, list[dict]] = defaultdict(list)
    for t in trials:
        if (t["scenario"] == "pickup" and not t.get("warmup") and "error" not in t
                and t.get("pickup_seconds") is not None):
            cells[(t["use_case"], float(t.get("start_jitter") or 0), t["version"])].append(t)

    result: dict = {"cells": {}, "comparisons": {}}
    for (uc, jitter, version), ts in sorted(cells.items(), key=lambda kv: (kv[0][0], kv[0][1], VERSIONS.index(kv[0][2]))):
        gaps = values(ts, "since_previous_submit_seconds")
        after = [t["pickup_seconds"] for t in ts if (t.get("worker_socket_timeouts_before") or 0) > 0]
        clean = [t["pickup_seconds"] for t in ts if t.get("worker_socket_timeouts_before") == 0]
        result["cells"][f"{uc}/{jitter:g}/{version}"] = {
            "n": len(ts),
            "pickup": describe_spread(values(ts, "pickup_seconds")),
            "work": describe_spread(values(ts, "work_seconds")),
            "e2e": describe_spread(values(ts, "e2e_seconds")),
            "pickup_share_median": _med([t["pickup_seconds"] / t["e2e_seconds"] for t in ts]),
            "submit_gap_median": _med(gaps),
            "socket_timeouts": sum(t.get("worker_socket_timeouts_before") or 0 for t in ts),
            "pickup_median_after_timeout": _med(after),
            "pickup_median_no_timeout": _med(clean),
            "n_after_timeout": len(after),
        }
    for uc, jitter in sorted({(u, j) for u, j, _ in cells}):
        old, new = cells.get((uc, jitter, "old"), []), cells.get((uc, jitter, "new"), [])
        if len(old) > 1 and len(new) > 1:
            e_old, e_new = values(old, "e2e_seconds"), values(new, "e2e_seconds")
            result["comparisons"][f"{uc}/{jitter:g}"] = {
                "pickup": compare_continuous(values(old, "pickup_seconds"), values(new, "pickup_seconds")),
                "e2e": compare_continuous(e_old, e_new),
                "brown_forsythe_p": float(stats.levene(e_old, e_new, center="median").pvalue),
                "fligner_p": float(stats.fligner(e_old, e_new).pvalue),
            }
    return result if result["cells"] else {}


def _history_totals(trials: list[dict]) -> dict | None:
    """NEW's own record over every checked job of these trials (None on OLD)."""
    outputs = [o for t in trials for o in t.get("outputs") or [] if o.get("history")]
    if not outputs:
        return None
    total = Counter()
    for o in outputs:
        h = o["history"]
        total.update({"jobs": 1, "not_clean": not h["clean"], "claims": h["claims"],
                      "reclaims": h["reclaims"], "deferrals": h["deferrals"], "retries": h["retries"]})
    return dict(total)


def _equivalence_counts(trials: list[dict]) -> dict:
    verdicts = Counter(o["verdict"] for t in trials for o in t.get("outputs") or [])
    return {"jobs": sum(verdicts.values()), "equivalent": verdicts["equivalent"]}


def analyse_sweeps(trials: list[dict]) -> dict:
    """Amendment 4 (PROTOCOL.md 11): each point of each sweep on its own.

    A point is (scenario, swept values); OLD and NEW are compared within a
    point with the tests of PROTOCOL.md 5. Errors are counted, never dropped.
    """
    points: dict[tuple, dict[str, list]] = defaultdict(lambda: {"old": [], "new": []})
    for t in trials:
        if t["scenario"] in SWEEP_SCENARIOS and not t.get("warmup"):
            if t["scenario"] == "scale":
                key = (t["scenario"], t.get("workers_requested"), t.get("workflows"))
            elif t["scenario"] == "inline-size":
                key = (t["scenario"], t.get("inline_bytes"))
            else:
                key = (t["scenario"], t.get("records"))
            points[key][t["version"]].append(t)

    result: dict = {"scale": {}, "inline-size": {}, "data-size": {}}
    for key, by in sorted(points.items(), key=lambda kv: tuple(x if x is not None else -1 for x in kv[0])):
        scenario = key[0]
        valid = {v: [t for t in by[v] if "error" not in t] for v in VERSIONS}
        r: dict = {"errors": {v: sum("error" in t for t in by[v]) for v in VERSIONS},
                   "n": {v: len(valid[v]) for v in VERSIONS}}

        if scenario == "scale":
            name = f"W={key[1]}/K={key[2]}"
            for v in VERSIONS:
                ts = valid[v]
                r[v] = {
                    "trials_all_completed": sum(bool(t.get("job_completed")) for t in ts),
                    "workflows_completed": sum(t.get("completed_workflows", 0) for t in ts),
                    "workflows": sum(t.get("workflows", 0) for t in ts),
                    "throughput_median": _med(values(ts, "throughput_wf_per_min")),
                    "throughput_iqr": _iqr(values(ts, "throughput_wf_per_min")),
                    # per trial, then the median over trials
                    "latency_p50_median": _med([float(np.percentile(t["latencies"], 50)) for t in ts if t.get("latencies")]),
                    "latency_p95_median": _med([float(np.percentile(t["latencies"], 95)) for t in ts if t.get("latencies")]),
                    "submit_seconds_median": _med(values(ts, "submit_seconds")),
                    "reexecutions": sum(t.get("reexecutions") or 0 for t in ts),
                    "equivalence": _equivalence_counts(ts),
                    "history": _history_totals(ts),
                }
            o, n = values(valid["old"], "throughput_wf_per_min"), values(valid["new"], "throughput_wf_per_min")
            if o and n:
                r["throughput"] = compare_continuous(o, n)
                r["throughput_ratio_new_over_old"] = _med(n) / _med(o)

        else:
            name = str(key[1])
            for v in VERSIONS:
                ts = valid[v]
                r[v] = {
                    "accepted": sum(bool(t.get("accepted")) for t in ts),
                    "refused": sum(bool(t.get("refused")) for t in ts),
                    "completed": sum(bool(t.get("job_completed")) for t in ts),
                    "e2e_median": _med(values(ts, "e2e_seconds")),
                    "e2e_iqr": _iqr(values(ts, "e2e_seconds")),
                    "makespan_median": _med(values(ts, "makespan_seconds")),
                    "submit_seconds_median": _med(values(ts, "submit_seconds")),
                }
                if scenario == "inline-size":
                    r[v]["equivalence"] = _equivalence_counts(ts)
                else:
                    sizes = [t["output_bytes"].get("raw_energy.csv") for t in ts if t.get("output_bytes")]
                    r[v]["raw_bytes"] = sizes[0] if sizes else None
            o, n = values(valid["old"], "e2e_seconds"), values(valid["new"], "e2e_seconds")
            if o and n:
                r["e2e"] = compare_continuous(o, n)
                r["e2e_ratio_new_over_old"] = _med(n) / _med(o)
            if scenario == "data-size":
                # Every completed job of a point, on both versions, must have
                # produced the same files: one distinct set of hashes.
                hashes = {json.dumps(t["output_hashes"], sort_keys=True)
                          for v in VERSIONS for t in valid[v] if t.get("output_hashes")}
                r["distinct_outputs"] = len(hashes)
                r["outputs_consistent"] = len(hashes) == 1

        result[scenario][name] = r
    return {k: v for k, v in result.items() if v}


def arm(t: dict) -> str:
    variant = t.get("variant", "default")
    return t["version"] if variant == "default" else f"{t['version']}/{variant}"


def _arm_order(name: str) -> tuple:
    return (VERSIONS.index(name.split("/")[0]), "/" in name, name)


def _role(container: str) -> str:
    return container.rsplit("-", 2)[-2]  # <project>-<service>-<n>


def _slope(xs, ys) -> dict | None:
    """Least-squares slope with a 95 % CI."""
    if len(set(xs)) < 2:
        return None
    fit = stats.linregress(xs, ys)
    t = stats.t.ppf(0.975, len(xs) - 2)
    return {"slope": float(fit.slope), "ci95": (float(fit.slope - t * fit.stderr), float(fit.slope + t * fit.stderr)),
            "intercept": float(fit.intercept), "n": len(xs)}


def window_rates(w: dict) -> dict:
    """Per-minute rates of one measured window, total and per role."""
    per_min = 60 / w["seconds"]
    cpu_by_role = Counter()
    for name, sec in w["cpu_seconds"].items():
        cpu_by_role[_role(name)] += sec
    mem_by_role = Counter()
    for name, b in w["memory_bytes"].items():
        if b is not None:
            mem_by_role[_role(name)] += b / 2**20
    return {
        "cpu_s_per_min": sum(cpu_by_role.values()) * per_min,
        "cpu_s_per_min_by_role": {r: v * per_min for r, v in cpu_by_role.items()},
        "redis_cmds_per_min": w["redis_commands"] * per_min,
        "service_requests_per_min": sum(w["service_requests"].values()) * per_min,
        "memory_mib": sum(mem_by_role.values()),
        "memory_mib_by_role": dict(mem_by_role),
    }


def per_workflow_cost(t: dict) -> dict | None:
    """The trickle window's consumption beyond the idle rate, divided by its workflows."""
    idle, busy = t.get("idle"), t.get("trickle")
    if not idle or not busy or not busy["workflows"]:
        return None
    scale = busy["seconds"] / idle["seconds"]
    cpu = sum(busy["cpu_seconds"].values()) - sum(idle["cpu_seconds"].values()) * scale
    cmds = busy["redis_commands"] - idle["redis_commands"] * scale
    reqs = sum(busy["service_requests"].values()) - sum(idle["service_requests"].values()) * scale
    n = busy["workflows"]
    return {"cpu_ms": 1000 * cpu / n, "redis_commands": cmds / n, "service_requests": reqs / n,
            "service_requests_by_kind": {k: v / n for k, v in busy["service_requests"].items()}}


def analyse_costs(trials: list[dict]) -> dict:
    """Amendment 5 (PROTOCOL.md 12): per point and arm; NEW as shipped against OLD and against its variant."""
    rows = [t for t in trials if t["scenario"] in COST_SCENARIOS and not t.get("warmup")]
    if not rows:
        return {}
    result: dict = {}

    def compare(by_arm: dict, metric) -> dict:
        out = {}
        new = [metric(t) for t in by_arm.get("new", [])]
        new = [x for x in new if x is not None]
        for other in sorted(by_arm, key=_arm_order):
            if other == "new":
                continue
            vals = [x for x in (metric(t) for t in by_arm[other]) if x is not None]
            if vals and new:
                out[f"new_vs_{other.replace('/', '_')}"] = compare_continuous(vals, new)
        return out

    def errors(ts):
        return sum("error" in t for t in ts)

    # RQ12, service delay
    delay = defaultdict(lambda: defaultdict(list))
    for t in rows:
        if t["scenario"] == "service-delay":
            delay[t.get("delay_ms")][arm(t)].append(t)
    if delay:
        med_submit = lambda t: _med(t["submit_latencies"]) if t.get("submit_latencies") else None
        points = {}
        for d in sorted(delay):
            by = {a: [t for t in ts if "error" not in t] for a, ts in delay[d].items()}
            points[str(d)] = {
                "arms": {a: {
                    "n": len(ts), "errors": errors(delay[d][a]),
                    "completed": sum(bool(t.get("job_completed")) for t in ts),
                    "submit_ms_median": 1000 * _med([med_submit(t) for t in ts if med_submit(t) is not None]) if ts else None,
                    "batch_submit_s_median": _med(values(ts, "submit_seconds")),
                    "throughput_median": _med(values(ts, "throughput_wf_per_min")),
                    "makespan_median": _med(values(ts, "makespan_seconds")),
                    "probe_rtt_ms_median": _med(values(ts, "probe_rtt_ms")),
                } for a, ts in sorted(by.items(), key=lambda kv: _arm_order(kv[0]))},
                "submit": compare(by, med_submit),
                "throughput": compare(by, lambda t: t.get("throughput_wf_per_min")),
            }
        slopes = {}
        for a in {a for d in delay for a in delay[d]}:
            xs, ys = [], []
            for d in delay:
                for t in delay[d].get(a, []):
                    if "error" not in t and med_submit(t) is not None:
                        xs.append(d)
                        ys.append(1000 * med_submit(t))
            slopes[a] = _slope(xs, ys)  # ms of submission time per ms of delay
        result["service-delay"] = {"points": points, "submit_slope": dict(sorted(slopes.items(), key=lambda kv: _arm_order(kv[0])))}

    # RQ12, hung service
    hung = defaultdict(list)
    for t in rows:
        if t["scenario"] == "hung-submit":
            hung[arm(t)].append(t)
    if hung:
        by = {a: [t for t in ts if "error" not in t] for a, ts in hung.items()}
        result["hung-submit"] = {
            "arms": {a: {"n": len(ts), "errors": errors(hung[a]),
                         "accepted": sum(bool(t.get("accepted")) for t in ts),
                         "completed": sum(bool(t.get("job_completed")) for t in ts),
                         "submit_s_median": _med(values(ts, "submit_seconds")),
                         "submit_s_iqr": _iqr(values(ts, "submit_seconds"))}
                     for a, ts in sorted(by.items(), key=lambda kv: _arm_order(kv[0]))},
            "submit": compare(by, lambda t: t.get("submit_seconds")),
        }

    # RQ13, running cost
    cost = defaultdict(list)
    for t in rows:
        if t["scenario"] == "running-cost":
            cost[arm(t)].append(t)
    if cost:
        by = {a: [t for t in ts if "error" not in t and t.get("idle")] for a, ts in cost.items()}
        idle = lambda key: (lambda t: window_rates(t["idle"])[key])
        pw = lambda key: (lambda t: (per_workflow_cost(t) or {}).get(key))
        arms = {}
        for a, ts in sorted(by.items(), key=lambda kv: _arm_order(kv[0])):
            roles = sorted({r for t in ts for r in window_rates(t["idle"])["cpu_s_per_min_by_role"]})
            kinds = sorted({k for t in ts for k in (per_workflow_cost(t) or {}).get("service_requests_by_kind", {})})
            arms[a] = {
                "n": len(ts), "errors": errors(cost[a]),
                "completed": sum(bool(t.get("job_completed")) for t in ts),
                "idle_cpu_s_per_min": _med([idle("cpu_s_per_min")(t) for t in ts]),
                "idle_cpu_s_per_min_by_role": {r: _med([window_rates(t["idle"])["cpu_s_per_min_by_role"].get(r, 0) for t in ts]) for r in roles},
                "idle_redis_cmds_per_min": _med([idle("redis_cmds_per_min")(t) for t in ts]),
                "idle_service_requests_per_min": _med([idle("service_requests_per_min")(t) for t in ts]),
                "idle_memory_mib": _med([idle("memory_mib")(t) for t in ts]),
                "trickle_memory_mib": _med([window_rates(t["trickle"])["memory_mib"] for t in ts]),
                "per_workflow_cpu_ms": _med([x for x in map(pw("cpu_ms"), ts) if x is not None]),
                "per_workflow_redis_commands": _med([x for x in map(pw("redis_commands"), ts) if x is not None]),
                "per_workflow_service_requests": _med([x for x in map(pw("service_requests"), ts) if x is not None]),
                "per_workflow_service_requests_by_kind": {k: _med([(per_workflow_cost(t) or {}).get("service_requests_by_kind", {}).get(k, 0) for t in ts]) for k in kinds},
            }
        result["running-cost"] = {
            "arms": arms,
            "compare": {
                "idle_cpu_s_per_min": compare(by, idle("cpu_s_per_min")),
                "idle_redis_cmds_per_min": compare(by, idle("redis_cmds_per_min")),
                "per_workflow_cpu_ms": compare(by, pw("cpu_ms")),
                "per_workflow_redis_commands": compare(by, pw("redis_commands")),
                "per_workflow_service_requests": compare(by, pw("service_requests")),
            },
        }
    return result


def exact_ci(k: int, n: int) -> tuple[float, float] | None:
    """Clopper-Pearson 95 % interval of a proportion k/n."""
    if not n:
        return None
    ci = stats.binomtest(k, n).proportion_ci(confidence_level=0.95, method="exact")
    return (float(ci.low), float(ci.high))


FAULT_SCENARIOS = ("outage", "short-outage", "concurrent-outage", "worker-death")
EQUIVALENCE_COUNTS = ("jobs", "equivalent", "divergent", "missing", "with_history",
                      "not_clean", "not_clean_equivalent", "reclaimed", "reclaimed_equivalent",
                      "verified", "not_verified")


def history_inconsistent(trials: list[dict]) -> list[dict]:
    """Trials where the services executed more often than NEW handed tasks out.
    """
    bad = []
    for t in trials:
        outputs = t.get("outputs") or []
        if (t.get("version") != "new" or "error" in t or not outputs
                or len(outputs) != len(t.get("carriers", []))
                or any(o.get("history") is None for o in outputs)):
            continue
        claims = sum(o["history"]["claims"] for o in outputs)
        executions = sum(t.get("executions", {}).values())
        if executions > claims:
            bad.append({"scenario": t["scenario"], "rep": t.get("rep"),
                        "claims": claims, "executions": executions})
    return bad


def analyse_equivalence(trials: list[dict]) -> dict:
    """Are completed jobs output-equivalent?
    """
    cells: dict[tuple, dict] = defaultdict(lambda: dict.fromkeys(EQUIVALENCE_COUNTS, 0))
    for t in trials:
        if t.get("warmup") or "error" in t or "outputs" not in t:
            continue
        if t.get("variant", "default") != "default":
            continue
        c = cells[(t["scenario"], t["use_case"], t["version"])]
        for o in t["outputs"]:
            c["jobs"] += 1
            c[o["verdict"]] += 1
            if o.get("verified") is not None:
                c["verified" if o["verified"] else "not_verified"] += 1
            history = o.get("history")
            if history is None:
                continue
            c["with_history"] += 1
            for kind, flag in (("not_clean", not history["clean"]),
                               ("reclaimed", history["reclaims"] > 0)):
                if flag:
                    c[kind] += 1
                    c[f"{kind}_equivalent"] += o["verdict"] == "equivalent"

    def summarise(c: dict) -> dict:
        return c | {"rate": c["equivalent"] / c["jobs"] if c["jobs"] else None,
                    "ci95": exact_ci(c["equivalent"], c["jobs"]),
                    "silent_divergences": c["divergent"] + c["missing"]}

    result: dict = {"cells": {}, "pooled_faults": {}, "reference_valid": {},
                    "history_inconsistent": history_inconsistent(trials)}
    pooled: dict[tuple, dict] = defaultdict(lambda: dict.fromkeys(EQUIVALENCE_COUNTS, 0))
    for (scenario, uc, version), c in sorted(cells.items()):
        result["cells"][f"{scenario}/{uc}/{version}"] = summarise(c)
        if scenario == "nominal":
            ok = result["reference_valid"].get(uc, True) and c["equivalent"] == c["jobs"]
            result["reference_valid"][uc] = ok
        elif scenario in FAULT_SCENARIOS:
            for key in c:
                pooled[(uc, version)][key] += c[key]
    for (uc, version), c in sorted(pooled.items()):
        result["pooled_faults"][f"{uc}/{version}"] = summarise(c)
    return result


# layer 1: Task 3.3
def analyse_t33(run: Path) -> dict:
    path = run / "t33.csv"
    if not path.exists():
        return {}
    rows = list(csv.DictReader(path.open()))
    latest: dict[tuple, dict] = {}
    for row in rows:  # the last run of a script wins
        latest[(row["node"], row["script"], row["version"])] = row
    table = defaultdict(dict)
    for (node, script, version), row in latest.items():
        table[(node, script)][version] = row["verdict"].strip()
    transitions = defaultdict(int)
    for verdicts in table.values():
        transitions[(_norm(verdicts.get("old")), _norm(verdicts.get("new")))] += 1
    return {
        "per_script": {f"{n}/{s}": v for (n, s), v in sorted(table.items())},
        "transitions": {f"{a} -> {b}": c for (a, b), c in sorted(transitions.items())},
        "regressions": [f"{n}/{s}" for (n, s), v in table.items()
                        if _norm(v.get("old")) == "PASS" and _norm(v.get("new")) not in ("PASS", "not run")],
    }


def _norm(verdict: str | None) -> str:
    if not verdict:
        return "not run"
    v = verdict.upper()
    for key in ("DEFECT", "FAIL", "BLOCKED", "PASS", "RECORDED"):
        if key in v:
            return {"DEFECT": "DEFECT/FAIL", "FAIL": "DEFECT/FAIL"}.get(key, key)
    return v


# layer 2: own suites
def analyse_suites(run: Path) -> dict:
    base = run / "suites"
    if not base.exists():
        return {}
    out = {}
    for cell in sorted(p for p in base.iterdir() if p.is_dir() and p.name != "work"):
        r = {}
        junit = cell / "junit.xml"
        if junit.exists():
            root = ET.parse(junit).getroot()
            suites = root.findall("testsuite") or [root]
            tot = {k: sum(int(s.get(k, 0)) for s in suites) for k in ("tests", "failures", "errors", "skipped")}
            tot["passed"] = tot["tests"] - tot["failures"] - tot["errors"] - tot["skipped"]
            r["tests"] = tot
            r["failed_cases"] = [
                f"{c.get('classname')}::{c.get('name')}"
                for s in suites for c in s.iter("testcase")
                if c.find("failure") is not None or c.find("error") is not None
            ]
            if cell.name.startswith("diff-"):  
                r["cases"] = {
                    c.get("name"): "fail" if (c.find("failure") is not None or c.find("error") is not None)
                    else "pass"
                    for s in suites for c in s.iter("testcase")
                }
        cov = cell / "coverage.json"
        if cov.exists():
            t = json.loads(cov.read_text())["totals"]
            r["coverage"] = {
                "line_pct": t["percent_covered"],
                "statements": t["num_statements"],
                "branches": t.get("num_branches"),
                "branch_pct": 100 * t["covered_branches"] / t["num_branches"] if t.get("num_branches") else None,
            }
        diff = cell / "differential.json"
        if diff.exists():
            r["differential"] = json.loads(diff.read_text())
        out[cell.name] = r
    return out


# output: LaTeX
def fmt(x, digits=1):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "--"
    return f"{x:.{digits}f}" if isinstance(x, float) else str(x)


def fmt_p(p):
    if p is None or (isinstance(p, float) and math.isnan(p)):
        return "--"
    return "$<$0.001" if p < 0.001 else f"{p:.3f}"


def tex_escape(s: str) -> str:
    return s.replace("_", r"\_").replace("%", r"\%").replace("&", r"\&").replace("->", r"$\to$")


def write_tables(out: Path, summary: dict) -> None:
    trials = summary.get("trials", {})
    if trials:
        lines = [
            r"\begin{tabular}{llrrrrrl}", r"\toprule",
            r"Scenario & Metric & OLD median & NEW median & $p$ & Cliff's $\delta$ & effect \\",
            r"\midrule",
        ]
        for name, r in trials.items():
            for key, label in (("completion", "completed"), ("refused", "refused at submission")):
                if key in r:
                    c = r[key]
                    lines.append(
                        f"{tex_escape(name)} & {label} & {c['k_old']}/{c['n_old']} & {c['k_new']}/{c['n_new']} "
                        f"& {fmt_p(c['fisher_p'])} & & \\\\"
                    )
            for key, label in (("recovery_seconds", "recovery [s]"), ("reexecutions", "re-executed tasks"),
                               ("operator_actions", "operator actions"), ("e2e_seconds", "latency [s]"),
                               ("throughput_wf_per_min", "throughput [wf/min]"),
                               ("error_visible_seconds", "error visible after [s]"),
                               ("tasks_started", "tasks started"),
                               ("residue_endpoints", "workflow keys left in Redis")):
                if key in r:
                    m = r[key]
                    lines.append(
                        f" & {label} & {fmt(m['median_old'], 2)} & {fmt(m['median_new'], 2)} & "
                        f"{fmt_p(m.get('mwu_p'))} & {fmt(m.get('cliffs_delta'), 2)} & {m.get('effect', '')} \\\\"
                    )
            for key, label in (("detection_seconds", "detection [s]"), ("first_alert_seconds", "first alert [s]")):
                if key in r:
                    lines.append(f" & {label} & n/s & {fmt(r[key]['new']['median_new'], 2)} & & & \\\\")
            lines.append(r"\midrule")
        lines[-1] = r"\bottomrule"
        lines.append(r"\end{tabular}")
        (out / "table_trials.tex").write_text("\n".join(lines) + "\n")

    equivalence = summary.get("equivalence", {})

    def recovered(c: dict) -> str:
        """Jobs that needed recovery, as 'equivalent/total', from NEW's history (n/s on OLD)."""
        if not c["with_history"]:
            return "n/s & n/s"
        return (f"{c['not_clean_equivalent']}/{c['not_clean']} & "
                f"{c['reclaimed_equivalent']}/{c['reclaimed']}")

    if equivalence.get("cells"):
        lines = [
            r"\begin{tabular}{lllrrrrlll}", r"\toprule",
            r"Scenario & Use case & Version & Jobs & Equivalent & Divergent & Missing & 95\,\% CI"
            r" & Not clean & Reclaimed \\",
            r"\midrule",
        ]
        for name, c in equivalence["cells"].items():
            scenario, uc, version = name.split("/")
            ci = c["ci95"]
            ci_text = f"[{ci[0]:.2f}, {ci[1]:.2f}]" if ci else ""
            lines.append(
                f"{tex_escape(scenario)} & {tex_escape(uc)} & {version.upper()} & {c['jobs']} & "
                f"{c['equivalent']} & {c['divergent']} & {c['missing']} & {ci_text} & {recovered(c)} \\\\"
            )
        lines.append(r"\midrule")
        for name, c in equivalence["pooled_faults"].items():
            uc, version = name.split("/")
            ci = c["ci95"]
            ci_text = f"[{ci[0]:.2f}, {ci[1]:.2f}]" if ci else ""
            lines.append(
                f"all faults & {tex_escape(uc)} & {version.upper()} & {c['jobs']} & "
                f"{c['equivalent']} & {c['divergent']} & {c['missing']} & {ci_text} & {recovered(c)} \\\\"
            )
        lines += [r"\bottomrule", r"\end{tabular}"]
        (out / "table_equivalence.tex").write_text("\n".join(lines) + "\n")

    overhead = summary.get("overhead", {})
    if overhead:
        a, b, c = overhead["a_old_vs_new_default"], overhead["b_old_vs_new_checks_off"], overhead["c_checks_cost"]
        ci = c.get("ci_median_diff") or (None, None)
        lines = [
            r"\begin{tabular}{lrrl}", r"\toprule",
            r"Comparison (nominal latency) & NEW/OLD ratio & $p_{\mathrm{TOST}}$ & equivalent ($\pm$5\,\%) \\",
            r"\midrule",
            f"OLD vs NEW (shipped) & {fmt(a.get('ratio_new_over_old'), 3)} & {fmt_p(a.get('p_tost'))} & {'yes' if a.get('equivalent') else 'no'} \\\\",
            f"OLD vs NEW (checks off) & {fmt(b.get('ratio_new_over_old'), 3)} & {fmt_p(b.get('p_tost'))} & {'yes' if b.get('equivalent') else 'no'} \\\\",
            r"\midrule",
            f"\\multicolumn{{4}}{{l}}{{Cost of the checks: median {fmt(c.get('median_diff_seconds'), 3)}\\,s "
            f"(95\\,\\% CI {fmt(ci[0], 3)} to {fmt(ci[1], 3)}\\,s), MWU $p$ = {fmt_p(c.get('mwu_p'))}}} \\\\",
            r"\bottomrule", r"\end{tabular}",
        ]
        (out / "table_overhead.tex").write_text("\n".join(lines) + "\n")

    dispersion = summary.get("dispersion", {})
    if dispersion:
        lines = [
            r"\begin{tabular}{llrrrrrrrrrr}", r"\toprule",
            r"Use case & Version & $n$ & min & P5 & median & P95 & max & IQR & SD & CV"
            r" & $p_{\mathrm{BF}}$ / $p_{\mathrm{FK}}$ \\",
            r"\midrule",
        ]
        for uc, r in dispersion.items():
            test = f"{fmt_p(r.get('brown_forsythe_p'))} / {fmt_p(r.get('fligner_p'))}"
            for i, (cell, d) in enumerate(r["cells"].items()):
                version, variant = cell.split("/")
                label = version.upper() + (f" ({variant})" if variant != "default" else "")
                lines.append(
                    f"{tex_escape(uc) if i == 0 else ''} & {label} & {d['n']} & {fmt(d['min'], 3)} & "
                    f"{fmt(d['p5'], 3)} & {fmt(d['median'], 3)} & {fmt(d['p95'], 3)} & {fmt(d['max'], 3)} & "
                    f"{fmt(d['iqr'], 3)} & {fmt(d['sd'], 3)} & {fmt(d['cv'], 3)} & {test if i == 0 else ''} \\\\"
                )
            lines.append(r"\midrule")
        lines[-1] = r"\bottomrule"
        lines.append(r"\end{tabular}")
        (out / "table_dispersion.tex").write_text("\n".join(lines) + "\n")

    pickup = summary.get("pickup", {})
    if pickup:
        lines = [
            r"\begin{tabular}{lrlrrrrrrr}", r"\toprule",
            r"Use case & Jitter [s] & Version & $n$ & Gap [s] & Pickup [s] (P5--P95) & Work [s]"
            r" & Pickup share & End-to-end [s] & Timeouts \\",
            r"\midrule",
        ]
        for name, c in pickup["cells"].items():
            uc, jitter, version = name.split("/")
            pk = c["pickup"]
            lines.append(
                f"{tex_escape(uc)} & {jitter} & {version.upper()} & {c['n']} & {fmt(c['submit_gap_median'], 2)} & "
                f"{fmt(pk['median'], 2)} ({fmt(pk['p5'], 2)}--{fmt(pk['p95'], 2)}) & "
                f"{fmt(c['work']['median'], 3)} & {fmt(c['pickup_share_median'], 2)} & "
                f"{fmt(c['e2e']['median'], 2)} & {c['socket_timeouts']} \\\\"
            )
        lines += [r"\bottomrule", r"\end{tabular}"]
        (out / "table_pickup.tex").write_text("\n".join(lines) + "\n")

    sweeps = summary.get("sweeps", {})
    if sweeps.get("scale"):
        lines = [
            r"\begin{tabular}{rrllrrrrrrl}", r"\toprule",
            r"$W$ & $K$ & & completed & thr. [wf/min] & lat. P50 [s] & lat. P95 [s] & submit [s]"
            r" & re-exec. & equivalent & NEW/OLD thr. ($\delta$) \\",
            r"\midrule",
        ]
        for name, r in sweeps["scale"].items():
            w, k = (part.split("=")[1] for part in name.split("/"))
            for i, v in enumerate(VERSIONS):
                d = r[v]
                eq = d["equivalence"]
                ratio = ""
                if i == 0 and "throughput" in r:
                    ratio = f"{fmt(r['throughput_ratio_new_over_old'], 2)} ({fmt(r['throughput'].get('cliffs_delta'), 2)})"
                lines.append(
                    f"{w if i == 0 else ''} & {k if i == 0 else ''} & {v.upper()} & "
                    f"{d['workflows_completed']}/{d['workflows']} & {fmt(d['throughput_median'], 0)} & "
                    f"{fmt(d['latency_p50_median'], 2)} & {fmt(d['latency_p95_median'], 2)} & "
                    f"{fmt(d['submit_seconds_median'], 2)} & {d['reexecutions']} & "
                    f"{eq['equivalent']}/{eq['jobs']} & {ratio} \\\\"
                )
        lines += [r"\bottomrule", r"\end{tabular}"]
        (out / "table_scaling.tex").write_text("\n".join(lines) + "\n")

    for scenario, fname, label in (("inline-size", "table_inline_size.tex", "inline bytes"),
                                   ("data-size", "table_data_size.tex", "records")):
        if not sweeps.get(scenario):
            continue
        extra = "equivalent" if scenario == "inline-size" else "raw file [B]"
        lines = [
            r"\begin{tabular}{rlrrrrrl}", r"\toprule",
            f"{label} & & accepted & completed & latency [s] & makespan [s] & {extra} & NEW/OLD lat. ($\\delta$) \\\\",
            r"\midrule",
        ]
        for name, r in sweeps[scenario].items():
            for i, v in enumerate(VERSIONS):
                d = r[v]
                if scenario == "inline-size":
                    eq = d["equivalence"]
                    col = f"{eq['equivalent']}/{eq['jobs']}"
                else:
                    col = fmt(d.get("raw_bytes"))
                ratio = ""
                if i == 0 and "e2e" in r:
                    ratio = f"{fmt(r['e2e_ratio_new_over_old'], 2)} ({fmt(r['e2e'].get('cliffs_delta'), 2)})"
                lines.append(
                    f"{name if i == 0 else ''} & {v.upper()} & {d['accepted']}/{r['n'][v]} & "
                    f"{d['completed']}/{r['n'][v]} & {fmt(d['e2e_median'], 2)} & {fmt(d['makespan_median'], 2)} & "
                    f"{col} & {ratio} \\\\"
                )
            if scenario == "data-size":
                lines.append(f"\\multicolumn{{8}}{{l}}{{\\quad output files identical across trials and versions: "
                             f"{'yes' if r.get('outputs_consistent') else 'NO'} "
                             f"({r.get('distinct_outputs')} distinct)}} \\\\")
        lines += [r"\bottomrule", r"\end{tabular}"]
        (out / fname).write_text("\n".join(lines) + "\n")

    costs = summary.get("costs", {})
    if costs.get("service-delay") or costs.get("hung-submit"):
        def delta(cmp: dict, key: str) -> str:
            c = cmp.get(key)
            return f"{fmt(c.get('cliffs_delta'), 2)} ({fmt_p(c.get('mwu_p'))})" if c else ""
        lines = [
            r"\begin{tabular}{llrrrrll}", r"\toprule",
            r"delay [ms] & arm & completed & submit [ms] & batch submit [s] & thr. [wf/min]"
            r" & $\delta$ vs OLD ($p$) & $\delta$ vs check off ($p$) \\",
            r"\midrule",
        ]
        for d, r in costs.get("service-delay", {}).get("points", {}).items():
            for i, (a, x) in enumerate(r["arms"].items()):
                first = a == "new"
                lines.append(
                    f"{d if i == 0 else ''} & {tex_escape(a.upper())} & {x['completed']}/{x['n']} & "
                    f"{fmt(x['submit_ms_median'], 1)} & {fmt(x['batch_submit_s_median'], 2)} & "
                    f"{fmt(x['throughput_median'], 0)} & "
                    f"{delta(r['submit'], 'new_vs_old') if first else ''} & "
                    f"{delta(r['submit'], 'new_vs_new_auth-check-off') if first else ''} \\\\"
                )
            lines.append(r"\midrule")
        hung = costs.get("hung-submit")
        if hung:
            for i, (a, x) in enumerate(hung["arms"].items()):
                first = a == "new"
                lines.append(
                    f"{'hung' if i == 0 else ''} & {tex_escape(a.upper())} & {x['completed']}/{x['n']} & "
                    f"{fmt(1000 * x['submit_s_median'] if x['submit_s_median'] is not None else None, 1)} & & & "
                    f"{delta(hung['submit'], 'new_vs_old') if first else ''} & "
                    f"{delta(hung['submit'], 'new_vs_new_auth-check-off') if first else ''} \\\\"
                )
            lines.append(r"\midrule")
        for a, sl in costs.get("service-delay", {}).get("submit_slope", {}).items():
            if sl:
                lines.append(f"\\multicolumn{{8}}{{l}}{{slope of submit time, {tex_escape(a.upper())}: "
                             f"{fmt(sl['slope'], 2)} ms per ms of delay (95\\,\\% CI {fmt(sl['ci95'][0], 2)} to "
                             f"{fmt(sl['ci95'][1], 2)})}} \\\\")
        if lines[-1] == r"\midrule":
            lines.pop()
        lines += [r"\bottomrule", r"\end{tabular}"]
        (out / "table_service_delay.tex").write_text("\n".join(lines) + "\n")

    rc = costs.get("running-cost")
    if rc:
        metrics = (("idle_cpu_s_per_min", "idle CPU [s/min]", 2), ("idle_redis_cmds_per_min", "idle Redis commands [/min]", 0),
                   ("idle_service_requests_per_min", "idle service requests [/min]", 1),
                   ("idle_memory_mib", "memory, idle [MiB]", 0), ("trickle_memory_mib", "memory, trickle [MiB]", 0),
                   ("per_workflow_cpu_ms", "CPU per workflow [ms]", 1),
                   ("per_workflow_redis_commands", "Redis commands per workflow", 0),
                   ("per_workflow_service_requests", "service requests per workflow", 1))
        arms = list(rc["arms"])
        lines = [r"\begin{tabular}{l" + "r" * len(arms) + "l}", r"\toprule",
                 "metric & " + " & ".join(tex_escape(a.upper()) for a in arms) + r" & NEW vs OLD $\delta$ ($p$) \\",
                 r"\midrule"]
        for key, label, digits in metrics:
            c = rc["compare"].get(key, {}).get("new_vs_old")
            d = f"{fmt(c.get('cliffs_delta'), 2)} ({fmt_p(c.get('mwu_p'))})" if c else ""
            lines.append(f"{label} & " + " & ".join(fmt(rc["arms"][a][key], digits) for a in arms) + f" & {d} \\\\")
        lines += [r"\midrule", "trials & " + " & ".join(str(rc["arms"][a]["n"]) for a in arms) + r" & \\",
                  r"\bottomrule", r"\end{tabular}"]
        (out / "table_running_cost.tex").write_text("\n".join(lines) + "\n")

    suites_d = summary.get("suites", {})
    old_cases = suites_d.get("diff-on-old", {}).get("cases", {})
    new_cases = suites_d.get("diff-on-new", {}).get("cases", {})
    if old_cases or new_cases:
        lines = [r"\begin{tabular}{lll}", r"\toprule", r"Differential test & OLD & NEW \\", r"\midrule"]
        for name in sorted(set(old_cases) | set(new_cases)):
            lines.append(f"{tex_escape(name.removeprefix('test_'))} & {old_cases.get(name, '--')} & "
                         f"{new_cases.get(name, '--')} \\\\")
        lines += [r"\bottomrule", r"\end{tabular}"]
        (out / "table_differential.tex").write_text("\n".join(lines) + "\n")

    t33 = summary.get("t33", {})
    if t33:
        lines = [r"\begin{tabular}{lll}", r"\toprule", r"Script & OLD & NEW \\", r"\midrule"]
        for name, v in t33["per_script"].items():
            lines.append(f"{tex_escape(name)} & {tex_escape(v.get('old', '--'))} & {tex_escape(v.get('new', '--'))} \\\\")
        lines += [r"\bottomrule", r"\end{tabular}"]
        (out / "table_t33.tex").write_text("\n".join(lines) + "\n")

    suites = summary.get("suites", {})
    if suites:
        lines = [
            r"\begin{tabular}{lrrrrr}", r"\toprule",
            r"Cell & tests & passed & failed & line cov. [\%] & branch cov. [\%] \\", r"\midrule",
        ]
        for cell, r in suites.items():
            t, c = r.get("tests", {}), r.get("coverage", {})
            lines.append(
                f"{cell} & {t.get('tests', '--')} & {t.get('passed', '--')} & "
                f"{(t.get('failures', 0) + t.get('errors', 0)) if t else '--'} & "
                f"{fmt(c.get('line_pct'))} & {fmt(c.get('branch_pct'))} \\\\"
            )
        lines += [r"\bottomrule", r"\end{tabular}"]
        (out / "table_suites.tex").write_text("\n".join(lines) + "\n")


# output: figures
def write_figures(out: Path, trials: list[dict]) -> None:
    if not trials:
        return
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update({
        "font.size": 9, "axes.edgecolor": MUTED, "axes.labelcolor": INK, "text.color": INK,
        "xtick.color": MUTED, "ytick.color": MUTED, "axes.spines.top": False,
        "axes.spines.right": False, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
        "axes.axisbelow": True, "legend.frameon": False, "hatch.color": "white", "hatch.linewidth": 1.0,
    })

    by = defaultdict(lambda: {"old": [], "new": []})
    for t in trials:
        if not t.get("warmup") and "error" not in t and t["scenario"] not in SWEEP_SCENARIOS + COST_SCENARIOS + DIAGNOSTIC_SCENARIOS:
            by[f"{t['scenario']}\n{t['use_case']}"][t["version"]].append(t)
    names = sorted(by)
    if not names:
        write_sweep_figures(out, trials, plt)
        write_cost_figures(out, trials, plt)
        return

    # 1. completion rate per scenario
    fig, ax = plt.subplots(figsize=(6.3, 2.6))
    x = np.arange(len(names))
    w = 0.38
    for i, v in enumerate(VERSIONS):
        rates = [100 * np.mean([bool(t.get("job_completed")) for t in by[n][v]]) if by[n][v] else 0 for n in names]
        bars = ax.bar(x + (i - 0.5) * w, rates, w - 0.02, color=COLOR[v], hatch=HATCH[v],
                      label=v.upper(), edgecolor="white", linewidth=0)
        for b, rate, n in zip(bars, rates, names):
            if by[n][v]:
                ax.annotate(f"{rate:.0f}", (b.get_x() + b.get_width() / 2, rate), ha="center",
                            va="bottom", fontsize=7, color=MUTED, xytext=(0, 1), textcoords="offset points")
    ax.set_xticks(x, names, fontsize=7)
    ax.set_ylabel("jobs completed [%]")
    ax.set_ylim(0, 110)
    ax.grid(axis="x", visible=False)
    ax.legend(loc="upper left", bbox_to_anchor=(0, 1.18), ncol=2)
    fig.tight_layout()
    fig.savefig(out / "fig_completion.pdf")
    plt.close(fig)

    # 2. distributions
    for key, label, fname in (("recovery_seconds", "time from service restored to job completed [s]", "fig_recovery.pdf"),
                              ("e2e_seconds", "end-to-end latency, no fault [s]", "fig_latency.pdf")):
        rows = [n for n in names if any(values(by[n][v], key) for v in VERSIONS)]
        if not rows:
            continue
        fig, ax = plt.subplots(figsize=(6.3, 0.55 * len(rows) * 2 + 0.9))
        yticks, ylabels = [], []
        for r, n in enumerate(rows):
            for i, v in enumerate(VERSIONS):
                y = r * 2.4 + i
                vals = values(by[n][v], key)
                yticks.append(y)
                ylabels.append(f"{n.replace(chr(10), ' / ')}  {v.upper()}")
                if not vals:
                    ax.annotate("never completed" if key == "recovery_seconds" else "no data",
                                (0, y), va="center", fontsize=7, color=MUTED,
                                xytext=(6, 0), textcoords="offset points")
                    continue
                jitter = RNG.uniform(-0.18, 0.18, len(vals))
                ax.scatter(vals, y + jitter, s=18, color=COLOR[v], edgecolors="white", linewidths=0.8,
                           marker="o" if v == "new" else "D", zorder=3)
                ax.plot([np.median(vals)] * 2, [y - 0.32, y + 0.32], color=INK, linewidth=1.5, zorder=2)
        ax.set_yticks(yticks, ylabels, fontsize=7)
        ax.invert_yaxis()
        ax.set_xlabel(label)
        ax.set_xlim(left=0)
        ax.grid(axis="y", visible=False)
        fig.tight_layout()
        fig.savefig(out / fname)
        plt.close(fig)

    # 3. exploratory
    lat = nominal_latencies(trials)
    ucs = sorted({k[0] for k in lat})
    if ucs:
        fig, axes = plt.subplots(1, len(ucs), figsize=(6.3, 2.3), squeeze=False)
        for ax, uc in zip(axes[0], ucs):
            for v in VERSIONS:
                x = np.sort(lat.get((uc, v, "default"), []))
                if len(x):
                    ax.step(x, np.arange(1, len(x) + 1) / len(x), where="post", color=COLOR[v],
                            linewidth=1.5, linestyle="--" if v == "old" else "-", label=v.upper())
            ax.set_title(uc, fontsize=8)
            ax.set_xlabel("latency, no fault [s]", fontsize=7)
            ax.set_ylim(0, 1.02)
        axes[0][0].set_ylabel("fraction of trials")
        axes[0][0].legend(loc="upper left", fontsize=7)
        fig.tight_layout()
        fig.savefig(out / "fig_latency_ecdf.pdf")
        plt.close(fig)

    write_sweep_figures(out, trials, plt)
    write_cost_figures(out, trials, plt)


ARM_STYLE = {"old": (COLOR["old"], "--", "D"), "new": (COLOR["new"], "-", "o")}


def _arm_style(a: str):
    return ARM_STYLE.get(a, (MUTED, ":", "s"))


def write_cost_figures(out: Path, trials: list[dict], plt) -> None:
    rows = [t for t in trials if t["scenario"] in COST_SCENARIOS and not t.get("warmup") and "error" not in t]
    delay = [t for t in rows if t["scenario"] == "service-delay"]
    if delay:
        fig, axes = plt.subplots(1, 2, figsize=(6.3, 2.6))
        by = defaultdict(list)
        for t in delay:
            by[arm(t)].append(t)
        for a in sorted(by, key=_arm_order):
            color, ls, marker = _arm_style(a)
            for ax, y_of in ((axes[0], lambda t: 1000 * _med(t["submit_latencies"]) if t.get("submit_latencies") else None),
                             (axes[1], lambda t: t.get("throughput_wf_per_min"))):
                pts = [(t["delay_ms"], y_of(t)) for t in by[a] if y_of(t) is not None]
                xs = sorted({x for x, _ in pts})
                ax.scatter([x for x, _ in pts], [y for _, y in pts], s=10, color=color, alpha=0.5, marker=marker, linewidths=0)
                ax.plot(xs, [float(np.median([y for x2, y in pts if x2 == x])) for x in xs], color=color,
                        linestyle=ls, linewidth=1.5, label=a.upper())
        axes[0].set_ylabel("time to submit one workflow [ms]")
        axes[1].set_ylabel("throughput, 10 workflows [wf/min]")
        for ax in axes:
            ax.set_xlabel("delay added to every service [ms]", fontsize=7)
            ax.set_ylim(bottom=0)
        axes[0].legend(loc="upper left", fontsize=7)
        fig.tight_layout()
        fig.savefig(out / "fig_service_delay.pdf")
        plt.close(fig)

    cost = [t for t in rows if t["scenario"] == "running-cost" and t.get("idle")]
    if cost:
        panels = (("idle CPU [s/min]", lambda t: window_rates(t["idle"])["cpu_s_per_min"]),
                  ("CPU per workflow [ms]", lambda t: (per_workflow_cost(t) or {}).get("cpu_ms")),
                  ("Redis commands per workflow", lambda t: (per_workflow_cost(t) or {}).get("redis_commands")),
                  ("service requests per workflow", lambda t: (per_workflow_cost(t) or {}).get("service_requests")))
        by = defaultdict(list)
        for t in cost:
            by[arm(t)].append(t)
        arms = sorted(by, key=_arm_order)
        fig, axes = plt.subplots(1, len(panels), figsize=(6.3, 2.4))
        for ax, (label, y_of) in zip(axes, panels):
            for i, a in enumerate(arms):
                color, _, marker = _arm_style(a)
                ys = [y for y in map(y_of, by[a]) if y is not None]
                if not ys:
                    continue
                ax.bar(i, float(np.median(ys)), 0.6, color=color, alpha=0.35, edgecolor="none")
                ax.scatter(i + RNG.uniform(-0.15, 0.15, len(ys)), ys, s=10, color=color, marker=marker, linewidths=0, zorder=3)
            ax.set_xticks(range(len(arms)), [a.upper().replace("/", "\n") for a in arms], fontsize=6)
            ax.set_title(label, fontsize=7)
            ax.set_ylim(bottom=0)
            ax.grid(axis="x", visible=False)
        fig.tight_layout()
        fig.savefig(out / "fig_running_cost.pdf")
        plt.close(fig)


def _sweep_series(ax, xs_by_version: dict, x_of, y_of, log_x=True):
    """Every trial as a point, the median per x as a line."""
    for v in VERSIONS:
        pts = [(x_of(t), y_of(t)) for t in xs_by_version.get(v, []) if y_of(t) is not None]
        if not pts:
            continue
        xs = sorted({x for x, _ in pts})
        med = [float(np.median([y for x2, y in pts if x2 == x])) for x in xs]
        shift = 0.94 if v == "old" else 1.06  # side by side on a log axis
        ax.scatter([x * shift if log_x else x for x, _ in pts], [y for _, y in pts], s=10,
                   color=COLOR[v], alpha=0.55, marker="D" if v == "old" else "o", linewidths=0, zorder=3)
        ax.plot(xs, med, color=COLOR[v], linewidth=1.5, linestyle="--" if v == "old" else "-",
                label=v.upper(), zorder=2)
    if log_x:
        ax.set_xscale("log")


def write_sweep_figures(out: Path, trials: list[dict], plt) -> None:
    """One curve per version along each sweep."""
    import matplotlib.ticker  
    import matplotlib
    sweep = defaultdict(lambda: {"old": [], "new": []})
    for t in trials:
        if t["scenario"] in SWEEP_SCENARIOS and not t.get("warmup") and "error" not in t:
            group = (t["scenario"], t.get("workers_requested")) if t["scenario"] == "scale" else (t["scenario"],)
            sweep[group][t["version"]].append(t)

    workers = sorted(g[1] for g in sweep if g[0] == "scale" and g[1] is not None)
    if workers:
        fig, axes = plt.subplots(2, len(workers), figsize=(6.3, 4.2), squeeze=False, sharex=True)
        for col, w in enumerate(workers):
            by = sweep[("scale", w)]
            _sweep_series(axes[0][col], by, lambda t: t["workflows"], lambda t: t.get("throughput_wf_per_min"))
            _sweep_series(axes[1][col], by, lambda t: t["workflows"],
                          lambda t: float(np.percentile(t["latencies"], 95)) if t.get("latencies") else None)
            axes[0][col].set_title(f"{w} worker{'s' if w != 1 else ''}", fontsize=8)
            axes[1][col].set_xlabel("workflows submitted at once", fontsize=7)
            axes[0][col].set_ylim(bottom=0)
            axes[1][col].set_ylim(bottom=0)
        axes[0][0].set_ylabel("throughput [wf/min]")
        axes[1][0].set_ylabel("workflow latency P95 [s]")
        axes[0][0].legend(loc="upper left", fontsize=7)
        fig.tight_layout()
        fig.savefig(out / "fig_scaling.pdf")
        plt.close(fig)

    inline, data = sweep.get(("inline-size",)), sweep.get(("data-size",))
    if inline or data:
        fig, axes = plt.subplots(1, 2, figsize=(6.3, 2.5), squeeze=False)
        if inline:
            ax = axes[0][0]
            sizes = sorted({t["inline_bytes"] for v in VERSIONS for t in inline[v]})
            pos = {s: i for i, s in enumerate(sizes)}  # categorical: 0 means no input
            _sweep_series(ax, inline, lambda t: pos[t["inline_bytes"]], lambda t: t.get("e2e_seconds"), log_x=False)
            for v in VERSIONS:  # a refused submission has no latency: a cross on the axis
                refused = sorted({pos[t["inline_bytes"]] for t in inline[v] if t.get("refused")})
                if refused:
                    ax.scatter(refused, [0] * len(refused), marker="x", s=28, color=COLOR[v],
                               clip_on=False, zorder=4, label=f"{v.upper()} refused")
            ax.set_xticks(range(len(sizes)), [_bytes_label(s) for s in sizes], fontsize=6, rotation=30)
            ax.set_xlabel("inline input [decoded bytes]", fontsize=7)
            ax.set_ylabel("end-to-end latency [s]")
            ax.set_ylim(bottom=0)
            ax.legend(loc="upper left", fontsize=7)
        if data:
            ax = axes[0][1]
            _sweep_series(ax, data, lambda t: t["records"], lambda t: t.get("e2e_seconds"))
            ax.set_yscale("log")
            ax.yaxis.set_major_locator(matplotlib.ticker.LogLocator(subs=(1, 2, 5)))
            ax.yaxis.set_major_formatter(matplotlib.ticker.FormatStrFormatter("%g"))
            ax.yaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
            ax.set_xlabel("records generated", fontsize=7)
            ax.set_ylabel("end-to-end latency [s]")
        fig.tight_layout()
        fig.savefig(out / "fig_size.pdf")
        plt.close(fig)


def _bytes_label(n: int) -> str:
    if n == 0:
        return "none"
    for unit, size in (("MiB", 2**20), ("KiB", 2**10)):
        if n >= size:
            whole, rest = divmod(n, size)
            return f"{whole} {unit}" + (f"+{rest}" if rest else "")
    return f"{n} B"


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 1
    run = Path(sys.argv[1])
    out = run / "thesis"
    out.mkdir(parents=True, exist_ok=True)

    raw = load_trials(run)
    summary = {
        "trials": analyse_trials(raw),
        "overhead": analyse_overhead(raw),
        "dispersion": analyse_dispersion(raw),
        "pickup": analyse_pickup(raw),
        "sweeps": analyse_sweeps(raw),
        "costs": analyse_costs(raw),
        "equivalence": analyse_equivalence(raw),
        "t33": analyse_t33(run),
        "suites": analyse_suites(run),
    }
    env = run / "env.json"
    if env.exists():
        summary["env"] = json.loads(env.read_text())

    (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    write_tables(out, summary)
    write_figures(out, raw)
    print(json.dumps({k: v for k, v in summary.items() if k != "env"}, indent=2, default=str)[:6000])
    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
