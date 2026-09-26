#!/usr/bin/env python3
"""Turn the raw evaluation outputs into thesis tables, figures and tests.

    python evaluation/analyze.py evaluation/results/<run-id>

Reads whatever exists in the run folder:
    trials.jsonl            runner.py         (layer 3: fault injection, overhead)
    t33.csv                 run_t33.sh        (layer 1: independent T3.3 suites)
    suites/<cell>/...       run_own_suites.sh (layer 2: own suites, differential)

Writes <run>/thesis/: LaTeX tables (booktabs), PDF figures and summary.json
with every number that appears in a table.

Statistics (see PROTOCOL.md, "Analysis"):
  binary outcomes     Fisher's exact test, two-sided
  continuous          Mann-Whitney U (two-sided), Cliff's delta,
                      95% bootstrap CI of the difference in medians
  overhead (nominal)  TOST equivalence on log latency, margin +/-5 %; plus the
                      attribution arm (NEW with its submission checks off,
                      PROTOCOL.md 8.4)
Metrics that OLD cannot produce at all are reported as "n/s" (not supported),
never as 0.
"""

import csv
import json
import math
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

import numpy as np
from scipy import stats

RNG = np.random.default_rng(20260925)
MARGIN = 0.05  # equivalence margin for overhead
VERSIONS = ("old", "new")
# Reference palette slots 1 and 2 (validated adjacent pair); OLD also hatched,
# so the two versions stay distinguishable in greyscale print.
COLOR = {"old": "#eb6834", "new": "#2a78d6"}
HATCH = {"old": "////", "new": ""}
INK, MUTED, GRID = "#1f1f1e", "#6b6a64", "#e4e3dd"


# ----------------------------------------------------------------------
# statistics
# ----------------------------------------------------------------------
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


# ----------------------------------------------------------------------
# layer 3: trials
# ----------------------------------------------------------------------
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
        # checks-off arm is analysed separately (analyse_overhead).
        if not t.get("warmup") and t.get("variant", "default") == "default":
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
        if any("refused" in t for t in valid["old"] + valid["new"]):  # RQ8
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
        if scenario == "nominal":
            r["tost_e2e"] = tost_log(values(valid["old"], "e2e_seconds"), values(valid["new"], "e2e_seconds"))
        result[f"{scenario}/{uc}"] = r
    return result


def analyse_overhead(trials: list[dict]) -> dict:
    """RQ6 with attribution (PROTOCOL.md 8.4): what do NEW's submission checks cost?"""
    def e2e(version, variant):
        return [t["e2e_seconds"] for t in trials
                if t["scenario"] == "nominal" and not t.get("warmup") and "error" not in t
                and t["version"] == version and t.get("variant", "default") == variant
                and t.get("e2e_seconds") is not None]

    old, new, off = e2e("old", "default"), e2e("new", "default"), e2e("new", "checks-off")
    if not off:
        return {}
    cost = compare_continuous(off, new)  # "old" slot = checks off, "new" slot = default
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


# ----------------------------------------------------------------------
# layer 1: Task 3.3
# ----------------------------------------------------------------------
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


# ----------------------------------------------------------------------
# layer 2: own suites
# ----------------------------------------------------------------------
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
            if cell.name.startswith("diff-"):  # per-test outcome, for the RQ4/RQ8 table
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


# ----------------------------------------------------------------------
# output: LaTeX
# ----------------------------------------------------------------------
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


# ----------------------------------------------------------------------
# output: figures
# ----------------------------------------------------------------------
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
        if not t.get("warmup") and "error" not in t:
            by[f"{t['scenario']}\n{t['use_case']}"][t["version"]].append(t)
    names = sorted(by)

    # 1. completion rate per scenario (magnitude -> bars, shared zero baseline)
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

    # 2. distributions: recovery time and nominal latency (strip + median tick)
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


# ----------------------------------------------------------------------
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
