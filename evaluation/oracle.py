"""Output-equivalence oracle (PROTOCOL.md, Amendment 3).

A job that completes under a fault must produce the same output as a job that
completes without one. For a deterministic use case, "the same" can be checked
without judging the data: every output file must be byte-identical to a
reference fixed before the measurements.

The oracle reads the files the services wrote on the host, never the copy the
orchestrator was told about, so it does not depend on either version's API.
"""

import hashlib
import re
from pathlib import Path

EQUIVALENT, DIVERGENT, MISSING = "equivalent", "divergent", "missing"


def sha256_file(path: Path) -> str | None:
    """``sha256:<hex>`` of a file, or None if it does not exist."""
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 16), b""):
            digest.update(block)
    return "sha256:" + digest.hexdigest()


def workflow_dir(data_dir: Path, workflow_id: str) -> Path:
    """Where the services write a workflow's files (same rule as their _output_dir)."""
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", workflow_id or "") or "unknown"
    return data_dir / safe


def check_outputs(data_dir: Path, workflow_id: str, reference: dict[str, str]) -> dict:
    """Compare one workflow's output files with the reference.

    Verdict: ``missing`` if any expected file is absent, else ``divergent`` if
    any differs from its reference, else ``equivalent``.
    """
    folder = workflow_dir(data_dir, workflow_id)
    files = {name: sha256_file(folder / name) for name in reference}
    if any(h is None for h in files.values()):
        verdict = MISSING
    elif any(files[name] != expected for name, expected in reference.items()):
        verdict = DIVERGENT
    else:
        verdict = EQUIVALENT
    return {
        "workflow_id": workflow_id,
        "verdict": verdict,
        "differing": sorted(n for n, e in reference.items() if files[n] != e),
        "files": files,
    }
