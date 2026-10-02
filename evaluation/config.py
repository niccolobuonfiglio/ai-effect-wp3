"""Fixed settings of the OLD vs NEW evaluation.

Everything that must be identical for both versions lives here, so that the
only thing that changes between a pair of trials is the orchestrator code.
Paths can be overridden with environment variables (see PROTOCOL.md).
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent
NEW_ROOT = Path(os.environ.get("EVAL_NEW_ROOT", HERE.parent))
OLD_ROOT = Path(
    os.environ.get("EVAL_OLD_ROOT", NEW_ROOT.parent / "oldCode" / "ai-effect-wp3")
)

# The frozen baseline pinned by the German and Portugal Task 3.3 suites.
OLD_COMMIT = "486ccc702bb2afca79fb7d31258d62ee0f94dbfc"

API = os.environ.get("ORCHESTRATOR_URL", "http://localhost:18000").rstrip("/")
API_KEY = os.environ.get("ORCHESTRATOR_API_KEY") or None


@dataclass(frozen=True)
class Version:
    name: str
    root: Path
    project: str  # docker compose project name

    @property
    def compose_file(self) -> Path:
        return self.root / "orchestrator" / "docker-compose.yml"


VERSIONS = {
    # Both folders are called "orchestrator", so without distinct project
    # names the two stacks would overwrite each other's containers and images.
    "old": Version("old", OLD_ROOT, "orch-old"),
    "new": Version("new", NEW_ROOT, "orchestrator"),
}


@dataclass(frozen=True)
class UseCase:
    name: str
    folder: Path
    # orchestrator container name -> (compose service, host health URL or None)
    services: dict[str, tuple[str, str | None]]
    # the service taken down in outage scenarios
    outage_target: str
    # a task that runs long enough to be interrupted mid-flight (V-A2)
    long_task: str | None = None
    # compose services recreated before every trial (use-case defects, e.g. F2)
    recreate_before_trial: tuple[str, ...] = field(default_factory=tuple)
    # compose services recreated before every recovery action of the operator
    # (OLD resubmission, NEW /recover); the F2 rule of PROTOCOL.md 9.3
    recreate_before_recovery: tuple[str, ...] = field(default_factory=tuple)
    # can several workflows run at the same time?
    concurrent: bool = True
    # Output-equivalence oracle (PROTOCOL.md, Amendment 3): file -> sha256 of
    # the output every completed job must reproduce, under the folder the
    # services write each workflow's files to. Empty: no oracle.
    output_reference: dict[str, str] = field(default_factory=dict)

    @property
    def compose_file(self) -> Path:
        return self.folder / "docker-compose.yml"

    def compose_name(self, service: str) -> str:
        return self.services[service][0]

    @property
    def data_dir(self) -> Path:
        """Host folder mounted as the services' ``data/``."""
        return self.folder / "data"


USE_CASES = {
    "file_based": UseCase(
        name="file_based",
        folder=NEW_ROOT / "use-cases" / "file_based_energy_pipeline",
        services={
            "input_provider1": ("input-provider", "http://localhost:18081/health"),
            "data_generator1": ("data-generator", "http://localhost:18082/health"),
            "data_analyzer1": ("data-analyzer", "http://localhost:18083/health"),
            "report_generator1": ("report-generator", "http://localhost:18084/health"),
        },
        outage_target="data_analyzer1",
        # Fixed on 2026-10-03, before the measurements: the 1 562 workflows on
        # disk that produced a report (2026-09-26 to 09-30, both versions,
        # earlier campaigns and tests) all have exactly these three files.
        output_reference={
            "raw_energy.csv": "sha256:3e58cf09a96f1149f136d3d090bc546da89dfb716cf57e3a1d52530b5ef2e62b",
            "analyzed_energy.csv": "sha256:030cd7c9c3c47b0e1a8fca67ed597789d0a132df837f2e547eedd11a0ff69a21",
            "energy_report.csv": "sha256:0f9f46392747e7dc5aa8f85797fe3c4bbf7f6315ca1791e746476cc4e519307d",
        },
    ),
    "germany": UseCase(
        name="germany",
        folder=NEW_ROOT / "use-cases" / "germany-node",
        services={
            "data_provider1": ("data-provider", "http://localhost:18091/health"),
            "villas_chronics1": ("villas-chronics", "http://localhost:18092/health"),
            "output_formatter1": ("output-formatter", "http://localhost:18093/health"),
        },
        outage_target="output_formatter1",
        long_task="villas_chronics1",
        # villas-node segfaults on its second restart (RUN_ANALYSIS F2)
        recreate_before_trial=("villas-node",),
        recreate_before_recovery=("villas-node",),
        concurrent=False,
    ),
    # Services pass data over gRPC from in-memory caches: a restart erases a
    # service's output, and concurrent workflows would overwrite each other's
    # data (PROTOCOL.md 9.1). The target has not produced its output yet when
    # it goes down, so the resume does not depend on a lost cache.
    "protobuf": UseCase(
        name="protobuf",
        folder=NEW_ROOT / "use-cases" / "protobuf_based_energy_pipeline",
        services={
            "input_provider1": ("pb-input-provider", "http://localhost:18181/health"),
            "data_generator1": ("pb-data-generator", "http://localhost:18182/health"),
            "data_analyzer1": ("pb-data-analyzer", "http://localhost:18183/health"),
            "report_generator1": ("pb-report-generator", "http://localhost:18184/health"),
        },
        outage_target="data_analyzer1",
        concurrent=False,
    ),
}
