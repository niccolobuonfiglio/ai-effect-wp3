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

    @property
    def compose_file(self) -> Path:
        return self.folder / "docker-compose.yml"

    def compose_name(self, service: str) -> str:
        return self.services[service][0]


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
