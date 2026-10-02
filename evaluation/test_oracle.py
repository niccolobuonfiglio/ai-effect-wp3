"""Checks of the output-equivalence oracle itself (PROTOCOL.md, Amendment 3).

The instrument must say "divergent" when one byte changes and "missing" when a
file is absent, or a run where every job is "equivalent" proves nothing.
Run with any pytest: ``python -m pytest evaluation/test_oracle.py``.
"""

import hashlib

from oracle import DIVERGENT, EQUIVALENT, MISSING, check_outputs, sha256_file, workflow_dir


def make(tmp_path, wf, files):
    folder = workflow_dir(tmp_path, wf)
    folder.mkdir(parents=True)
    for name, content in files.items():
        (folder / name).write_bytes(content)
    return {name: "sha256:" + hashlib.sha256(c).hexdigest() for name, c in files.items()}


FILES = {"raw.csv": b"a,b\n1,2\n", "report.csv": b"metric,value\nx,1\n"}


def test_identical_files_are_equivalent(tmp_path):
    reference = make(tmp_path, "wf-1", FILES)
    result = check_outputs(tmp_path, "wf-1", reference)
    assert result["verdict"] == EQUIVALENT
    assert result["differing"] == []


def test_one_changed_byte_is_divergent(tmp_path):
    reference = make(tmp_path, "wf-1", FILES)
    (workflow_dir(tmp_path, "wf-1") / "report.csv").write_bytes(b"metric,value\nx,2\n")
    result = check_outputs(tmp_path, "wf-1", reference)
    assert result["verdict"] == DIVERGENT
    assert result["differing"] == ["report.csv"]


def test_an_absent_file_is_missing(tmp_path):
    reference = make(tmp_path, "wf-1", FILES)
    (workflow_dir(tmp_path, "wf-1") / "raw.csv").unlink()
    result = check_outputs(tmp_path, "wf-1", reference)
    assert result["verdict"] == MISSING
    assert result["files"]["raw.csv"] is None


def test_an_absent_workflow_folder_is_missing(tmp_path):
    reference = make(tmp_path, "wf-1", FILES)
    assert check_outputs(tmp_path, "wf-2", reference)["verdict"] == MISSING


def test_the_folder_name_follows_the_services_rule(tmp_path):
    assert workflow_dir(tmp_path, "wf/../x").name == "wf_.._x"
    assert workflow_dir(tmp_path, "").name == "unknown"


def test_hash_format(tmp_path):
    (tmp_path / "f").write_bytes(b"")
    assert sha256_file(tmp_path / "f") == "sha256:" + hashlib.sha256(b"").hexdigest()
    assert sha256_file(tmp_path / "absent") is None
