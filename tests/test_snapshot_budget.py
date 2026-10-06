"""Storage refusal preserves accepted reads, receipts and pins across retry/restart."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import snapshot_budget
from snapshot_budget import StorageBudget, StorageRefused, allocated_usage, prospective_usage
from snapshot_export import SnapshotExporter
from tests.test_snapshot_export import FOLDER, RELAY, FakeClient


def filesystem(bytes_available=10**9, inodes=10**6):
    return SimpleNamespace(
        f_frsize=4096, f_bsize=4096, f_bavail=bytes_available // 4096, f_favail=inodes
    )


def mirror(client, root, budget):
    return SnapshotExporter(
        client, RELAY, [{"id": FOLDER, "prefix": "eng-relay"}], root, budget=budget
    )


def preserve(root):
    return {
        str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()
    }


@pytest.mark.parametrize("reserve", ["bytes", "inodes"])
def test_reserve_refusal_preserves_last_good_and_retry_after_restart(
    tmp_path, monkeypatch, reserve
):
    budget = StorageBudget(10**8, 2**20, 32)
    client = FakeClient()
    first = mirror(client, tmp_path, budget).export()
    (tmp_path / "served.json").write_text(json.dumps(first))
    (tmp_path / "pins").mkdir()
    (tmp_path / "pins" / first["revision"]).touch()
    before = preserve(tmp_path)
    client.body = "# Subsequent content"
    unavailable = (
        filesystem(bytes_available=2**20 - 1) if reserve == "bytes" else filesystem(inodes=31)
    )
    monkeypatch.setattr(snapshot_budget.os, "statvfs", lambda _root: unavailable)
    with pytest.raises(StorageRefused, match="reserve"):
        mirror(client, tmp_path, budget).export()
    assert preserve(tmp_path) == before
    monkeypatch.setattr(snapshot_budget.os, "statvfs", lambda _root: filesystem())
    next_receipt = mirror(client, tmp_path, budget).export()
    assert next_receipt != first
    assert Path(first["content_dir"]).joinpath("eng-relay/note.md").read_text() == "# First"
    assert (tmp_path / "pins" / first["revision"]).exists()


def test_budget_counts_pinned_history_and_prospective_generation_not_just_file_count(tmp_path):
    client = FakeClient()
    roomy = StorageBudget(10**8, 2**20, 32)
    first = mirror(client, tmp_path, roomy).export()
    (tmp_path / "pins").mkdir()
    (tmp_path / "pins" / first["revision"]).touch()
    unrelated_history = tmp_path / "generations" / ("b" * 64)
    unrelated_history.mkdir()
    unrelated_history.joinpath("previous.html").write_bytes(b"x" * 2**20)
    before = preserve(tmp_path)
    client.body = "x" * 2**20
    measured = allocated_usage(tmp_path)
    budget = StorageBudget(measured + 2**20 + 4096, 2**20, 32)
    with pytest.raises(StorageRefused, match="allocation budget"):
        mirror(client, tmp_path, budget).export()
    assert preserve(tmp_path) == before
    assert not list(tmp_path.glob("staging-*"))
    assert mirror(client, tmp_path, roomy).export() != first


def test_mid_stage_free_space_loss_cleans_attempt_and_preserves_receipts(tmp_path, monkeypatch):
    client = FakeClient()
    budget = StorageBudget(10**8, 2**20, 32)
    first = mirror(client, tmp_path, budget).export()
    before = preserve(tmp_path)
    client.body = "# A different snapshot"
    original_open = Path.open
    failed = False

    def open_file(path, *args, **kwargs):
        nonlocal failed
        handle = original_open(path, *args, **kwargs)
        if "staging-" in str(path) and args == ("wb",):
            failed = True
        return handle

    monkeypatch.setattr(Path, "open", open_file)
    monkeypatch.setattr(
        snapshot_budget.os, "statvfs", lambda _root: filesystem(0) if failed else filesystem()
    )
    with pytest.raises(StorageRefused):
        mirror(client, tmp_path, budget).export()
    assert preserve(tmp_path) == before
    assert not list(tmp_path.glob("staging-*"))
    assert Path(first["content_dir"]).exists()


def test_prospective_budget_reserves_dense_blocks_even_for_sparse_input(tmp_path):
    path = tmp_path / "sparse"
    with path.open("wb") as handle:
        handle.truncate(2**20)
    estimated, inodes = prospective_usage({"deep/folder/sparse": path.read_bytes()}, 8193, 4096)
    assert estimated >= 2**20 + 4 * 12288 + 4 * 4096
    assert inodes >= 9
    assert allocated_usage(tmp_path) < estimated


def test_symlink_accounting_refuses_and_does_not_follow_other_storage(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    other = tmp_path / "other"
    other.write_text("private fixture")
    (root / "outside").symlink_to(other)
    with pytest.raises(StorageRefused, match="symlink"):
        StorageBudget(10**8, 2**20, 32).admit(root, {})
    assert other.read_text() == "private fixture"


@pytest.mark.parametrize("values", [(0, 1, 1), (10, 10, 1), (10, 1, 0), (10, 0, 1)])
def test_nonpositive_or_unusable_budget_is_rejected(values):
    with pytest.raises(ValueError):
        StorageBudget(*values)
