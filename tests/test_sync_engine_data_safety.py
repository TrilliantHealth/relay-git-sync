"""Regression tests for the repository's data-safety invariants.

See CLAUDE.md "Data-Safety Invariants". The scenarios here reproduce the
mass-wipe incident class: unsynced docs served as empty, publication/reset
states misread as deletions, and destructive bursts applied without notice.
"""

import os
from unittest.mock import Mock

import git
import pytest
from pycrdt import Doc, Text

from models import OperationType, SyncOperation
from persistence import PersistenceManager
from relay_client import RelayClient
from s3rn import S3RemoteDocument, S3RemoteFolder
from sync_engine import EMPTY_CANVAS_CONTENT, SyncEngine, mass_change_threshold

RELAY_ID = "85a06712-af14-47bc-a859-e8106cc786e8"
FOLDER_ID = "3667fcda-755e-472b-abea-4b4fc96873a9"
DOC_ID = "615bae6b-9ca2-4d73-8b63-1d8145276125"


def unsynced_doc_update() -> bytes:
    """Guid registered on the server, no peer ever uploaded content."""
    return Doc().get_update()


def emptied_doc_update() -> bytes:
    """A doc that had content which a peer then deleted (history remains)."""
    doc = Doc()
    text = doc.get("contents", type=Text)
    text += "real content"
    del text[:]
    return doc.get_update()


def make_relay_client(update: bytes) -> RelayClient:
    client = RelayClient("https://relay.example", "TOKEN")
    client.dm = Mock()
    client.dm.get_doc_as_update.return_value = update
    return client


class FakePersistence:
    """Minimal persistence surface over a real directory tree."""

    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.filemeta_folders = {}
        self.document_hashes = {RELAY_ID: {}}
        self.local_file_state = {}
        self.deleted_paths = []
        self.written = []
        self.git_repos = {}
        self._unpushed_snapshot_refs = {}
        self._snapshot_push_attempts = {}

    def get_folder_path_with_prefix(self, relay_id, folder_uuid):
        return os.path.join(self.base_dir, relay_id, folder_uuid)

    PUSH_RETRY_INTERVAL_SECONDS = PersistenceManager.PUSH_RETRY_INTERVAL_SECONDS
    snapshot_before_deletion = PersistenceManager.snapshot_before_deletion
    _push_snapshot_refs = PersistenceManager._push_snapshot_refs
    _retry_snapshot_pushes = PersistenceManager._retry_snapshot_pushes
    _push_error_reason = staticmethod(PersistenceManager._push_error_reason)

    def _safe_git_operation(self, func):
        return func()

    def _sanitize_path(self, path, folder_path):
        return os.path.join(folder_path, path.lstrip("/"))

    def write_file_content(self, document_resource, path, content, file_hash=None):
        full_path = self._sanitize_path(path, self.get_folder_path_with_prefix(RELAY_ID, FOLDER_ID))
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(content or "")
        self.written.append(path)
        return full_path

    def delete_file(self, folder_resource, path):
        full_path = self._sanitize_path(path, self.get_folder_path_with_prefix(RELAY_ID, FOLDER_ID))
        os.remove(full_path)
        self.deleted_paths.append(path)
        return full_path

    def find_local_file_by_doc_id(self, relay_id, folder_uuid, doc_id):
        return None


@pytest.fixture
def engine(tmp_path):
    persistence = FakePersistence(str(tmp_path))
    relay_client = make_relay_client(unsynced_doc_update())
    return SyncEngine(str(tmp_path), relay_client, persistence)


def folder_dir(engine):
    path = engine.persistence_manager.get_folder_path_with_prefix(RELAY_ID, FOLDER_ID)
    os.makedirs(path, exist_ok=True)
    return path


def seed_files(engine, count, content="existing content"):
    base = folder_dir(engine)
    paths = []
    for i in range(count):
        name = f"note-{i:03d}.md"
        with open(os.path.join(base, name), "w") as f:
            f.write(content)
        paths.append("/" + name)
    return paths


def doc_filemeta(paths):
    return {
        path: {"id": f"{DOC_ID[:-3]}{i:03d}", "type": "markdown"} for i, path in enumerate(paths)
    }


def update_operation(path):
    return SyncOperation(
        type=OperationType.UPDATE,
        path=path,
        folder_resource=S3RemoteFolder(RELAY_ID, FOLDER_ID),
        document_resource=S3RemoteDocument(RELAY_ID, FOLDER_ID, DOC_ID),
        metadata={"id": DOC_ID, "type": "markdown"},
    )


class TestUnsyncedDocsDefer:
    def test_document_fetch_defers_on_empty_state_vector(self):
        client = make_relay_client(unsynced_doc_update())
        assert client.fetch_document_content(S3RemoteDocument(RELAY_ID, FOLDER_ID, DOC_ID)) is None

    def test_canvas_fetch_defers_on_empty_state_vector(self):
        from s3rn import S3RemoteCanvas

        client = make_relay_client(unsynced_doc_update())
        assert client.fetch_canvas_content(S3RemoteCanvas(RELAY_ID, FOLDER_ID, DOC_ID)) is None

    def test_document_structure_reports_unsynced(self):
        client = make_relay_client(unsynced_doc_update())
        _doc, parsed = client.get_document_structure(S3RemoteFolder(RELAY_ID, FOLDER_ID))
        assert parsed["type"] == "unsynced"

    def test_genuinely_emptied_doc_is_authoritative(self):
        client = make_relay_client(emptied_doc_update())
        content = client.fetch_document_content(S3RemoteDocument(RELAY_ID, FOLDER_ID, DOC_ID))
        assert content == ""

    def test_unsynced_fetch_skips_update_without_touching_file(self, engine):
        base = folder_dir(engine)
        with open(os.path.join(base, "note.md"), "w") as f:
            f.write("real content")

        engine.handle_server_update(RELAY_ID, update_operation("/note.md"))

        with open(os.path.join(base, "note.md")) as f:
            assert f.read() == "real content"


class TestPublicationRule:
    def test_empty_filemeta_with_local_files_refuses_sync(self, engine):
        seed_files(engine, 5)

        operations = engine.apply_remote_folder_changes(
            RELAY_ID, S3RemoteFolder(RELAY_ID, FOLDER_ID), old_filemeta={}, new_filemeta={}
        )

        assert operations == []
        assert engine.persistence_manager.deleted_paths == []
        assert len(os.listdir(folder_dir(engine))) == 5


class TestDeletionBurst:
    def test_burst_above_threshold_applies_and_alerts(self, engine, caplog):
        seed_files(engine, 100)
        kept = doc_filemeta(["/note-000.md", "/note-001.md"])
        engine.relay_client = make_relay_client(emptied_doc_update())

        engine.apply_remote_folder_changes(
            RELAY_ID, S3RemoteFolder(RELAY_ID, FOLDER_ID), old_filemeta={}, new_filemeta=kept
        )

        assert len(engine.persistence_manager.deleted_paths) == 98
        alerts = [r for r in caplog.records if "Mass deletion applied" in r.getMessage()]
        assert len(alerts) == 1
        assert "98 deletions exceed threshold 25" in alerts[0].getMessage()

    def test_burst_snapshot_keeps_uncommitted_content(self, engine, caplog):
        base = folder_dir(engine)
        repo = git.Repo.init(base)
        with open(os.path.join(base, "note-099.md"), "w") as f:
            f.write("committed content")
        repo.index.add(["note-099.md"])
        branch_head = repo.index.commit("initial").hexsha
        seed_files(engine, 100, content="uncommitted content")
        engine.persistence_manager.git_repos[f"{RELAY_ID}/{FOLDER_ID}"] = repo
        kept = doc_filemeta(["/note-000.md", "/note-001.md"])
        engine.relay_client = make_relay_client(emptied_doc_update())

        engine.apply_remote_folder_changes(
            RELAY_ID, S3RemoteFolder(RELAY_ID, FOLDER_ID), old_filemeta={}, new_filemeta=kept
        )

        assert len(engine.persistence_manager.deleted_paths) == 98
        assert repo.head.commit.hexsha == branch_head
        (ref,) = [r for r in repo.refs if r.path.startswith("refs/mass-deletions/")]
        alert = next(r.getMessage() for r in caplog.records if "Mass deletion" in r.getMessage())
        assert ref.path in alert
        for name in ("note-050.md", "note-099.md"):
            assert repo.git.show(f"{ref.path}:{name}") == "uncommitted content"

    def test_rejected_snapshot_push_is_retried(self, engine, caplog, tmp_path):
        remote = git.Repo.init(tmp_path / "remote.git", bare=True)
        hook = tmp_path / "remote.git" / "hooks" / "pre-receive"
        hook.write_text("#!/bin/sh\nexit 1\n")
        hook.chmod(0o755)
        base = folder_dir(engine)
        repo = git.Repo.init(base)
        repo.create_remote("origin", str(tmp_path / "remote.git"))
        seed_files(engine, 100, content="uncommitted content")
        engine.persistence_manager.git_repos[f"{RELAY_ID}/{FOLDER_ID}"] = repo
        kept = doc_filemeta(["/note-000.md", "/note-001.md"])
        engine.relay_client = make_relay_client(emptied_doc_update())

        engine.apply_remote_folder_changes(
            RELAY_ID, S3RemoteFolder(RELAY_ID, FOLDER_ID), old_filemeta={}, new_filemeta=kept
        )

        alert = next(r.getMessage() for r in caplog.records if "Mass deletion" in r.getMessage())
        assert "NOT yet pushed" in alert
        assert not [r for r in remote.refs if r.path.startswith("refs/mass-deletions/")]

        hook.unlink()
        engine.persistence_manager._snapshot_push_attempts.clear()
        engine.persistence_manager._retry_snapshot_pushes(f"{RELAY_ID}/{FOLDER_ID}", repo)

        (ref,) = [r for r in remote.refs if r.path.startswith("refs/mass-deletions/")]
        assert remote.git.show(f"{ref.path}:note-050.md") == "uncommitted content"
        assert engine.persistence_manager._unpushed_snapshot_refs == {}

    def test_burst_below_threshold_applies_quietly(self, engine, caplog):
        paths = seed_files(engine, 30)
        kept = doc_filemeta(paths[:28])
        engine.relay_client = make_relay_client(emptied_doc_update())

        engine.apply_remote_folder_changes(
            RELAY_ID, S3RemoteFolder(RELAY_ID, FOLDER_ID), old_filemeta={}, new_filemeta=kept
        )

        assert sorted(engine.persistence_manager.deleted_paths) == [
            "note-028.md",
            "note-029.md",
        ]
        assert not any("Mass deletion" in r.getMessage() for r in caplog.records)


class TestTruncationBurstGate:
    def test_mass_truncation_is_capped_per_pass(self, engine):
        paths = seed_files(engine, 40)
        engine.relay_client = make_relay_client(emptied_doc_update())

        engine.apply_remote_folder_changes(
            RELAY_ID,
            S3RemoteFolder(RELAY_ID, FOLDER_ID),
            old_filemeta={},
            new_filemeta=doc_filemeta(paths),
        )

        base = folder_dir(engine)
        emptied = sum(1 for n in os.listdir(base) if os.path.getsize(os.path.join(base, n)) == 0)
        # Threshold for membership 40 is max(4, 25) = 25: the rest survive.
        assert emptied == mass_change_threshold(40) == 25
        assert 40 - emptied == 15

    def test_single_truncation_outside_folder_pass_is_allowed(self, engine):
        base = folder_dir(engine)
        with open(os.path.join(base, "note.md"), "w") as f:
            f.write("real content")
        engine.relay_client = make_relay_client(emptied_doc_update())

        engine.handle_server_update(RELAY_ID, update_operation("/note.md"))

        with open(os.path.join(base, "note.md")) as f:
            assert f.read() == ""

    def test_document_event_burst_is_capped_across_individual_updates(self, engine):
        paths = seed_files(engine, 40)
        engine.persistence_manager.filemeta_folders = {RELAY_ID: {FOLDER_ID: doc_filemeta(paths)}}
        engine.relay_client = make_relay_client(emptied_doc_update())

        for path in paths:
            engine.handle_server_update(RELAY_ID, update_operation(path))

        base = folder_dir(engine)
        emptied = sum(
            1 for name in os.listdir(base) if os.path.getsize(os.path.join(base, name)) == 0
        )
        assert emptied == mass_change_threshold(40) == 25
        assert 40 - emptied == 15

    def test_empty_canvas_constant_matches_fetch_output(self):
        doc = Doc()
        from pycrdt import Map

        doc.get("edges", type=Map)
        doc.get("nodes", type=Map)
        client = make_relay_client(b"")
        assert client._export_canvas_data(doc) == {"edges": [], "nodes": []}
        import json

        assert EMPTY_CANVAS_CONTENT == json.dumps(
            {"edges": [], "nodes": []}, indent=2, sort_keys=True
        )
