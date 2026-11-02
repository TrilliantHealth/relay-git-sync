import hashlib
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

from models import OperationType, SyncOperation, SyncRequest
from s3rn import S3RemoteDocument, S3RemoteFolder
from sync_engine import SyncEngine


def make_engine():
    relay_client = Mock()
    persistence = Mock()
    persistence.filemeta_folders = {}
    persistence.load_persistent_data.return_value = None
    persistence.save_persistent_data.return_value = None
    persistence.init_git_repo.return_value = None
    return SyncEngine("/tmp/git-sync-test", relay_client, persistence)


def test_process_document_change_skips_unconfigured_folder_event():
    engine = make_engine()
    engine.persistence_manager.filemeta_folders = {"relay": {"stale-folder": {}}}
    engine.persistence_manager.should_sync_folder.return_value = False

    result = engine.process_document_change("relay", "stale-folder", datetime.now(timezone.utc))

    assert result.success is True
    assert result.operations == []
    engine.relay_client.get_doc_object.assert_not_called()
    engine.persistence_manager.init_git_repo.assert_not_called()


def test_process_sync_request_skips_unconfigured_folder():
    engine = make_engine()
    engine.relay_client.get_document_structure.return_value = (
        {"filemeta_v0": object()},
        {"type": "folder", "filemeta": {}},
    )
    engine.persistence_manager.should_sync_folder.return_value = False
    folder = S3RemoteFolder("relay", "stale-folder")

    result = engine.process_sync_request(
        SyncRequest(resource=folder, timestamp=datetime.now(timezone.utc))
    )

    assert result.success is True
    assert result.operations == []
    engine.relay_client.get_document_structure.assert_not_called()
    engine.persistence_manager.init_git_repo.assert_not_called()


def test_sync_relay_all_folders_uses_configured_connectors():
    engine = make_engine()
    engine.persistence_manager.git_config.get_connectors_for_relay.return_value = [
        SimpleNamespace(shared_folder_id="configured-folder")
    ]
    engine.process_sync_request = Mock(return_value=object())

    results = engine.sync_relay_all_folders("relay")

    assert results == [engine.process_sync_request.return_value]
    request = engine.process_sync_request.call_args.args[0]
    assert request.resource.relay_id == "relay"
    assert request.resource.folder_id == "configured-folder"


def test_refused_folder_snapshot_preserves_prior_filemeta():
    engine = make_engine()
    old_filemeta = {"/existing.md": {"id": "existing-doc", "type": "markdown"}}
    engine.persistence_manager.filemeta_folders = {"relay": {"folder": old_filemeta}}
    engine.persistence_manager.should_sync_folder.return_value = True
    engine.relay_client.get_document_structure.return_value = (
        {"filemeta_v0": object()},
        {"type": "folder", "filemeta": {}},
    )
    engine._apply_remote_folder_changes = Mock(return_value=([], False))

    result = engine.process_sync_request(
        SyncRequest(
            resource=S3RemoteFolder("relay", "folder"),
            timestamp=datetime.now(timezone.utc),
        )
    )

    assert result.success is True
    assert engine.persistence_manager.filemeta_folders["relay"]["folder"] == old_filemeta
    engine.persistence_manager._build_resource_index.assert_called_once_with("relay")


def test_gated_document_update_does_not_advance_hash():
    engine = make_engine()
    document = S3RemoteDocument("relay", "folder", "document")
    engine.persistence_manager.filemeta_folders = {"relay": {"folder": {}}}
    engine.persistence_manager.document_hashes = {"relay": {}}
    engine.persistence_manager.lookup_resource.return_value = document
    engine.persistence_manager.should_sync_folder.return_value = True
    engine.relay_client.fetch_document_content.return_value = ""
    gated_operation = SyncOperation(
        type=OperationType.UPDATE,
        path="/existing.md",
        folder_resource=S3RemoteFolder("relay", "folder"),
        document_resource=document,
        error="Truncation burst gated",
    )
    engine.handle_document_update = Mock(return_value=gated_operation)

    result = engine.process_document_change("relay", "document", datetime.now(timezone.utc))

    assert result.success is True
    assert "document" not in engine.persistence_manager.document_hashes["relay"]


def test_folder_event_only_processes_changed_filemeta_entries():
    engine = make_engine()
    unchanged = {"id": "existing-doc", "type": "markdown"}
    renamed = {"id": "renamed-doc", "type": "markdown"}
    old_filemeta = {
        "/unchanged.md": unchanged,
        "/old-name.md": renamed,
    }
    new_filemeta = {
        "/unchanged.md": dict(unchanged),
        "/new-name.md": dict(renamed),
        "/created.md": {"id": "created-doc", "type": "markdown"},
    }

    changed = engine._changed_filemeta_entries(old_filemeta, new_filemeta)

    assert changed == {
        "/new-name.md": renamed,
        "/created.md": {"id": "created-doc", "type": "markdown"},
    }


def test_initial_subdoc_head_is_persisted_without_refetch_after_startup_sync():
    engine = make_engine()
    document = S3RemoteDocument("relay", "folder", "document")
    engine.persistence_manager.filemeta_folders = {"relay": {"folder": {}}}
    engine.persistence_manager.document_hashes = {"relay": {}}
    engine.persistence_manager.subdoc_heads = {"relay": {}}
    engine.persistence_manager.lookup_resource.return_value = document
    engine.persistence_manager.should_sync_folder.return_value = True
    snapshot = b"initial-subdoc-head"

    result = engine.process_document_change(
        "relay",
        "document",
        datetime.now(timezone.utc),
        subdoc_snapshot=snapshot,
        baseline_only=True,
    )

    assert result.success is True
    assert (
        engine.persistence_manager.subdoc_heads["relay"]["document"]
        == hashlib.sha256(snapshot).hexdigest()
    )
    engine.relay_client.fetch_document_content.assert_not_called()


def test_changed_subdoc_head_with_unchanged_content_avoids_file_write():
    engine = make_engine()
    document = S3RemoteDocument("relay", "folder", "document")
    content = "same content"
    engine.persistence_manager.filemeta_folders = {"relay": {"folder": {}}}
    engine.persistence_manager.document_hashes = {
        "relay": {"document": hashlib.sha256(content.encode()).hexdigest()}
    }
    engine.persistence_manager.subdoc_heads = {"relay": {"document": "old-head"}}
    engine.persistence_manager.lookup_resource.return_value = document
    engine.persistence_manager.should_sync_folder.return_value = True
    engine.relay_client.fetch_document_content.return_value = content
    engine.handle_document_update = Mock()
    snapshot = b"new-subdoc-head"

    result = engine.process_document_change(
        "relay",
        "document",
        datetime.now(timezone.utc),
        subdoc_snapshot=snapshot,
    )

    assert result.success is True
    assert result.operations == []
    assert (
        engine.persistence_manager.subdoc_heads["relay"]["document"]
        == hashlib.sha256(snapshot).hexdigest()
    )
    engine.handle_document_update.assert_not_called()
