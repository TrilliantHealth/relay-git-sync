import hashlib
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import urlparse

import pytest
from pycrdt import Doc, Map, Text

from relay_client import RelayClient
from snapshot_export import DeferredExport, SnapshotExporter, native_path

RELAY = "11111111-1111-1111-1111-111111111111"
FOLDER = "22222222-2222-2222-2222-222222222222"
NOTE = "33333333-3333-3333-3333-333333333333"
FILE = "44444444-4444-4444-4444-444444444444"
CANVAS = "55555555-5555-5555-5555-555555555555"


class CanvasSDK:
    def __init__(self, canvas):
        self.canvas = canvas
        self.canvas_reads = 0

    def get_doc_as_update(self, identifier):
        if identifier.endswith(FOLDER):
            return Doc(
                {"filemeta_v0": Map({"/diagram.canvas": {"id": CANVAS, "type": "canvas"}})}
            ).get_update()
        assert identifier.endswith(CANVAS)
        self.canvas_reads += 1
        return self.canvas.get_update()


def canvas_client(canvas):
    client = RelayClient("http://127.0.0.1:1")
    client.dm = CanvasSDK(canvas)
    return client


def test_foreign_canvas_schema_preserves_last_good_and_reconciles(tmp_path):
    client = canvas_client(Doc({"nodes": Map({"n": {"id": "n", "type": "group"}})}))
    mirror = exporter(client, tmp_path)
    first = mirror.export()
    receipt = (tmp_path / "current.json").read_bytes()
    state = (tmp_path / "state.json").read_bytes()
    first_body = tmp_path / "generations" / first["revision"] / "eng-relay/diagram.canvas"
    accepted = first_body.read_bytes()
    for invalid in (
        Doc({"contents": Text("foreign Markdown")}),
        Doc({"relay": Map({"v": 0}), "contents": Text("foreign Markdown")}),
        Doc({"filemeta_v0": Map({"x": {"id": NOTE}})}),
        Doc({"unrelated": Map({"value": 1})}),
        Doc(),
    ):
        client.dm.canvas = invalid
        with pytest.raises(DeferredExport):
            mirror.export()
        assert (tmp_path / "current.json").read_bytes() == receipt
        assert (tmp_path / "state.json").read_bytes() == state
        assert first_body.read_bytes() == accepted
    client.dm.canvas = Doc({"relay": Map({"v": 0})})
    recovered = mirror.export()
    assert recovered["revision"] != first["revision"]
    assert json.loads(
        (tmp_path / "generations" / recovered["revision"] / "eng-relay/diagram.canvas").read_text()
    ) == {"nodes": [], "edges": []}


@pytest.mark.parametrize("shape", ["header", "nodes", "edges", "deleted-nodes", "deleted-edges"])
def test_canvas_enrollment_sparse_maps_and_deleted_history_are_authoritative(tmp_path, shape):
    if shape == "header":
        canvas = Doc({"relay": Map({"v": 0})})
    else:
        key = "nodes" if "nodes" in shape else "edges"
        canvas = Doc({key: Map({"n": {"id": "n", "type": "group"}})})
        if shape.startswith("deleted"):
            del canvas.get(key, type=Map)["n"]
    client = canvas_client(canvas)
    receipt = exporter(client, tmp_path).export()
    result = json.loads(
        (tmp_path / "generations" / receipt["revision"] / "eng-relay/diagram.canvas").read_text()
    )
    expected = {"nodes": [], "edges": []}
    if shape in ("nodes", "edges"):
        expected[shape] = [{"id": "n", "type": "group"}]
    assert result == expected
    assert client.dm.canvas_reads == 1, "validation and decoding must use the same fetched document"


@pytest.mark.parametrize("node_id", ["contents", "filemeta_v0", "arbitrary-text-root"])
@pytest.mark.parametrize("deleted", [False, True])
def test_canvas_text_node_root_names_and_tombstones_are_not_foreign_schema(
    tmp_path, node_id, deleted
):
    canvas = Doc(
        {
            "relay": Map({"v": 0}),
            "nodes": Map({node_id: {"id": node_id, "type": "text"}}),
            node_id: Text("legitimate canvas text"),
        }
    )
    if deleted:
        del canvas.get("nodes", type=Map)[node_id]
    client = canvas_client(canvas)
    receipt = exporter(client, tmp_path).export()
    result = json.loads(
        (tmp_path / "generations" / receipt["revision"] / "eng-relay/diagram.canvas").read_text()
    )
    expected = (
        [] if deleted else [{"id": node_id, "type": "text", "text": "legitimate canvas text"}]
    )
    assert result == {"nodes": expected, "edges": []}
    assert client.dm.canvas_reads == 1


class FakeClient:
    def __init__(self):
        self.metadata = {"/note.md": {"id": NOTE, "type": "markdown"}}
        self.body = "# First"
        self.fail = False

    def get_document_structure(self, _resource):
        if self.fail:
            raise ConnectionError("offline")
        return None, {"type": "folder", "filemeta": self.metadata.copy()}

    def fetch_document_content(self, _resource):
        return self.body


def exporter(client, root):
    return SnapshotExporter(client, RELAY, [{"id": FOLDER, "prefix": "eng-relay"}], root)


def test_updates_and_rename_keep_immutable_previous_snapshot(tmp_path):
    client = FakeClient()
    mirror = exporter(client, tmp_path)
    first = mirror.export()
    client.body = "# Updated"
    client.metadata["/nested/renamed.md"] = client.metadata.pop("/note.md")
    second = mirror.export()
    assert (tmp_path / "current.json").read_text() == json.dumps(second, sort_keys=True)
    assert (
        tmp_path / "generations" / first["revision"] / "eng-relay/note.md"
    ).read_text() == "# First"
    assert (
        tmp_path / "generations" / second["revision"] / "eng-relay/nested/renamed.md"
    ).read_text() == "# Updated"
    assert not (tmp_path / "generations" / second["revision"] / "eng-relay/note.md").exists()


def test_incomplete_upload_and_outage_preserve_last_good_and_reconcile(tmp_path):
    client = FakeClient()
    mirror = exporter(client, tmp_path)
    first = mirror.export()
    client.body = None
    with pytest.raises(DeferredExport):
        mirror.export()
    client.fail = True
    with pytest.raises(ConnectionError):
        mirror.export()
    assert json.loads((tmp_path / "current.json").read_text()) == first
    client.fail = False
    client.body = "# Recovery"
    assert mirror.export()["revision"] != first["revision"]


def test_deletion_needs_confirmation_but_empty_reset_never_deletes(tmp_path):
    client = FakeClient()
    client.metadata["/retained.md"] = {"id": FILE, "type": "markdown"}
    mirror = exporter(client, tmp_path)
    first = mirror.export()
    del client.metadata["/note.md"]
    with pytest.raises(DeferredExport, match="second"):
        mirror.export()
    assert json.loads((tmp_path / "current.json").read_text()) == first
    second = mirror.export()
    assert second["files"] == 1
    client.metadata = {}
    with pytest.raises(DeferredExport, match="empty"):
        mirror.export()
    assert json.loads((tmp_path / "current.json").read_text()) == second


@pytest.mark.parametrize(
    "path", ["../../outside", "/../outside", "a/../b", "a/.git/config", "a//b", "a\\b"]
)
def test_paths_cannot_escape_or_publish_repository_metadata(path):
    with pytest.raises(DeferredExport):
        native_path(path)


def test_real_sdk_http_yjs_and_attachment_download_are_read_only(tmp_path):
    body = b"<img src='photo.png'>"
    file_hash = hashlib.sha256(body).hexdigest()
    folder = Doc(
        {
            "filemeta_v0": Map(
                {
                    "/note.md": {"id": NOTE, "type": "markdown"},
                    "/test.html": {"id": FILE, "type": "file", "hash": file_hash},
                }
            )
        }
    )
    note = Doc({"contents": Text("# Protocol proof")})
    requests_seen = []
    updates = {
        f"{RELAY}-{FOLDER}": folder.get_update(),
        f"{RELAY}-{NOTE}": note.get_update(),
    }

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            path = urlparse(self.path).path
            requests_seen.append((self.command, path))
            if path.startswith("/d/"):
                response = updates[path.split("/")[2]]
            elif path.startswith("/f/"):
                response = json.dumps(
                    {"downloadUrl": f"http://127.0.0.1:{self.server.server_port}/blob"}
                ).encode()
            elif path == "/blob":
                response = body
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.end_headers()
            self.wfile.write(response)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = RelayClient(f"http://127.0.0.1:{server.server_port}")
        result = exporter(client, tmp_path).export()
        assert (
            tmp_path / "generations" / result["revision"] / "eng-relay/note.md"
        ).read_text() == "# Protocol proof"
        assert (
            tmp_path / "generations" / result["revision"] / "eng-relay/test.html"
        ).read_bytes() == body
        assert all(method == "GET" for method, _ in requests_seen)
        assert any(path.endswith("/download-url") for _, path in requests_seen)
        plan = tmp_path / "plan.json"
        plan.write_text(
            json.dumps(
                {
                    "server_url": f"http://127.0.0.1:{server.server_port}",
                    "relay_id": RELAY,
                    "folders": [{"id": FOLDER, "prefix": "eng-relay"}],
                }
            )
        )
        environment = os.environ.copy()
        environment.pop("RELAY_SERVER_API_KEY", None)
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "snapshot_export",
                "--config",
                str(plan),
                "--root",
                str(tmp_path / "cli"),
                "--budget-bytes",
                "134217728",
                "--reserve-bytes",
                "1048576",
                "--reserve-inodes",
                "32",
                "--interval",
                "60",
            ],
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            with ThreadPoolExecutor(max_workers=1) as reader:
                line = reader.submit(process.stdout.readline)
                try:
                    published = json.loads(line.result(timeout=10))
                    assert published["revision"] == result["revision"]
                finally:
                    process.terminate()
                    process.wait(timeout=10)
            assert process.returncode == 0
            assert (tmp_path / "cli/current.json").is_file()
            assert all(method == "GET" for method, _ in requests_seen)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            process.stdout.close()
            process.stderr.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_mass_delete_and_mass_truncation_never_publish(tmp_path):
    client = FakeClient()
    client.metadata = {f"/{n}.md": {"id": str(n), "type": "markdown"} for n in range(30)}
    mirror = exporter(client, tmp_path)
    first = mirror.export()
    client.metadata = {"/0.md": {"id": "0", "type": "markdown"}}
    with pytest.raises(DeferredExport, match="mass"):
        mirror.export()
    assert json.loads((tmp_path / "current.json").read_text()) == first
    client.metadata = {f"/{n}.md": {"id": str(n), "type": "markdown"} for n in range(30)}
    client.body = ""
    with pytest.raises(DeferredExport, match="mass"):
        mirror.export()
    assert json.loads((tmp_path / "current.json").read_text()) == first


def test_changed_membership_and_attachment_checksum_defer_publication(tmp_path):
    client = FakeClient()
    mirror = exporter(client, tmp_path)
    first = mirror.export()
    calls = 0
    original = client.get_document_structure

    def changing(resource):
        nonlocal calls
        calls += 1
        result = original(resource)
        if calls == 2:
            return None, {"type": "folder", "filemeta": {}}
        return result

    client.get_document_structure = changing
    with pytest.raises(DeferredExport, match="changed"):
        mirror.export()
    client.get_document_structure = original
    client.metadata["/photo.png"] = {"id": FILE, "type": "image", "hash": "a" * 64}
    client.fetch_s3_file_content = lambda _resource, _hash: b"bad-checksum"
    with pytest.raises(DeferredExport, match="bytes"):
        mirror.export()
    assert json.loads((tmp_path / "current.json").read_text()) == first


def test_restart_discards_staging_and_restores_interrupted_quarantine(tmp_path):
    client = FakeClient()
    first = exporter(client, tmp_path).export()
    stale = tmp_path / ("staging-" + "a" * 32)
    stale.mkdir()
    (stale / "incomplete.md").write_text("half-written")
    generation = tmp_path / "generations" / first["revision"]
    trash = tmp_path / "retiring"
    trash.mkdir(exist_ok=True)
    generation.rename(trash / first["revision"])
    assert exporter(client, tmp_path).export() == first
    assert not stale.exists()
    assert (generation / "eng-relay/note.md").read_text() == "# First"


@pytest.mark.parametrize("phase", ["body", "state", "receipt"])
def test_disk_failure_preserves_last_good_and_restart_recovers(tmp_path, monkeypatch, phase):
    import errno
    from pathlib import Path

    import snapshot_export

    client = FakeClient()
    first = exporter(client, tmp_path).export()
    client.body = "# Updated after restart"
    original_open = Path.open
    original_replace = snapshot_export.os.replace

    def fail_open(path, *args, **kwargs):
        if phase == "body" and "staging-" in str(path) and args == ("wb",):
            raise OSError(errno.ENOSPC, "fixture full disk")
        return original_open(path, *args, **kwargs)

    def fail_replace(source, destination):
        target = "state.json" if phase == "state" else "current.json"
        if phase != "body" and destination == tmp_path / target:
            raise OSError(errno.ENOSPC, "fixture full disk")
        return original_replace(source, destination)

    with monkeypatch.context() as faults:
        faults.setattr(Path, "open", fail_open)
        faults.setattr(snapshot_export.os, "replace", fail_replace)
        with pytest.raises(OSError):
            exporter(client, tmp_path).export()
    assert json.loads((tmp_path / "current.json").read_text()) == first
    assert (tmp_path / "generations" / first["revision"] / "eng-relay/note.md").exists()
    assert not list(tmp_path.glob("staging-*"))
    recovered = exporter(client, tmp_path).export()
    assert recovered["revision"] != first["revision"]
    assert (
        tmp_path / "generations" / recovered["revision"] / "eng-relay/note.md"
    ).read_text() == client.body


def test_retention_protects_current_served_journal_and_permanent_reader_pins(tmp_path):
    client = FakeClient()
    mirror = SnapshotExporter(
        client, RELAY, [{"id": FOLDER, "prefix": "eng-relay"}], tmp_path, retain=1
    )
    receipts = []
    for version in range(5):
        client.body = f"# Version {version}"
        receipts.append(mirror.export())
        if version == 0:
            pins = tmp_path / "pins"
            pins.mkdir()
            (pins / receipts[0]["revision"]).touch()
        if version == 1:
            (tmp_path / "served.json").write_text(json.dumps(receipts[1]))
        if version == 2:
            (tmp_path / "served.previous.json").write_text(json.dumps(receipts[2]))
    assert (tmp_path / "generations" / receipts[0]["revision"]).exists()
    assert (tmp_path / "generations" / receipts[1]["revision"]).exists()
    assert (tmp_path / "generations" / receipts[2]["revision"]).exists()
    assert not (tmp_path / "generations" / receipts[3]["revision"]).exists()
    assert (tmp_path / "generations" / receipts[4]["revision"]).exists()


def test_reader_pin_racing_quarantine_restores_the_generation(tmp_path, monkeypatch):
    import snapshot_export

    client = FakeClient()
    mirror = SnapshotExporter(
        client, RELAY, [{"id": FOLDER, "prefix": "eng-relay"}], tmp_path, retain=1
    )
    first = mirror.export()
    pins = tmp_path / "pins"
    pins.mkdir()
    original = snapshot_export.os.replace

    def racing(source, destination):
        original(source, destination)
        if destination == tmp_path / "retiring" / first["revision"]:
            (pins / first["revision"]).touch()

    monkeypatch.setattr(snapshot_export.os, "replace", racing)
    client.body = "# New"
    mirror.export()
    assert (
        tmp_path / "generations" / first["revision"] / "eng-relay/note.md"
    ).read_text() == "# First"


def test_malformed_app_receipt_defers_retention_without_invalidating_publication(
    tmp_path,
):
    client = FakeClient()
    mirror = SnapshotExporter(
        client, RELAY, [{"id": FOLDER, "prefix": "eng-relay"}], tmp_path, retain=1
    )
    first = mirror.export()
    (tmp_path / "served.json").write_text("{incomplete")
    client.body = "# New"
    second = mirror.export()
    assert json.loads((tmp_path / "current.json").read_text()) == second
    assert (tmp_path / "generations" / first["revision"]).exists()


def test_incomplete_existing_generation_cannot_be_published(tmp_path):
    client = FakeClient()
    first = exporter(client, tmp_path).export()
    (tmp_path / "generations" / first["revision"] / "eng-relay/note.md").unlink()
    with pytest.raises(DeferredExport, match="incomplete"):
        exporter(client, tmp_path).export()
