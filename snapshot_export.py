"""Read-only Relay export into immutable, native-path vault snapshots.

The executable prints a small JSON publication receipt, never document bodies or
credentials. Membership is checked before and after fetching every body; missing
uploads defer the entire publication. Deletion requires two successful observations.
"""

import argparse
import fcntl
import hashlib
import json
import logging
import os
import re
import shutil
import signal
import sys
import threading
import uuid
from pathlib import Path, PurePosixPath

from models import create_document_resource_from_metadata
from relay_client import RelayClient
from s3rn import S3RemoteCanvas, S3RemoteDocument, S3RemoteFolder


class DeferredExport(RuntimeError):
    """The remote does not yet prove a complete, safely publishable snapshot."""


def native_path(value):
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise DeferredExport("invalid native path")
    value = value.removeprefix("/")
    parts = value.split("/")
    if any(part in ("", ".", "..", ".git", ".obsidian") for part in parts):
        raise DeferredExport("invalid native path")
    return str(PurePosixPath(value))


def atomic_json(path, value):
    temp = path.with_suffix(".pending")
    try:
        with temp.open("w") as handle:
            handle.write(json.dumps(value, sort_keys=True))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
        sync_directory(path.parent)
    finally:
        temp.unlink(missing_ok=True)


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def revision_name(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


class SnapshotExporter:
    def __init__(self, client, relay_id, folders, root, retain=3):
        self.client = client
        self.relay_id = relay_id
        self.folders = folders
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        if retain < 1:
            raise ValueError("retain must be positive")
        self.retain = retain
        self.plan = hashlib.sha256(
            json.dumps([relay_id, folders], sort_keys=True).encode()
        ).hexdigest()

    def _metadata(self, folder):
        _, parsed = self.client.get_document_structure(S3RemoteFolder(self.relay_id, folder["id"]))
        if parsed.get("type") != "folder" or not isinstance(parsed.get("filemeta"), dict):
            raise DeferredExport("folder has not been uploaded")
        return parsed["filemeta"]

    def export(self):
        with (self.root / "export.lock").open("w") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            self._recover_storage()
            receipt = self._export_locked()
            try:
                self._retain_generations()
            except (OSError, DeferredExport, ValueError):
                # Cleanup is optional; publication already points at complete content.
                print(
                    json.dumps({"warning": "retention deferred"}),
                    file=sys.stderr,
                    flush=True,
                )
            return receipt

    def _export_locked(self):
        state_path = self.root / "state.json"
        state = json.loads(state_path.read_text()) if state_path.exists() else {}
        if state and state.get("plan") != self.plan:
            raise DeferredExport("snapshot root belongs to another folder plan")
        previous = state.get("membership", {})
        membership, files = {}, {}
        for folder in self.folders:
            prefix = native_path(folder["prefix"])
            metadata = self._metadata(folder)
            if not metadata and any(key.startswith(folder["id"] + "/") for key in previous):
                raise DeferredExport("empty folder map cannot establish deletion")
            for remote_path, entry in sorted(metadata.items()):
                path = prefix + "/" + native_path(remote_path)
                if entry.get("type") == "folder":
                    continue
                resource = create_document_resource_from_metadata(
                    self.relay_id, folder["id"], entry
                )
                identity = folder["id"] + "/" + entry["id"]
                if identity in membership or path in files:
                    raise DeferredExport("duplicate resource or native path")
                if isinstance(resource, S3RemoteDocument):
                    body = self.client.fetch_document_content(resource)
                    content = None if body is None else body.encode("utf-8")
                elif isinstance(resource, S3RemoteCanvas):
                    body = self.client.fetch_canvas_content(resource)
                    content = None if body is None else body.encode("utf-8")
                else:
                    expected = entry.get("hash")
                    if not isinstance(expected, str) or len(expected) != 64:
                        raise DeferredExport("attachment upload has no content hash")
                    content = self.client.fetch_s3_file_content(resource, expected)
                    if content is not None and hashlib.sha256(content).hexdigest() != expected:
                        raise DeferredExport("attachment bytes do not match the manifest")
                if content is None:
                    raise DeferredExport("resource has not been uploaded")
                membership[identity] = {"path": path, "size": len(content)}
                files[path] = content
            if metadata != self._metadata(folder):
                raise DeferredExport("folder changed while exporting")
        if not self.folders or not files:
            raise DeferredExport("no publishable content")
        removed = sorted(set(previous) - set(membership))
        truncated = sum(
            entry["size"] == 0 and previous.get(key, {}).get("size", 0) > 0
            for key, entry in membership.items()
        )
        threshold = max(int(len(previous) * 0.1), 25)
        if len(removed) > threshold or truncated > threshold:
            raise DeferredExport("mass deletion or truncation requires review")
        if removed and state.get("pending_removed") != removed:
            atomic_json(state_path, {**state, "plan": self.plan, "pending_removed": removed})
            raise DeferredExport("deletions await a second successful observation")
        digest = hashlib.sha256()
        for path, content in sorted(files.items()):
            digest.update(json.dumps([path, hashlib.sha256(content).hexdigest()]).encode())
        revision = digest.hexdigest()
        generation = self.root / "generations" / revision
        if not generation.exists():
            staging = self.root / ("staging-" + uuid.uuid4().hex)
            staging.mkdir()
            try:
                for path, content in files.items():
                    destination = staging / path
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    with destination.open("wb") as handle:
                        handle.write(content)
                        handle.flush()
                        os.fsync(handle.fileno())
                for directory, _children, _files in os.walk(staging, topdown=False):
                    sync_directory(directory)
                generation.parent.mkdir(exist_ok=True)
                os.replace(staging, generation)
                sync_directory(generation.parent)
            finally:
                shutil.rmtree(staging, ignore_errors=True)
        self._verify_generation(generation, files)
        receipt = {
            "revision": revision,
            "content_dir": str(generation),
            "files": len(files),
        }
        atomic_json(state_path, {"plan": self.plan, "membership": membership})
        atomic_json(self.root / "current.json", receipt)
        return receipt

    def _verify_generation(self, generation, files):
        if generation.is_symlink() or generation.parent.is_symlink():
            raise DeferredExport("generation cannot be a symlink")
        observed = set()
        for directory, children, entries in os.walk(generation):
            for name in children + entries:
                if (Path(directory) / name).is_symlink():
                    raise DeferredExport("generation cannot contain symlinks")
            for name in entries:
                path = Path(directory) / name
                relative = path.relative_to(generation).as_posix()
                if relative not in files or path.read_bytes() != files[relative]:
                    raise DeferredExport("generation differs from complete export")
                observed.add(relative)
        if observed != set(files):
            raise DeferredExport("generation is incomplete")

    def _recover_storage(self):
        # An interrupted quarantine is always restored before considering cleanup.
        trash = self.root / "retiring"
        generations = self.root / "generations"
        if generations.is_symlink():
            raise DeferredExport("storage directory cannot be a symlink")
        if trash.exists():
            if trash.is_symlink() or generations.is_symlink():
                raise DeferredExport("storage directory cannot be a symlink")
            for candidate in trash.iterdir():
                if (
                    revision_name(candidate.name)
                    and candidate.is_dir()
                    and not candidate.is_symlink()
                ):
                    destination = generations / candidate.name
                    if not destination.exists():
                        os.replace(candidate, destination)
        for staging in self.root.glob("staging-*"):
            suffix = staging.name.removeprefix("staging-")
            if re.fullmatch(r"[0-9a-f]{32}", suffix) and not staging.is_symlink():
                shutil.rmtree(staging)

    def _protected_revisions(self):
        protected = set()
        for name in ("current.json", "served.json", "served.previous.json"):
            path = self.root / name
            if path.exists():
                receipt = json.loads(path.read_text())
                if receipt is None and name == "served.previous.json":
                    continue
                if not isinstance(receipt, dict):
                    raise DeferredExport("invalid receipt prevents retention")
                revision = receipt.get("revision")
                if not revision_name(revision):
                    raise DeferredExport("invalid receipt prevents retention")
                protected.add(revision)
        pins = self.root / "pins"
        if pins.is_symlink():
            raise DeferredExport("pin directory cannot be a symlink")
        if pins.exists():
            for pin in pins.iterdir():
                if not revision_name(pin.name):
                    raise DeferredExport("invalid pin prevents retention")
                protected.add(pin.name)
        return protected

    def _retain_generations(self):
        generations = self.root / "generations"
        if generations.is_symlink():
            raise DeferredExport("generation directory cannot be a symlink")
        candidates = sorted(
            (
                path
                for path in generations.iterdir()
                if revision_name(path.name) and path.is_dir() and not path.is_symlink()
            ),
            key=lambda path: path.stat().st_mtime_ns,
            reverse=True,
        )
        protected = self._protected_revisions() | {path.name for path in candidates[: self.retain]}
        trash = self.root / "retiring"
        trash.mkdir(exist_ok=True)
        if trash.is_symlink():
            raise DeferredExport("quarantine cannot be a symlink")
        for candidate in candidates:
            if candidate.name in protected:
                continue
            quarantined = trash / candidate.name
            if quarantined.exists():
                raise DeferredExport("unrecovered quarantine")
            os.replace(candidate, quarantined)
            sync_directory(generations)
            # App pins before opening the generation and verifies its original path.
            # This second check closes the pin-versus-quarantine acquisition race.
            if candidate.name in self._protected_revisions():
                os.replace(quarantined, candidate)
                sync_directory(generations)
            else:
                shutil.rmtree(quarantined)
                sync_directory(trash)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument(
        "--interval",
        type=float,
        default=0,
        help="Reconcile every N seconds; zero exports once",
    )
    parser.add_argument(
        "--retain",
        type=int,
        default=3,
        help="Keep N newest generations plus every app pin and source receipt",
    )
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    try:
        config = json.loads(args.config.read_text())
        client = RelayClient(config["server_url"], os.environ.get("RELAY_SERVER_API_KEY"))
        exporter = SnapshotExporter(
            client, config["relay_id"], config["folders"], args.root, args.retain
        )
        stopped = threading.Event()
        signal.signal(signal.SIGTERM, lambda *_args: stopped.set())
        signal.signal(signal.SIGINT, lambda *_args: stopped.set())
        if args.interval < 0:
            raise ValueError("interval cannot be negative")
        while not stopped.is_set():
            try:
                print(json.dumps(exporter.export()), flush=True)
            except Exception as error:
                if not args.interval:
                    raise
                print(
                    json.dumps({"error": "export deferred", "kind": type(error).__name__}),
                    file=sys.stderr,
                    flush=True,
                )
            if not args.interval:
                return 0
            stopped.wait(args.interval)
        return 0
    except Exception as error:
        print(
            json.dumps({"error": "export deferred", "kind": type(error).__name__}),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
