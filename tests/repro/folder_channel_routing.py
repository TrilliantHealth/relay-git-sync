#!/usr/bin/env python3
"""Reproduce lost edits from a channel-less first read, and their fix by folder keys.

relay-server fixes a document's routing channel when it first loads it, from the
key of the request that loaded it. This script runs a real relay-server and two
Git Sync processes from this checkout on one shared folder:

  backup  Git Sync with only RELAY_SERVER_API_KEY (channel-less), as a deployed
          backup runs today. Started first; its own load is then unloaded.
  reader  a second Git Sync whose startup read is the note's first load:
          channel-less in mode "channel-less", RELAY_FOLDER_TOKENS in "folder-keys".

An editor (a document key carrying the folder channel, as Obsidian's) then edits the
note. The script reports whether the edit reached each checkout. Expected:

  channel-less  the edit reaches neither the reader nor the backup
  folder-keys   the edit reaches both

Usage, from the repository root (needs a relay-server binary; no network access):

    RELAY_BIN=/path/to/relay uv run python tests/repro/folder_channel_routing.py

It exits 0 when both expectations hold. Every key is generated for the run and
discarded with it.
"""

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import requests
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from pycrdt import Doc, Map, Text

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
import relay_auth  # noqa: E402

RELAY_BIN = os.environ["RELAY_BIN"]
# relay-server unloads a document about 2 x checkpoint_freq_seconds after its last
# use; 1 s keeps the run short.
UNLOAD_WAIT_S = 4
EDIT_WAIT_S = 20
procs = []


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start(name, cmd, cwd, env, logs):
    log = open(logs / f"{name}.log", "w")
    proc = subprocess.Popen(
        cmd, cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True
    )
    procs.append(proc)
    return proc


def stop(proc):
    if proc.poll() is None:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)


def wait_until(condition, timeout, interval=0.2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if condition():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


class Relay:
    def __init__(self, work, logs):
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.relay_id = str(uuid.uuid4())
        self.admin_key = Ed25519PrivateKey.generate()
        self.admin_key_id = "repro-admin"
        self.setup = relay_auth.generate_setup(
            server_url=self.url, relay_id=self.relay_id, expires_days=1
        )
        admin_public = relay_auth.b64url_encode(relay_auth.raw_public_key(self.admin_key))
        (work / "relay.toml").write_text(
            f"""[server]
url = "{self.url}"
host = "127.0.0.1"
port = {self.port}
checkpoint_freq_seconds = 1

[metrics]
port = {free_port()}

[[auth]]
key_id = "{self.admin_key_id}"
public_key = "{admin_public}"
allowed_token_types = ["server", "document", "file", "prefix"]

{relay_auth.relay_toml_snippet(self.setup.keypair)}

[store]
type = "filesystem"
path = "{work / 'store'}"
"""
        )
        start(
            "relay",
            [RELAY_BIN, "serve", "-c", str(work / "relay.toml")],
            work,
            dict(os.environ, RUST_LOG=os.environ.get("RUST_LOG", "info")),
            logs,
        )
        assert wait_until(lambda: requests.get(f"{self.url}/ready").ok, 60), "relay did not start"

    def mint(self, scope, channel=None, key=None, key_id=None):
        return relay_auth.create_cwt_sign1_token(
            key or self.admin_key,
            key_id=key_id or self.admin_key_id,
            server_url=self.url,
            scope=scope,
            issued_at=int(time.time()),
            expires_at=int(time.time()) + 3600,
            channel=channel,
        )

    def folder_key(self, folder_doc):
        """A read-only key from the setup's own key pair, with the folder channel."""
        key = Ed25519PrivateKey.from_private_bytes(
            relay_auth.b64url_decode(self.setup.keypair.private_key)
        )
        return self.mint(self.setup.token.scope, folder_doc, key, self.setup.keypair.key_id)

    def push(self, doc_id, token, build):
        headers = {"Authorization": f"Bearer {token}"}
        r = requests.get(f"{self.url}/d/{doc_id}/as-update", headers=headers)
        r.raise_for_status()
        doc = Doc()
        doc.apply_update(r.content)
        before = doc.get_state()
        build(doc)
        requests.post(
            f"{self.url}/d/{doc_id}/update", data=doc.get_update(before), headers=headers
        ).raise_for_status()


def set_text(text):
    def build(doc):
        t = doc.get("contents", type=Text)
        del t[0 : len(t)]
        t.insert(0, text)

    return build


def git_sync(name, relay, folder_id, data, logs, folder_tokens=None):
    data.mkdir()
    (data / "git_connectors.toml").write_text(
        f'[relay]\nurl = "{relay.url}"\nid = "{relay.relay_id}"\n\n'
        f'[[git_connector]]\nshared_folder_id = "{folder_id}"\nprefix = "notes"\n'
    )
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in ("SSH_PRIVATE_KEY", "WEBHOOK_SECRET", "RELAY_FOLDER_TOKENS")
    }
    env.update(RELAY_SERVER_API_KEY=relay.setup.token.value, PYTHONUNBUFFERED="1")
    if folder_tokens:
        env["RELAY_FOLDER_TOKENS"] = json.dumps(folder_tokens)
    port = free_port()
    proc = start(
        name,
        [
            sys.executable,
            "app.py",
            "--data-dir",
            str(data),
            "--port",
            str(port),
            "--commit-interval",
            "2",
        ],
        REPO,
        env,
        logs,
    )
    assert wait_until(lambda: requests.get(f"http://127.0.0.1:{port}/health").ok, 120), name
    return proc


def run_mode(mode, relay, work, logs):
    folder_id, note_id = str(uuid.uuid4()), str(uuid.uuid4())
    folder_doc, note_doc = f"{relay.relay_id}-{folder_id}", f"{relay.relay_id}-{note_id}"
    editor = relay.mint(f"doc:{note_doc}:rw", channel=folder_doc)
    relay.push(note_doc, editor, set_text("v0\n"))
    relay.push(
        folder_doc,
        relay.mint("server"),
        lambda doc: doc.get("filemeta_v0", type=Map).__setitem__(
            "/note.md", {"id": note_id, "type": "markdown", "version": 0}
        ),
    )
    time.sleep(UNLOAD_WAIT_S)

    files = {}
    procs_here = []
    for name, tokens in (
        ("backup", None),
        ("reader", {folder_id: relay.folder_key(folder_doc)} if mode == "folder-keys" else None),
    ):
        data = work / f"{mode}-{name}"
        procs_here.append(git_sync(f"{mode}-{name}", relay, folder_id, data, logs, tokens))
        files[name] = data / "repos" / relay.relay_id / folder_id / "notes" / "note.md"
        assert wait_until(lambda: files[name].exists(), 60), f"{name} never wrote the note"
        if name == "backup":
            time.sleep(UNLOAD_WAIT_S)  # unload what the backup's own read loaded

    time.sleep(1)
    text = f"edited after the reader's first read ({mode})\n"
    relay.push(note_doc, editor, set_text(text))
    arrived = {
        name: wait_until(lambda p=path: p.read_text() == text, EDIT_WAIT_S)
        for name, path in files.items()
    }
    for proc in procs_here:
        stop(proc)
    return arrived


def main():
    work = Path(tempfile.mkdtemp(prefix="folder-channel-repro-"))
    logs = work / "logs"
    logs.mkdir()
    print(f"work directory: {work}", flush=True)
    try:
        relay = Relay(work, logs)
        results = {
            mode: run_mode(mode, relay, work, logs) for mode in ("channel-less", "folder-keys")
        }
    finally:
        for proc in reversed(procs):
            stop(proc)
    print(json.dumps(results, indent=2), flush=True)
    reproduced = not any(results["channel-less"].values()) and all(results["folder-keys"].values())
    print(
        (
            "REPRODUCED: channel-less loses the edit; folder keys deliver it"
            if reproduced
            else "NOT REPRODUCED"
        ),
        flush=True,
    )
    if reproduced and not os.environ.get("KEEP_WORK"):
        shutil.rmtree(work)
    return 0 if reproduced else 1


if __name__ == "__main__":
    sys.exit(main())
