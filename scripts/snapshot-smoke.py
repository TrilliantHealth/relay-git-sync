"""Exercise the installed companion with loopback Yjs data and a synthetic credential."""

import hashlib
import json
import os
import subprocess
import sys
import tempfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from urllib.parse import urlparse

from pycrdt import Doc, Map, Text

RELAY = "11111111-1111-1111-1111-111111111111"
FOLDER = "22222222-2222-2222-2222-222222222222"
NOTE = "33333333-3333-3333-3333-333333333333"
FILE = "44444444-4444-4444-4444-444444444444"
SENTINEL = "synthetic-smoke-credential-not-a-real-token"


def main():
    binary = Path(sys.executable).parent / "relay-snapshot-export"
    assert binary.is_file(), "installed console entrypoint missing"
    asset = b"<p>Native attachment fixture</p>"
    digest = hashlib.sha256(asset).hexdigest()
    document = Doc({"contents": Text("# Synthetic snapshot")})
    folder = Doc(
        {
            "filemeta_v0": Map(
                {
                    "/fixture.md": {"id": NOTE, "type": "markdown"},
                    "/fixture.html": {"id": FILE, "type": "file", "hash": digest},
                }
            )
        }
    )
    seen = []
    unavailable = False

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            path = urlparse(self.path).path
            seen.append(self.command)
            if unavailable:
                self.send_error(503)
                return
            if path.startswith("/d/"):
                guid = path.split("/")[2]
                assert guid in (f"{RELAY}-{FOLDER}", f"{RELAY}-{NOTE}")
                payload = (folder if guid.endswith(FOLDER) else document).get_update()
            elif path.startswith("/f/"):
                payload = json.dumps(
                    {"downloadUrl": f"http://127.0.0.1:{self.server.server_port}/asset"}
                ).encode()
            elif path == "/asset":
                payload = asset
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with tempfile.TemporaryDirectory() as scratch:
            scratch = Path(scratch)
            plan = scratch / "plan.json"
            plan.write_text(
                json.dumps(
                    {
                        "server_url": f"http://127.0.0.1:{server.server_port}",
                        "relay_id": RELAY,
                        "folders": [{"id": FOLDER, "prefix": "eng-relay"}],
                    }
                )
            )
            root = scratch / "snapshots"
            command = [
                str(binary),
                "--config",
                str(plan),
                "--root",
                str(root),
                "--budget-bytes",
                "134217728",
                "--reserve-bytes",
                "1048576",
                "--reserve-inodes",
                "32",
            ]
            env = {
                key: value
                for key, value in os.environ.items()
                if key not in ("RELAY_SERVER_API_KEY", "PYTHONPATH")
            }
            env["RELAY_SERVER_API_KEY"] = SENTINEL
            successful = subprocess.run(
                command, env=env, capture_output=True, text=True, timeout=30
            )
            assert SENTINEL not in successful.stdout + successful.stderr
            assert successful.returncode == 0, "synthetic export failed"
            receipt = json.loads(successful.stdout)
            generation = Path(receipt["content_dir"])
            assert (generation / "eng-relay/fixture.html").read_bytes() == asset
            assert (generation / "eng-relay/fixture.md").read_text() == "# Synthetic snapshot"
            accepted = (root / "current.json").read_bytes()
            unavailable = True
            failed = subprocess.run(command, env=env, capture_output=True, text=True, timeout=30)
            assert failed.returncode == 1
            assert SENTINEL not in failed.stdout + failed.stderr
            assert json.loads(failed.stderr)["error"] == "export deferred"
            assert (root / "current.json").read_bytes() == accepted
            unavailable = False
            refused_command = command.copy()
            refused_command[refused_command.index("--budget-bytes") + 1] = "1048577"
            refused = subprocess.run(
                refused_command, env=env, capture_output=True, text=True, timeout=30
            )
            assert refused.returncode == 1
            assert SENTINEL not in refused.stdout + refused.stderr
            assert json.loads(refused.stderr)["freshness"] == "frozen"
            assert (root / "current.json").read_bytes() == accepted
            assert all(method == "GET" for method in seen)
            print(
                json.dumps(
                    {
                        "installed_entrypoint": True,
                        "native_asset": True,
                        "read_only_methods": True,
                        "outage_preserves_current": True,
                        "budget_refusal_preserves_current": True,
                        "synthetic_secret_suppressed": True,
                    }
                )
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


if __name__ == "__main__":
    main()
