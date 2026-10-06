"""Verify the companion console command and its runtime modules are packaged."""

import zipfile
from pathlib import Path


def main():
    wheels = sorted(Path("dist").glob("*.whl"))
    if len(wheels) != 1:
        raise SystemExit("expected exactly one freshly built wheel")
    with zipfile.ZipFile(wheels[0]) as wheel:
        names = wheel.namelist()
        required = (
            "snapshot_export.py",
            "snapshot_budget.py",
            "relay_client.py",
            "models.py",
            "s3rn.py",
            "relay_sdk/__init__.py",
        )
        assert all(name in names for name in required)
        assert not any(name.endswith((".env", ".key", ".pem")) for name in names)
        entry = next(name for name in names if name.endswith(".dist-info/entry_points.txt"))
        assert "relay-snapshot-export = snapshot_export:main" in wheel.read(entry).decode()
    print(
        "wheel contains actual console entrypoint and snapshot/SDK runtime, without credential files"
    )


if __name__ == "__main__":
    main()
