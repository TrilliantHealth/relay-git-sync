"""Conservative admission for another complete snapshot on a dedicated filesystem."""

import os
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Mapping


class StorageRefused(RuntimeError):
    """Freshness is frozen because a storage reserve cannot be maintained."""


def allocated_usage(root: Path) -> int:
    """Count allocated bytes of all owned history, pins, receipts and staging."""
    total = 0
    for path in (root, *root.rglob("*")):
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode):
            raise StorageRefused("storage symlink prevents budget accounting")
        total += info.st_blocks * 512
    return total


def prospective_usage(
    files: Mapping[str, bytes], metadata_bytes: int, block: int
) -> tuple[int, int]:
    """Reserve dense blocks for files/directories and concurrent JSON replacements."""
    directories = {".", "generations"}
    for name in files:
        directories.update(str(parent) for parent in PurePosixPath(name).parents)
    rounded = lambda size: ((size + block - 1) // block) * block
    # A directory, receipt, pending journal and reader pin each need their own inode.
    metadata_blocks = 4 * rounded(metadata_bytes)
    return (
        sum(rounded(len(body)) for body in files.values())
        + len(directories) * block
        + metadata_blocks,
        len(files) + len(directories) + 4,
    )


@dataclass(frozen=True)
class StorageBudget:
    limit_bytes: int
    reserve_bytes: int
    reserve_inodes: int

    def __post_init__(self):
        if (
            self.limit_bytes <= self.reserve_bytes
            or min(self.reserve_bytes, self.reserve_inodes) <= 0
        ):
            raise ValueError("budget must exceed positive byte and inode reserves")

    def before_write(
        self, root: Path, files: Mapping[str, bytes], metadata_bytes: int = 4096
    ) -> tuple[int, int]:
        filesystem = os.statvfs(root)
        block = max(filesystem.f_frsize, filesystem.f_bsize, 4096)
        prospective_bytes, prospective_inodes = prospective_usage(files, metadata_bytes, block)
        if filesystem.f_bavail * filesystem.f_frsize < prospective_bytes + self.reserve_bytes:
            raise StorageRefused("filesystem byte reserve reached")
        if filesystem.f_favail < prospective_inodes + self.reserve_inodes:
            raise StorageRefused("filesystem inode reserve reached")
        return prospective_bytes, prospective_inodes

    def admit(self, root: Path, files: Mapping[str, bytes], metadata_bytes: int = 4096) -> dict:
        prospective_bytes, _inodes = self.before_write(root, files, metadata_bytes)
        used = allocated_usage(root)
        if used + prospective_bytes + self.reserve_bytes > self.limit_bytes:
            raise StorageRefused("snapshot allocation budget reached")
        return {"allocated_bytes": used, "prospective_bytes": prospective_bytes}
