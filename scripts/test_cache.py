#!/usr/bin/env python3
"""Report and enforce the workspace's bounded Cargo test cache."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
import os
from pathlib import Path
import sys


GIBIBYTE = 1024**3
DEFAULT_LIMIT_BYTES = 30 * GIBIBYTE
PROFILE_DIRECTORIES = ("debug", "workspace-test", "release")
WORKSPACE = Path(__file__).resolve().parents[1]


class CacheLimitExceeded(RuntimeError):
    """Raised when the isolated workspace-test cache exceeds its limit."""


def cargo_target_dir(
    workspace: Path,
    environ: Mapping[str, str],
) -> Path:
    """Resolve Cargo's target directory, including a configured override."""
    configured = environ.get("CARGO_TARGET_DIR")
    if not configured:
        return workspace / "target"
    target = Path(configured)
    return target if target.is_absolute() else workspace / target


def allocated_size(path: Path) -> int:
    """Return allocated filesystem bytes, counting hard links only once."""
    total = 0
    seen: set[tuple[int, int]] = set()
    pending = [path]
    while pending:
        current = pending.pop()
        try:
            stat = current.lstat()
        except FileNotFoundError:
            continue
        inode = (stat.st_dev, stat.st_ino)
        if inode in seen:
            continue
        seen.add(inode)
        total += stat.st_blocks * 512
        if current.is_dir() and not current.is_symlink():
            try:
                pending.extend(current.iterdir())
            except FileNotFoundError:
                continue

    return total


def profile_sizes(target: Path) -> dict[str, int]:
    """Return allocated sizes for the Cargo profile directories we manage."""
    return {
        profile: allocated_size(target / profile)
        for profile in PROFILE_DIRECTORIES
    }


def format_size(size: int) -> str:
    """Format an allocated byte count for a human-facing diagnostic."""
    return f"{size / GIBIBYTE:.2f} GiB"


def enforce_limit(
    size: int,
    limit: int = DEFAULT_LIMIT_BYTES,
) -> None:
    """Reject a cache strictly larger than the configured limit."""
    if size <= limit:
        return
    raise CacheLimitExceeded(
        "workspace-test cache is "
        f"{format_size(size)}, above the {limit // GIBIBYTE} GiB limit; "
        "run `just clean-test-cache` and retry"
    )


def status(target: Path) -> None:
    """Print allocated sizes for the managed profile directories."""
    print(f"Cargo target: {target}")
    for profile, size in profile_sizes(target).items():
        print(f"{profile}: {format_size(size)}")


def guard(target: Path) -> None:
    """Enforce the workspace-test cache limit."""
    enforce_limit(allocated_size(target / "workspace-test"))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("status", "guard"))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    target = cargo_target_dir(WORKSPACE, os.environ)
    if args.command == "status":
        status(target)
        return 0
    try:
        guard(target)
    except CacheLimitExceeded as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
