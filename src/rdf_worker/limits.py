"""Operator-controlled resource limits shared by SQL and graph operations."""

from __future__ import annotations

import os


def setting(name: str, default: int, *, maximum: int = 2**63 - 1) -> int:
    """Read a positive integer limit, rejecting invalid deployment configuration."""
    value = int(os.environ.get("VGI_RDF_" + name, default))
    if not 1 <= value <= maximum:
        raise ValueError(f"VGI_RDF_{name} must be between 1 and {maximum}")
    return value


def query_limits(rows: int, timeout_ms: int) -> tuple[int, int]:
    """Enforce deployment ceilings; callers may request smaller limits."""
    maximum_rows = setting("MAX_QUERY_ROWS", 100_000)
    maximum_time = setting("QUERY_TIMEOUT_MS", 30_000)
    if not 1 <= rows <= maximum_rows:
        raise ValueError(f"max_rows must be between 1 and {maximum_rows}")
    if not 1 <= timeout_ms <= maximum_time:
        raise ValueError(f"timeout_ms must be between 1 and {maximum_time}")
    return rows, timeout_ms
