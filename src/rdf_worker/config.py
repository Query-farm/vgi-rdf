"""Deployment boundaries for filesystem access and network listeners."""

from __future__ import annotations

import os
from pathlib import Path


def database_path(location: str) -> Path:
    """Resolve a database path within an optional operator-owned root directory."""
    root = os.environ.get("VGI_RDF_ROOT")
    candidate = Path(location).expanduser()
    if root:
        base = Path(root).expanduser().resolve()
        candidate = (base / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()
        if not candidate.is_relative_to(base):
            raise ValueError("RDF database path is outside VGI_RDF_ROOT")
    return candidate.resolve()


def forced_read_only() -> bool:
    """Honor a server-enforced read-only policy that clients cannot override."""
    value = os.environ.get("VGI_RDF_READ_ONLY", "false").lower()
    if value not in {"true", "false", "1", "0"}:
        raise ValueError("VGI_RDF_READ_ONLY must be true or false")
    return value in {"true", "1"}


def validate_listener(arguments: list[str]) -> None:
    """Require authentication and restricted storage for non-loopback HTTP listeners."""
    if "--http" not in arguments or "--help" in arguments:
        return
    host = "0.0.0.0"
    for index, argument in enumerate(arguments):
        if argument.startswith("--host="):
            host = argument.partition("=")[2]
        elif argument == "--host" and index + 1 < len(arguments):
            host = arguments[index + 1]
    if host in {"127.0.0.1", "::1", "localhost"}:
        return
    if not (
        os.environ.get("VGI_BEARER_TOKENS") or (os.environ.get("VGI_JWT_ISSUER") and os.environ.get("VGI_JWT_AUDIENCE"))
    ):
        raise ValueError("Non-loopback HTTP requires VGI_BEARER_TOKENS or configured JWT authentication")
    if not os.environ.get("VGI_RDF_ROOT"):
        raise ValueError("Non-loopback HTTP requires VGI_RDF_ROOT to restrict database paths")
