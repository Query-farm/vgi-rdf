from __future__ import annotations

import os
import secrets
import socket
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest
from sql_support import WORKER, literal

from rdf_worker.config import database_path, validate_listener


def test_storage_root_prevents_path_and_symlink_escape(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    monkeypatch.setenv("VGI_RDF_ROOT", str(root))
    assert database_path("inside") == root / "inside"
    with pytest.raises(ValueError, match="outside"):
        database_path("../outside")
    try:
        (root / "link").symlink_to(outside, target_is_directory=True)
    except OSError:
        return  # Windows may not grant the symlink privilege.
    with pytest.raises(ValueError, match="outside"):
        database_path("link/database")


def test_network_listener_requires_authentication_and_storage_root(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ["VGI_BEARER_TOKENS", "VGI_JWT_ISSUER", "VGI_JWT_AUDIENCE", "VGI_RDF_ROOT"]:
        monkeypatch.delenv(key, raising=False)
    validate_listener([])
    validate_listener(["--http", "--host", "127.0.0.1"])
    with pytest.raises(ValueError, match="authentication"):
        validate_listener(["--http"])
    monkeypatch.setenv("VGI_BEARER_TOKENS", "example=operator")
    with pytest.raises(ValueError, match="VGI_RDF_ROOT"):
        validate_listener(["--http"])
    monkeypatch.setenv("VGI_RDF_ROOT", "/data")
    validate_listener(["--http"])


@contextmanager
def http_server(root: Path, token: str, *, read_only: bool = False) -> Iterator[str]:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    environment = {
        **os.environ,
        "VGI_BEARER_TOKENS": token + "=operator",
        "VGI_SIGNING_KEY": secrets.token_hex(32),
        "VGI_RDF_ROOT": str(root),
        "VGI_RDF_DATABASE": str(root / "shared"),
        "VGI_RDF_READ_ONLY": str(read_only).lower(),
    }
    process = subprocess.Popen(
        [WORKER, "--http", "--host", "127.0.0.1", "--port", str(port)],
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise AssertionError("HTTP server exited during startup")
            try:
                with urllib.request.urlopen(url + "/health", timeout=0.2):
                    break
            except urllib.error.HTTPError as error:
                if error.code == 401:
                    break
                raise
            except urllib.error.URLError, TimeoutError:
                time.sleep(0.05)
        else:
            raise AssertionError("HTTP server did not become ready")
        yield url
    finally:
        process.terminate()
        try:
            process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=5)


def test_authenticated_http_and_server_read_only_policy(tmp_path: Path) -> None:
    token = secrets.token_urlsafe(24)
    root = tmp_path / "root"
    root.mkdir()
    with http_server(root, token) as url, duckdb.connect() as c:
        c.execute("LOAD vgi FROM query_farm")
        with pytest.raises(duckdb.Error):
            c.execute(f"ATTACH 'rdf' (TYPE vgi, LOCATION {literal(url)}, bearer_token 'incorrect')")
        c.execute(f"ATTACH 'rdf' (TYPE vgi, LOCATION {literal(url)}, bearer_token {literal(token)})")
        c.execute("CREATE TABLE rdf.main.t(id INT)")
        c.execute("INSERT INTO rdf.main.t VALUES(7)")
        assert c.execute("SELECT answer FROM rdf.main.rdf_sparql('ASK {}')").fetchone() == (True,)
        c.execute("DETACH rdf")
        outside = tmp_path / "forbidden"
        with pytest.raises(duckdb.Error, match="outside VGI_RDF_ROOT"):
            c.execute(
                f"ATTACH 'rdf' (TYPE vgi, LOCATION {literal(url)}, bearer_token {literal(token)}, "
                f"database {literal(str(outside))})"
            )
        assert not outside.exists()
    with http_server(root, token, read_only=True) as url, duckdb.connect() as c:
        c.execute("LOAD vgi FROM query_farm")
        c.execute(f"ATTACH 'rdf' (TYPE vgi, LOCATION {literal(url)}, bearer_token {literal(token)}, read_only false)")
        assert c.execute("SELECT id FROM rdf.main.t").fetchone() == (7,)
        with pytest.raises(duckdb.Error, match="read-only"):
            c.execute("INSERT INTO rdf.main.t VALUES(8)")
