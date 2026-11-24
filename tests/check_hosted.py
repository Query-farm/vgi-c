"""Orchard hosted mode (VGI_ORCHARD_HOSTED=1) against the C example worker.

A port of vgi-python's tests/test_orchard_hosted.py, plus the C runtime's own
rules (JWKS outage, authentication before the body). It runs the worker as a
real process in hosted mode against a loopback JWKS server holding freshly
generated keys, and checks the contract in docs/index.md ("Orchard hosted
mode"):

- a valid Pass reaches function code, with claims in their JSON types;
- no Pass, a tampered claim, the wrong aud, an expired Pass, the wrong typ and
  RS256 / HS256 tokens all get 401 with WWW-Authenticate: Bearer
  error="invalid_token", the capability headers and no resource_metadata;
  HEAD /health and CORS preflights alone are anonymous;
- authentication happens before the body is read, size-checked or decoded;
- a JWKS outage is 503 with Retry-After, never stale keys;
- Authorization never reaches the access log or standard error;
- every misconfiguration refuses to start, with a non-zero exit.

Run with the vgi-python environment, which needs the oauth extra (joserfc):

    VGI_C_BUILD=build ../vgi-python/.venv/bin/python -m pytest tests/check_hosted.py

VGI_WORKER_BIN overrides the worker binary.
"""

from __future__ import annotations

import base64
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, os.environ.get("VGI_PYTHON", str(ROOT.parent / "vgi-python")))

joserfc = pytest.importorskip("joserfc")
from joserfc import jwt  # noqa: E402
from joserfc.jwk import ECKey, OctKey, RSAKey  # noqa: E402

from vgi.arguments import Arguments  # noqa: E402
from vgi.client import Client  # noqa: E402

ISSUER = "https://auth.orchard.test"
AUDIENCE = "https://desk.orchard.test/d/dep_123"
SIGNING_KEY = "orchard-hosted-test-signing-key-0001"

EC = ECKey.generate_key("P-256", parameters={"kid": "desk-es256-1"}, private=True)
EC_ROTATED = ECKey.generate_key(
    "P-256", parameters={"kid": "desk-es256-2"}, private=True
)
RSA = RSAKey.generate_key(2048, parameters={"kid": "desk-rs256-1"}, private=True)

_AUTH_ENV = (
    "VGI_SIGNING_KEY_MINTED",
    "VGI_BEARER_TOKENS",
    "VGI_OPTIONAL_BEARER_TOKENS",
    "VGI_JWT_ISSUER",
    "VGI_JWT_AUDIENCE",
    "VGI_JWT_JWKS_URI",
    "VGI_JWT_ALGORITHMS",
    "VGI_RPC_GRANT_KEYS",
    "VGI_OAUTH_RESOURCE",
    "VGI_OAUTH_AUTH_SERVERS",
    "VGI_PROXY_PROOF_MODE",
    "VGI_INTROSPECT_PRINCIPALS",
    "VGI_FIXTURE_IDENTITY",
    "VGI_FIXTURE_TEST_BEARERS",
    "VGI_ORCHARD_HOSTED",
    "VGI_CORS_ORIGIN",
    "VGI_RPC_MAX_REQUEST_BYTES",
)


def _worker_binary() -> Path:
    build = Path(os.environ.get("VGI_C_BUILD", ROOT / "build"))
    return Path(
        os.environ.get(
            "VGI_WORKER_BIN", build / "example-worker" / "vgi-example-worker"
        )
    )


WORKER = _worker_binary()


# ---------------------------------------------------------------------------
# Passes
# ---------------------------------------------------------------------------


def _pass(
    key: ECKey | RSAKey | OctKey = EC,
    *,
    alg: str = "ES256",
    typ: str = "at+jwt",
    **overrides: Any,
) -> str:
    now = int(time.time())
    claims: dict[str, Any] = {
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": "user_01M7KNJYC18T",
        "kind": "user",
        "email": "ada@example.com",
        "email_verified": True,
        "name": "Ada Lovelace",
        "org_id": "org_01M7",
        "org_name": "Acme Analytics",
        "federated_iss": "https://token.actions.githubusercontent.com",
        "federated_sub": "repo:acme/etl:ref:refs/heads/main",
        "client_id": "vgi-duckdb",
        "iat": now,
        "nbf": now,
        "exp": now + 900,
        "jti": f"jti-{time.monotonic_ns()}",
    }
    claims.update(overrides)
    claims = {name: value for name, value in claims.items() if value is not None}
    header = {"alg": alg, "typ": typ, "kid": key.kid}
    return jwt.encode(header, claims, key, algorithms=[alg], default_type=None)


def _tamper(token: str, **changes: Any) -> str:
    header, payload, signature = token.split(".")
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    claims.update(changes)
    forged = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=").decode()
    return f"{header}.{forged}.{signature}"


# ---------------------------------------------------------------------------
# A controllable loopback JWKS endpoint
# ---------------------------------------------------------------------------


@dataclass
class _Jwks:
    url: str
    keys: list[Any] = field(default_factory=lambda: [EC, RSA])
    status: int = 200
    cache_control: str | None = None
    requests: int = 0

    def body(self) -> bytes:
        return json.dumps(
            {"keys": [k.as_dict(private=False) for k in self.keys]}
        ).encode()


def _serve_jwks() -> tuple[_Jwks, ThreadingHTTPServer]:
    state = _Jwks(url="")

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            state.requests += 1
            body = state.body() if state.status == 200 else b"{}"
            self.send_response(state.status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            if state.cache_control:
                self.send_header("Cache-Control", state.cache_control)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    state.url = f"http://127.0.0.1:{server.server_address[1]}/jwks"
    return state, server


@pytest.fixture(scope="module")
def jwks() -> Iterator[_Jwks]:
    state, server = _serve_jwks()
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()


def _env(jwks_url: str | None, **overrides: str | None) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k not in _AUTH_ENV}
    env.update(
        {
            "VGI_ORCHARD_HOSTED": "1",
            "VGI_JWT_ISSUER": ISSUER,
            "VGI_JWT_AUDIENCE": AUDIENCE,
            "VGI_JWT_JWKS_URI": jwks_url or "",
            "VGI_SIGNING_KEY": SIGNING_KEY,
        }
    )
    for key, value in overrides.items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value
    return env


@dataclass
class _Worker:
    url: str
    port: int
    proc: subprocess.Popen[str]
    stderr: Path
    access_log: Path


def _start(env: dict[str, str], tmp: Path) -> _Worker:
    stderr = tmp / "worker.stderr"
    access_log = tmp / "access.jsonl"
    with stderr.open("w") as err:
        proc = subprocess.Popen(
            [str(WORKER), "--http", "0", "--access-log", str(access_log)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=err,
            text=True,
        )
    assert proc.stdout is not None
    deadline = time.monotonic() + 30
    port = None
    while time.monotonic() < deadline:
        line = proc.stdout.readline()
        if not line:
            break
        if line.startswith("PORT:"):
            port = int(line.strip().split(":", 1)[1])
            break
    if port is None:
        proc.kill()
        pytest.fail(f"hosted worker did not start: {stderr.read_text()}")
    threading.Thread(target=lambda: proc.stdout.read(), daemon=True).start()  # type: ignore[union-attr]
    return _Worker(f"http://127.0.0.1:{port}", port, proc, stderr, access_log)


def _stop(worker: _Worker) -> None:
    worker.proc.terminate()
    try:
        worker.proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        worker.proc.kill()


@pytest.fixture(scope="module")
def worker(jwks: _Jwks, tmp_path_factory: pytest.TempPathFactory) -> Iterator[_Worker]:
    w = _start(_env(jwks.url), tmp_path_factory.mktemp("orchard"))
    try:
        yield w
    finally:
        _stop(w)


def _request(
    worker: _Worker,
    method: str,
    path: str,
    token: str | None = None,
    headers: dict[str, str] | None = None,
    data: bytes | None = None,
) -> tuple[int, dict[str, str], bytes]:
    if data is None and method == "POST":
        data = b""
    req = urllib.request.Request(worker.url + path, method=method, data=data)
    for name, value in (headers or {}).items():
        req.add_header(name, value)
    if token is not None:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return (
                resp.status,
                {k.lower(): v for k, v in resp.headers.items()},
                resp.read(),
            )
    except urllib.error.HTTPError as err:
        return err.code, {k.lower(): v for k, v in err.headers.items()}, err.read()


def _raw(
    worker: _Worker, head: str, body: bytes = b"", timeout: float = 10
) -> tuple[int, dict[str, str], float]:
    """Send a raw request; return the status, headers and seconds to the reply."""
    started = time.monotonic()
    with socket.create_connection(("127.0.0.1", worker.port), timeout=timeout) as sock:
        sock.sendall(head.encode() + body)
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = sock.recv(65536)
            if not chunk:
                break
            data += chunk
    elapsed = time.monotonic() - started
    lines = data.split(b"\r\n\r\n", 1)[0].decode("latin-1").split("\r\n")
    status = int(lines[0].split()[1])
    headers = {}
    for line in lines[1:]:
        name, _, value = line.partition(":")
        headers[name.strip().lower()] = value.strip()
    return status, headers, elapsed


def _claims(worker: _Worker, token: str) -> dict[str, Any]:
    with Client.from_http(worker.url, bearer_token=token) as client:
        out = list(
            client.scalar_function(
                function_name="pass_claims",
                schema_path=["main"],
                arguments=Arguments(),
                input=iter(
                    (pa.record_batch({"x": pa.array([1, 2], type=pa.int64())}),)
                ),
            )
        )
    column = out[0].column("result")
    assert len(column) == 2
    payload: dict[str, Any] = json.loads(column[0].as_py())
    return payload


_USED_TOKENS: list[str] = []


def _use(token: str) -> str:
    _USED_TOKENS.append(token)
    return token


def _assert_401(status: int, headers: dict[str, str]) -> None:
    assert status == 401
    assert headers.get("www-authenticate") == 'Bearer error="invalid_token"'
    assert "resource_metadata" not in headers.get("www-authenticate", "")
    assert headers.get("vgi-auth-reason") in {
        "missing_credential",
        "invalid_credential",
        "expired_credential",
    }
    assert "vgi-supported-encodings" in headers
    assert "vgi-accept-max-response-bytes-support" in headers
    assert headers.get("access-control-allow-origin") == "*"
    # PROTOCOL_VERSIONS_V1 §4.1: hosted 401s advertise the windows too.
    assert "vgi.v3=3.4-3.4" in headers.get("vgi-protocol-versions", ""), headers


# ---------------------------------------------------------------------------
# A running hosted worker
# ---------------------------------------------------------------------------


class TestHostedWorker:
    """Requests against a real hosted-mode worker."""

    def test_valid_pass_reaches_function_with_json_types(self, worker: _Worker) -> None:
        token = _use(_pass())
        result = _claims(worker, token)
        assert result["principal"] == "user_01M7KNJYC18T"
        assert result["domain"] == "orchard"
        claims = result["claims"]
        assert claims["email_verified"] is True
        assert isinstance(claims["exp"], int)
        assert isinstance(claims["iat"], int) and isinstance(claims["nbf"], int)
        for name, value in {
            "sub": "user_01M7KNJYC18T",
            "kind": "user",
            "email": "ada@example.com",
            "name": "Ada Lovelace",
            "org_id": "org_01M7",
            "org_name": "Acme Analytics",
            "federated_iss": "https://token.actions.githubusercontent.com",
            "federated_sub": "repo:acme/etl:ref:refs/heads/main",
            "client_id": "vgi-duckdb",
            "iss": ISSUER,
            "aud": AUDIENCE,
        }.items():
            assert claims[name] == value
        assert claims["jti"].startswith("jti-")

    def test_pass_claims_matches_python_json(self, worker: _Worker) -> None:
        """The row text is what vgi-python's fixture writes (json.dumps, sort_keys)."""
        token = _use(_pass(name='Zoë "Z" 😀', aud=[AUDIENCE], weight=1.5))
        with Client.from_http(worker.url, bearer_token=token) as client:
            out = list(
                client.scalar_function(
                    function_name="pass_claims",
                    schema_path=["main"],
                    arguments=Arguments(),
                    input=iter(
                        (pa.record_batch({"x": pa.array([1], type=pa.int64())}),)
                    ),
                )
            )
        text = out[0].column("result")[0].as_py()
        payload = token.split(".")[1]
        claims = json.loads(
            base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
        )
        expected = json.dumps(
            {"principal": "user_01M7KNJYC18T", "domain": "orchard", "claims": claims},
            sort_keys=True,
        )
        assert text == expected

    def test_request_cap_defaults_to_32_mib(self, worker: _Worker) -> None:
        """Hosted mode advertises a 32 MiB request cap when none is configured."""
        _, headers, _ = _request(worker, "HEAD", "/health")
        assert headers.get("vgi-max-request-bytes") == str(32 * 1024 * 1024)

    def test_request_cap_from_environment(self, jwks: _Jwks, tmp_path: Path) -> None:
        w = _start(_env(jwks.url, VGI_RPC_MAX_REQUEST_BYTES="65536"), tmp_path)
        try:
            _, headers, _ = _request(w, "HEAD", "/health")
            assert headers.get("vgi-max-request-bytes") == "65536"
            head = "POST /vgi.v3/catalog_attach HTTP/1.1\r\nHost: x\r\nContent-Length: 65537\r\n"
            status, _, _ = _raw(
                w, head + f"Authorization: Bearer {_use(_pass())}\r\n\r\n"
            )
            assert status == 413
        finally:
            _stop(w)

    def test_head_health_is_anonymous(self, worker: _Worker) -> None:
        status, headers, _ = _request(worker, "HEAD", "/health")
        assert status == 200
        assert "vgi-supported-encodings" in headers

    @pytest.mark.parametrize(
        ("method", "path"),
        [
            ("GET", "/health"),
            ("OPTIONS", "/health"),
            ("GET", "/"),
            ("GET", "/describe"),
            ("GET", "/no/such/path"),
            ("HEAD", "/"),
            ("POST", "/vgi.v3/catalog_attach"),
            ("POST", "/no/such/path"),
            ("DELETE", "/__session__"),
        ],
    )
    def test_no_pass_401(self, worker: _Worker, method: str, path: str) -> None:
        status, headers, _ = _request(
            worker, method, path, headers={"Origin": "https://shell.example.com"}
        )
        _assert_401(status, headers)
        if method != "HEAD":
            assert headers["vgi-auth-reason"] == "missing_credential"

    def test_authenticated_simple_requests(self, worker: _Worker) -> None:
        token = _use(_pass())
        status, _, body = _request(worker, "GET", "/health", token)
        assert status == 200 and json.loads(body)["status"] == "ok"
        assert _request(worker, "GET", "/", token)[0] == 404
        assert _request(worker, "GET", "/no/such/path", token)[0] == 404
        assert _request(worker, "OPTIONS", "/health", token)[0] == 200

    @pytest.mark.parametrize(
        "case",
        [
            "garbage",
            "tampered",
            "wrong_aud",
            "multi_aud",
            "expired",
            "wrong_typ",
            "rs256",
            "hs256",
            "wrong_iss",
            "no_exp",
            "future_nbf",
            "unknown_kid",
        ],
    )
    def test_bad_pass_401(self, worker: _Worker, case: str) -> None:
        now = int(time.time())
        makers: dict[str, Callable[[], str]] = {
            "garbage": lambda: "not-a-jwt",
            "tampered": lambda: _tamper(_pass(), org_id="org_EVIL"),
            "wrong_aud": lambda: _pass(aud="https://desk.orchard.test/d/dep_other"),
            "multi_aud": lambda: _pass(
                aud=[AUDIENCE, "https://desk.orchard.test/d/dep_other"]
            ),
            "expired": lambda: _pass(iat=now - 1200, nbf=now - 1200, exp=now - 120),
            "wrong_typ": lambda: _pass(typ="JWT"),
            "rs256": lambda: _pass(RSA, alg="RS256"),
            "hs256": lambda: _pass(
                OctKey.import_key(b"k" * 32, {"kid": "desk-es256-1"}), alg="HS256"
            ),
            "wrong_iss": lambda: _pass(iss="https://evil.test"),
            "no_exp": lambda: _pass(exp=None),
            "future_nbf": lambda: _pass(nbf=now + 600),
            "unknown_kid": lambda: _pass(EC_ROTATED),
        }
        token = makers[case]()
        status, headers, _ = _request(
            worker,
            "POST",
            "/vgi.v3/catalog_attach",
            _use(token),
            headers={"Origin": "https://shell.example.com"},
        )
        _assert_401(status, headers)
        expected = "expired_credential" if case == "expired" else "invalid_credential"
        assert headers["vgi-auth-reason"] == expected

    def test_preflight_open_cors(self, worker: _Worker) -> None:
        status, headers, _ = _request(
            worker,
            "OPTIONS",
            "/vgi.v3/catalog_attach",
            headers={
                "Origin": "https://shell.example.com",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "authorization",
            },
        )
        assert status == 200
        assert headers.get("access-control-allow-origin") == "*"
        assert (
            "authorization" in headers.get("access-control-allow-headers", "").lower()
        )

    def test_auth_before_body_size(self, worker: _Worker) -> None:
        """A declared body over the limit gets 401 at once, unread; with a Pass, 413."""
        head = "POST /vgi.v3/catalog_attach HTTP/1.1\r\nHost: x\r\nContent-Type: application/vnd.apache.arrow.stream\r\nContent-Length: 10000000000\r\n"
        status, headers, elapsed = _raw(worker, head + "\r\n")
        _assert_401(status, headers)
        assert elapsed < 5
        assert headers.get("connection", "").lower() == "close"
        status, headers, _ = _raw(
            worker, head + f"Authorization: Bearer {_use(_pass())}\r\n\r\n"
        )
        assert status == 413
        status, headers, _ = _raw(
            worker,
            head + "Authorization: Bearer not-a-jwt\r\nExpect: 100-continue\r\n\r\n",
        )
        _assert_401(status, headers)

    def test_auth_before_body_read(self, worker: _Worker) -> None:
        """A refused request's body is never read: the reply needs none of it."""
        head = (
            "POST /vgi.v3/catalog_attach HTTP/1.1\r\nHost: x\r\n"
            "Content-Type: application/vnd.apache.arrow.stream\r\nContent-Length: 1000\r\n\r\n"
        )
        status, headers, elapsed = _raw(worker, head)  # the 1000 bytes never come
        _assert_401(status, headers)
        assert elapsed < 5
        status, headers, _ = _raw(
            worker,
            "POST /vgi.v3/catalog_attach HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n",
        )
        _assert_401(status, headers)

    def test_auth_before_decoding(self, worker: _Worker) -> None:
        """A body that cannot be decoded is still a 401 without a Pass."""
        status, headers, _ = _request(
            worker,
            "POST",
            "/vgi.v3/catalog_attach",
            headers={
                "Content-Type": "application/vnd.apache.arrow.stream",
                "Content-Encoding": "zstd",
            },
            data=b"\x28\xb5\x2f\xfd garbage",
        )
        _assert_401(status, headers)
        status, headers, _ = _request(
            worker,
            "POST",
            "/vgi.v3/catalog_attach",
            headers={"Content-Type": "text/plain"},
            data=b"x",
        )
        _assert_401(status, headers)

    def test_zz_authorization_never_logged(self, worker: _Worker) -> None:
        _claims(worker, _use(_pass()))
        for bad in (
            _tamper(_pass(), sub="mallory"),
            _pass(aud="https://other.test"),
            _pass(typ="JWT"),
        ):
            status, _headers, _body = _request(
                worker, "POST", "/vgi.v3/catalog_attach", _use(bad)
            )
            assert status == 401
        time.sleep(0.5)
        log = worker.access_log.read_text()
        assert '"status_code":401' in log, "expected access-log records"
        assert '"principal":"user_01M7KNJYC18T"' in log
        text = log + worker.stderr.read_text()
        for token in _USED_TOKENS:
            assert token not in text
            if token.count(".") == 2:
                assert token.split(".")[2] not in text
        assert "Bearer " not in text


class TestJwksCache:
    """Key-set behaviour visible over HTTP."""

    def test_outage_is_503_never_stale(self, tmp_path: Path) -> None:
        state, server = _serve_jwks()
        try:
            state.status = 500
            w = _start(_env(state.url), tmp_path)
            try:
                token = _use(_pass())
                status, headers, _ = _request(
                    w, "POST", "/vgi.v3/catalog_attach", token
                )
                assert status == 503
                assert headers.get("retry-after")
                assert "www-authenticate" not in headers
                # A missing Pass is decided without keys.
                status, headers, _ = _request(w, "POST", "/vgi.v3/catalog_attach")
                _assert_401(status, headers)
                state.status = 200
                assert _claims(w, token)["principal"] == "user_01M7KNJYC18T"
            finally:
                _stop(w)
        finally:
            server.shutdown()
            server.server_close()

    def test_unknown_kid_refetch_is_rate_limited(self, tmp_path: Path) -> None:
        state, server = _serve_jwks()
        try:
            w = _start(_env(state.url), tmp_path)
            try:
                assert _claims(w, _use(_pass()))["domain"] == "orchard"
                assert state.requests == 1
                rotated = _use(_pass(EC_ROTATED))
                # First unknown kid inside 60 s of the last fetch: refused, no fetch.
                status, headers, _ = _request(
                    w, "POST", "/vgi.v3/catalog_attach", rotated
                )
                _assert_401(status, headers)
                assert state.requests == 1
                state.keys = [EC, EC_ROTATED, RSA]
                status, _, _ = _request(w, "POST", "/vgi.v3/catalog_attach", rotated)
                assert status == 401 and state.requests == 1
            finally:
                _stop(w)
        finally:
            server.shutdown()
            server.server_close()


# ---------------------------------------------------------------------------
# Startup refusals
# ---------------------------------------------------------------------------


def _refused(env: dict[str, str], *argv: str) -> str:
    args = list(argv) if argv else ["--http", "0"]
    proc = subprocess.run(
        [str(WORKER), *args], env=env, capture_output=True, text=True, timeout=60
    )
    assert proc.returncode != 0, (
        f"worker started when it should have refused:\n{proc.stdout}\n{proc.stderr}"
    )
    assert "PORT:" not in proc.stdout
    return proc.stderr


_JWKS = "http://127.0.0.1:9/jwks"  # never contacted: startup refuses first


class TestStartupRefusals:
    """Each misconfiguration stops the worker with a clear error."""

    @pytest.mark.parametrize(
        "name", ["VGI_JWT_ISSUER", "VGI_JWT_AUDIENCE", "VGI_JWT_JWKS_URI"]
    )
    def test_unset(self, name: str) -> None:
        assert name in _refused(_env(_JWKS, **{name: None}))

    @pytest.mark.parametrize(
        "name", ["VGI_JWT_ISSUER", "VGI_JWT_AUDIENCE", "VGI_JWT_JWKS_URI"]
    )
    def test_empty(self, name: str) -> None:
        assert name in _refused(_env(_JWKS, **{name: "  "}))

    @pytest.mark.parametrize(
        "name", ["VGI_JWT_ISSUER", "VGI_JWT_AUDIENCE", "VGI_JWT_JWKS_URI"]
    )
    def test_comma(self, name: str) -> None:
        stderr = _refused(_env(_JWKS, **{name: "https://a.test,https://b.test"}))
        assert name in stderr
        assert "exactly one" in stderr

    def test_signing_key_unset(self) -> None:
        assert "VGI_SIGNING_KEY" in _refused(_env(_JWKS, VGI_SIGNING_KEY=None))

    def test_signing_key_short(self) -> None:
        stderr = _refused(_env(_JWKS, VGI_SIGNING_KEY="short-key"))
        assert "VGI_SIGNING_KEY" in stderr
        assert "32 bytes" in stderr

    def test_signing_key_minted(self) -> None:
        env = _env(_JWKS, VGI_SIGNING_KEY="minted-by-a-parent-vgi-serve-process-0001")
        env["VGI_SIGNING_KEY_MINTED"] = "1"
        assert "VGI_SIGNING_KEY" in _refused(env)

    @pytest.mark.parametrize(
        "name", ["VGI_BEARER_TOKENS", "VGI_OPTIONAL_BEARER_TOKENS"]
    )
    def test_bearer_tokens(self, name: str) -> None:
        assert name in _refused(_env(_JWKS, **{name: "tok=alice"}))

    def test_grant_keys_env(self) -> None:
        key = base64.b64encode(bytes(range(32))).decode()
        assert "VGI_RPC_GRANT_KEYS" in _refused(_env(_JWKS, VGI_RPC_GRANT_KEYS=key))

    def test_grant_keys_flag(self) -> None:
        key = base64.b64encode(bytes(range(32))).decode()
        assert (
            "grant" in _refused(_env(_JWKS), "--http", "0", "--grant-key", key).lower()
        )

    def test_resolve_token_hook(self) -> None:
        env = _env(_JWKS, VGI_FIXTURE_IDENTITY="1", VGI_INTROSPECT_PRINCIPALS="ops")
        assert "resolve_token" in _refused(env)

    def test_custom_authenticate(self) -> None:
        assert "authenticate" in _refused(_env(_JWKS, VGI_FIXTURE_TEST_BEARERS="1"))

    def test_oauth_resource(self) -> None:
        stderr = _refused(
            _env(
                _JWKS,
                VGI_OAUTH_RESOURCE="https://x.test",
                VGI_OAUTH_AUTH_SERVERS=ISSUER,
            )
        )
        assert "VGI_OAUTH_RESOURCE" in stderr

    def test_proxy_proof(self) -> None:
        assert "VGI_PROXY_PROOF_MODE" in _refused(
            _env(_JWKS, VGI_PROXY_PROOF_MODE="require")
        )

    @pytest.mark.parametrize("value", ["ES256,HS256", "none", "HS512"])
    def test_symmetric_algorithm_override(self, value: str) -> None:
        assert "VGI_JWT_ALGORITHMS" in _refused(_env(_JWKS, VGI_JWT_ALGORITHMS=value))

    def test_unsupported_algorithm_override(self) -> None:
        assert "EdDSA" in _refused(_env(_JWKS, VGI_JWT_ALGORITHMS="EdDSA"))

    def test_unrecognised_flag_value(self) -> None:
        assert "VGI_ORCHARD_HOSTED" in _refused(
            _env(_JWKS, VGI_ORCHARD_HOSTED="yes please")
        )

    @pytest.mark.parametrize(
        "argv",
        [
            [],
            ["--tcp", "127.0.0.1:0"],
            ["--unix", "/tmp/vgi-c-orchard-hosted-refuse.sock"],
        ],
    )
    def test_unauthenticated_transports(self, argv: list[str]) -> None:
        proc = subprocess.run(
            [str(WORKER), *argv],
            env=_env(_JWKS),
            input="",
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert proc.returncode != 0
        assert "only --http" in proc.stderr

    @pytest.mark.parametrize("value", ["0", "false", "", "off", " OFF "])
    def test_off_values(self, value: str) -> None:
        """Hosted mode off: the worker serves stdio as usual (no refusal)."""
        proc = subprocess.run(
            [str(WORKER)],
            env=_env(_JWKS, VGI_ORCHARD_HOSTED=value),
            input="",
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert "Orchard hosted mode" not in proc.stderr
        assert proc.returncode == 0, proc.stderr
