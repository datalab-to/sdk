"""`datalab login` (device code) against a small fake server, plus the saved-key fallback."""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from click.testing import CliRunner

from datalab_sdk import auth
from datalab_sdk.exceptions import DatalabAPIError


class FakeServer:
    """Answers /api/auth/device/code once, then `pending` polls, then the final reply."""

    def __init__(self, pending_polls=1, final=None):
        self.pending_polls = pending_polls
        self.final = final or (
            200,
            {"api_key": "key_123", "team_name": "Acme", "key_name": "Datalab CLI"},
        )
        self.polls = 0
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, status, body):
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length") or 0))
                if self.path == "/api/auth/device/code":
                    host = f"http://127.0.0.1:{outer.port}"
                    return self._send(
                        200,
                        {
                            "device_code": "dc",
                            "user_code": "BCDF-GHJK",
                            "verification_uri": f"{host}/app/device",
                            "verification_uri_complete": f"{host}/app/device?code=BCDF-GHJK",
                            "expires_in": 600,
                            "interval": 0,
                        },
                    )
                outer.polls += 1
                if outer.polls <= outer.pending_polls:
                    return self._send(400, {"error": "authorization_pending"})
                return self._send(*outer.final)

        self.httpd = HTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.httpd.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()


@pytest.fixture
def config_home(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    return tmp_path


def test_device_login_returns_key_after_pending():
    server = FakeServer(pending_polls=2)
    try:
        shown = []
        result = auth.device_login(
            host=server.url,
            open_browser=False,
            on_code=shown.append,
            sleep=lambda s: None,
        )
    finally:
        server.close()
    assert result["api_key"] == "key_123"
    assert shown[0]["user_code"] == "BCDF-GHJK"
    assert server.polls == 3


@pytest.mark.parametrize(
    "error,message", [("access_denied", "denied"), ("expired_token", "expired")]
)
def test_device_login_errors(error, message):
    server = FakeServer(pending_polls=0, final=(400, {"error": error}))
    try:
        with pytest.raises(DatalabAPIError, match=message):
            auth.device_login(host=server.url, open_browser=False, sleep=lambda s: None)
    finally:
        server.close()


def test_save_load_delete_key(config_home):
    path = auth.save_api_key("key_abc", "https://www.datalab.to", "Acme")
    assert path == config_home / "datalab" / "config.json"
    assert oct(path.stat().st_mode & 0o777) == "0o600"
    assert auth.load_saved_api_key() == "key_abc"
    assert auth.delete_saved_api_key() is True
    assert auth.load_saved_api_key() is None


def test_client_falls_back_to_saved_key(config_home, monkeypatch):
    from datalab_sdk.client import DatalabClient
    from datalab_sdk.settings import settings

    monkeypatch.setattr(settings, "DATALAB_API_KEY", None)
    auth.save_api_key("key_saved", "https://www.datalab.to")
    assert DatalabClient()._async_client.api_key == "key_saved"
    monkeypatch.setattr(settings, "DATALAB_API_KEY", "key_env")
    assert DatalabClient()._async_client.api_key == "key_env"


def test_client_error_names_login_and_keys_page(config_home, monkeypatch):
    from datalab_sdk.client import DatalabClient
    from datalab_sdk.settings import settings

    monkeypatch.setattr(settings, "DATALAB_API_KEY", None)
    with pytest.raises(DatalabAPIError, match="datalab login"):
        DatalabClient()


def test_cli_login_saves_key(config_home):
    from datalab_sdk.cli import cli

    server = FakeServer(pending_polls=0)
    try:
        res = CliRunner().invoke(cli, ["login", "--host", server.url, "--no-browser"])
    finally:
        server.close()
    assert res.exit_code == 0, res.output
    assert "BCDF-GHJK" in res.output and "Signed in to team Acme" in res.output
    assert auth.load_saved_api_key() == "key_123"
    out = CliRunner().invoke(cli, ["logout"])
    assert out.exit_code == 0 and auth.load_saved_api_key() is None
