"""Which API key the SDK and the CLI send, and to which host.

The order is: an explicit key, then DATALAB_API_KEY, then the key saved by
`datalab login`, and the saved key only for the host it was saved for.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from click.testing import CliRunner

from datalab_sdk import auth
from datalab_sdk.cli import cli
from datalab_sdk.client import AsyncDatalabClient, DatalabClient
from datalab_sdk.exceptions import DatalabAPIError
from datalab_sdk.settings import settings


class KeyRecorder:
    """A local API that records the X-Api-Key of each request and rejects it."""

    def __init__(self):
        self.keys = []
        recorder = self

        class Handler(BaseHTTPRequestHandler):
            def _reject(self):
                recorder.keys.append(self.headers.get("X-Api-Key"))
                length = int(self.headers.get("Content-Length") or 0)
                self.rfile.read(length)
                body = json.dumps({"detail": "Invalid API key"}).encode()
                self.send_response(401)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            do_GET = do_POST = _reject

            def log_message(self, *args):
                pass

        self.httpd = HTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


@pytest.fixture
def no_keys(tmp_path, monkeypatch):
    """No saved key and no DATALAB_API_KEY, whatever the developer's machine has."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setattr(settings, "DATALAB_API_KEY", None)
    return tmp_path


@pytest.fixture
def api():
    server = KeyRecorder()
    yield server
    server.close()


def _sent_key(base_url, **kwargs):
    return DatalabClient(base_url=base_url, **kwargs)._async_client.api_key


# The SDK client


def test_client_uses_a_saved_key_for_its_own_host(no_keys):
    auth.save_api_key("key_saved", "https://www.datalab.to")
    assert _sent_key("https://www.datalab.to") == "key_saved"
    assert _sent_key("https://www.datalab.to/") == "key_saved"
    assert _sent_key("HTTPS://WWW.DATALAB.TO:443") == "key_saved"


@pytest.mark.parametrize(
    "base_url",
    [
        "https://staging.datalab.to",
        "http://www.datalab.to",
        "https://www.datalab.to:8443",
        "https://www.datalab.to.example.com",
        "http://localhost:8000",
    ],
)
def test_client_never_sends_a_saved_key_to_another_host(no_keys, base_url):
    auth.save_api_key("key_saved", "https://www.datalab.to")
    with pytest.raises(DatalabAPIError) as info:
        DatalabClient(base_url=base_url)
    # The error says why the saved key was not used, and how to get one for this host.
    assert "https://www.datalab.to" in str(info.value)
    assert "datalab login --host" in str(info.value)
    assert "key_saved" not in str(info.value)


def test_async_client_never_sends_a_saved_key_to_another_host(no_keys):
    auth.save_api_key("key_saved", "https://www.datalab.to")
    with pytest.raises(DatalabAPIError):
        AsyncDatalabClient(base_url="https://staging.datalab.to")


@pytest.mark.parametrize(
    "saved_host,base_url",
    [
        ("www.datalab.to", "other-host:8080"),
        ("www.datalab.to", "www.datalab.to"),
        (None, "other-host:8080"),
        (123, "https://www.datalab.to"),
    ],
)
def test_saved_key_with_an_unparseable_host_is_never_sent(
    no_keys, saved_host, base_url
):
    path = auth.config_path()
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"api_key": "key_saved", "host": saved_host}))
    with pytest.raises(DatalabAPIError):
        DatalabClient(base_url=base_url)


def test_mismatch_error_shows_origins_without_credentials(no_keys):
    auth.save_api_key("key_saved", "https://www.datalab.to")
    with pytest.raises(DatalabAPIError) as info:
        DatalabClient(base_url="https://user:s3cret@staging.datalab.to/api?x=1")
    message = str(info.value)
    assert "s3cret" not in message and "user" not in message
    assert "datalab login --host https://staging.datalab.to`" in message


def test_an_empty_explicit_key_is_an_error(no_keys, monkeypatch):
    auth.save_api_key("key_saved", "https://www.datalab.to")
    monkeypatch.setattr(settings, "DATALAB_API_KEY", "key_env")
    with pytest.raises(DatalabAPIError, match="empty"):
        DatalabClient(api_key="", base_url="https://www.datalab.to")


@pytest.mark.parametrize("blank", ["", " ", "\t\n"])
def test_an_empty_environment_key_counts_as_unset(no_keys, monkeypatch, blank):
    auth.save_api_key("key_saved", "https://www.datalab.to")
    monkeypatch.setattr(settings, "DATALAB_API_KEY", blank)
    assert _sent_key("https://www.datalab.to") == "key_saved"


def test_saved_key_without_a_host_is_not_sent(no_keys):
    path = auth.config_path()
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"api_key": "key_saved"}))
    with pytest.raises(DatalabAPIError):
        DatalabClient(base_url="https://www.datalab.to")


def test_client_key_precedence_with_every_source_set(no_keys, monkeypatch):
    host = "https://www.datalab.to"
    auth.save_api_key("key_saved", host)
    monkeypatch.setattr(settings, "DATALAB_API_KEY", "key_env")
    assert _sent_key(host, api_key="key_explicit") == "key_explicit"
    assert _sent_key(host) == "key_env"
    # An environment key goes to any host: the caller chose it for this process.
    assert _sent_key("https://staging.datalab.to") == "key_env"


# The CLI


def _run(args):
    return CliRunner().invoke(cli, args, catch_exceptions=False)


@pytest.fixture
def document(tmp_path):
    path = tmp_path / "inputs" / "doc.pdf"
    path.parent.mkdir()
    path.write_bytes(b"%PDF-1.4\n%%EOF\n")
    return path


def test_cli_convert_uses_a_saved_key_for_its_host(no_keys, api, document, tmp_path):
    auth.save_api_key("key_saved", api.url)
    result = _run(
        [
            "convert",
            str(document),
            "--base_url",
            api.url,
            "--output_dir",
            str(tmp_path / "out"),
        ]
    )
    assert "No API key" not in result.output, result.output
    assert api.keys and set(api.keys) == {"key_saved"}


def test_cli_create_document_uses_a_saved_key_for_its_host(no_keys, api, tmp_path):
    auth.save_api_key("key_saved", api.url)
    _run(
        [
            "create-document",
            "--markdown",
            "# Title",
            "-o",
            str(tmp_path / "out.docx"),
            "--base_url",
            api.url,
        ]
    )
    assert api.keys and set(api.keys) == {"key_saved"}


def test_cli_compare_get_uses_a_saved_key_for_its_host(no_keys, api):
    auth.save_api_key("key_saved", api.url)
    _run(["compare", "get", "1", "--base_url", api.url])
    assert api.keys and set(api.keys) == {"key_saved"}


def test_cli_never_sends_a_saved_key_to_another_host(no_keys, api, document, tmp_path):
    auth.save_api_key("key_saved", "https://www.datalab.to")
    result = _run(
        [
            "convert",
            str(document),
            "--base_url",
            api.url,
            "--output_dir",
            str(tmp_path / "out"),
        ]
    )
    assert result.exit_code == 1
    assert api.keys == []
    assert "datalab login --host" in result.output


def test_cli_key_precedence_with_every_source_set(no_keys, api, monkeypatch, tmp_path):
    auth.save_api_key("key_saved", api.url)
    monkeypatch.setattr(settings, "DATALAB_API_KEY", "key_env")
    args = ["create-document", "--markdown", "# T", "-o", str(tmp_path / "o.docx")]
    _run(args + ["--base_url", api.url, "--api_key", "key_explicit"])
    _run(args + ["--base_url", api.url])
    assert api.keys == ["key_explicit", "key_env"]


def test_whitespace_around_a_key_is_removed(no_keys, monkeypatch):
    host = "https://www.datalab.to"
    assert _sent_key(host, api_key=" key_explicit\n") == "key_explicit"
    monkeypatch.setattr(settings, "DATALAB_API_KEY", "key_env\n")
    assert _sent_key(host) == "key_env"


def test_cli_empty_api_key_is_an_error_not_a_fallback(
    no_keys, api, monkeypatch, tmp_path
):
    auth.save_api_key("key_saved", api.url)
    monkeypatch.setattr(settings, "DATALAB_API_KEY", "key_env")
    result = _run(
        [
            "create-document",
            "--markdown",
            "# T",
            "-o",
            str(tmp_path / "o.docx"),
            "--base_url",
            api.url,
            "--api_key",
            "",
        ]
    )
    assert result.exit_code == 1
    assert "The API key passed is empty" in result.output
    assert api.keys == []


@pytest.mark.parametrize("blank", ["", " ", "\t\n"])
def test_cli_blank_environment_key_uses_the_saved_key(
    no_keys, api, monkeypatch, tmp_path, blank
):
    auth.save_api_key("key_saved", api.url)
    monkeypatch.setattr(settings, "DATALAB_API_KEY", blank)
    _run(
        [
            "create-document",
            "--markdown",
            "# T",
            "-o",
            str(tmp_path / "o.docx"),
            "--base_url",
            api.url,
        ]
    )
    assert api.keys == ["key_saved"]
