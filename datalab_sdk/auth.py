"""Sign in from a terminal with a device code (`datalab login`), and keep the API key on disk.

The flow (server side: /api/auth/device/code, /app/device, /api/auth/device/token):
1. Ask the server for a code. Show the human the code and a URL, and open the browser.
2. The human approves in the browser, where they are signed in as usual.
3. Poll until the server returns a new API key, then save it to the config file.

The key is saved to $XDG_CONFIG_HOME/datalab/config.json (default ~/.config/datalab/config.json)
with mode 0600, together with the host it was issued for. resolve_api_key() picks the key that the
SDK and the CLI send: an explicit key, then DATALAB_API_KEY, then the saved key, and the saved key
only to the host it was saved for.
"""

from __future__ import annotations

import json
import os
import stat
import time
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path
from typing import Callable, NamedTuple, Optional
from urllib.parse import urlsplit

from datalab_sdk.exceptions import DatalabAPIError
from datalab_sdk.settings import settings

KEYS_PAGE = "https://www.datalab.to/app/keys"


def config_path() -> Path:
    base = os.environ.get("XDG_CONFIG_HOME") or os.path.join(Path.home(), ".config")
    return Path(base) / "datalab" / "config.json"


def _load_config() -> dict:
    try:
        config = json.loads(config_path().read_text())
    except (OSError, ValueError):
        return {}
    return config if isinstance(config, dict) else {}


def load_saved_api_key() -> Optional[str]:
    """Return the saved key, whatever host it is for. Use resolve_api_key() to send one."""
    return _load_config().get("api_key") or None


_DEFAULT_PORTS = {"http": 80, "https": 443}


class _Origin(NamedTuple):
    """The scheme, host and port of a URL, with the default port filled in."""

    scheme: str
    host: str
    port: Optional[int]

    def __str__(self) -> str:
        """A URL without any user name, password, path or query."""
        host = f"[{self.host}]" if ":" in self.host else self.host
        default = _DEFAULT_PORTS.get(self.scheme)
        return f"{self.scheme}://{host}" + (
            "" if self.port == default else f":{self.port}"
        )


def _origin(url: str) -> Optional[_Origin]:
    try:
        parts = urlsplit(url.strip())
        port = parts.port
    except (AttributeError, ValueError):
        return None
    scheme = parts.scheme.lower()
    if not scheme or not parts.hostname:
        return None
    return _Origin(scheme, parts.hostname.lower(), port or _DEFAULT_PORTS.get(scheme))


def resolve_api_key(api_key: Optional[str], base_url: str) -> str:
    """Return the API key to send to base_url. Raises DatalabAPIError when there is none.

    The order is an explicit key, then DATALAB_API_KEY, then the key saved by
    `datalab login`. The saved key is used only when base_url has the same
    origin (scheme, host and port) as the host it was saved for, so it is never
    sent to another server. An empty or blank DATALAB_API_KEY counts as unset.
    Whitespace around a key, such as a newline from a .env file, is removed.

    This chooses the key for base_url only. Requests that base_url's server
    sends the client on to, such as check URLs and redirects, use the same
    key: that server already has it.
    """
    if api_key is not None:
        if not api_key.strip():
            raise DatalabAPIError("The API key passed is empty.")
        return api_key.strip()
    env_key = (settings.DATALAB_API_KEY or "").strip()
    if env_key:
        return env_key
    config = _load_config()
    saved_key = config.get("api_key")
    saved_host = config.get("host")
    saved_origin = _origin(saved_host) if isinstance(saved_host, str) else None
    target_origin = _origin(base_url)
    if saved_key and saved_origin is not None and saved_origin == target_origin:
        return saved_key
    if saved_key:
        target = str(target_origin) if target_origin else None
        login = (
            f"datalab login --host {target}" if target else "datalab login --host <url>"
        )
        raise DatalabAPIError(
            f"No API key for {target or 'this base_url'}. The key saved by "
            f"`datalab login` is for {saved_origin or 'an unknown host'}, "
            f"and is only sent to the same scheme, host and port. Run `{login}`, "
            "set DATALAB_API_KEY, or pass an API key."
        )
    raise DatalabAPIError(
        "No API key. Run `datalab login`, set DATALAB_API_KEY, or pass an API key. "
        f"Get a free API key at {KEYS_PAGE}"
    )


def save_api_key(api_key: str, host: str, team_name: Optional[str] = None) -> Path:
    path = config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"api_key": api_key, "host": host, "team_name": team_name}, indent=2)
    )
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    return path


def delete_saved_api_key() -> bool:
    try:
        config_path().unlink()
        return True
    except FileNotFoundError:
        return False


def _post(url: str, payload: dict, headers: dict) -> tuple[int, dict]:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": f"datalab-python-sdk/{settings.VERSION}",
            **headers,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as res:
            return res.status, json.loads(res.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except ValueError:
            return e.code, {}


def device_login(
    host: str = "https://www.datalab.to",
    client_name: str = "Datalab CLI",
    open_browser: bool = True,
    extra_headers: Optional[dict] = None,
    on_code: Optional[Callable[[dict], None]] = None,
    sleep: Callable[[float], None] = time.sleep,
) -> dict:
    """Run the device flow and return {"api_key", "team_name", "key_name"}. Raises DatalabAPIError."""
    host = host.rstrip("/")
    headers = extra_headers or {}
    status, code = _post(
        f"{host}/api/auth/device/code", {"client_name": client_name}, headers
    )
    if status != 200:
        raise DatalabAPIError(
            f"Could not start the sign-in ({status}): {code.get('detail', code)}"
        )
    if on_code:
        on_code(code)
    if open_browser:
        try:
            webbrowser.open(code["verification_uri_complete"])
        except Exception:
            pass

    interval = float(code.get("interval", 5))
    deadline = time.monotonic() + float(code.get("expires_in", 600))
    while time.monotonic() < deadline:
        sleep(interval)
        status, body = _post(
            f"{host}/api/auth/device/token",
            {"device_code": code["device_code"]},
            headers,
        )
        if status == 200 and body.get("api_key"):
            return body
        error = body.get("error")
        if error == "authorization_pending":
            continue
        if error == "slow_down":
            interval += 5
            continue
        if error == "access_denied":
            raise DatalabAPIError("The sign-in was denied in the browser.")
        if error == "expired_token":
            raise DatalabAPIError(
                "The sign-in code expired. Run `datalab login` again."
            )
        raise DatalabAPIError(f"Sign-in failed ({status}): {body}")
    raise DatalabAPIError("The sign-in code expired. Run `datalab login` again.")
