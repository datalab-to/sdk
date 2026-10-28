"""Sign in from a terminal with a device code (`datalab login`), and keep the API key on disk.

The flow (server side: /api/auth/device/code, /app/device, /api/auth/device/token):
1. Ask the server for a code. Show the human the code and a URL, and open the browser.
2. The human approves in the browser, where they are signed in as usual.
3. Poll until the server returns a new API key, then save it to the config file.

The key is saved to $XDG_CONFIG_HOME/datalab/config.json (default ~/.config/datalab/config.json)
with mode 0600. DATALAB_API_KEY in the environment always wins over the saved key.
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
from typing import Callable, Optional

from datalab_sdk.exceptions import DatalabAPIError
from datalab_sdk.settings import settings

KEYS_PAGE = "https://www.datalab.to/app/keys"


def config_path() -> Path:
    base = os.environ.get("XDG_CONFIG_HOME") or os.path.join(Path.home(), ".config")
    return Path(base) / "datalab" / "config.json"


def load_saved_api_key() -> Optional[str]:
    try:
        return json.loads(config_path().read_text()).get("api_key") or None
    except (OSError, ValueError):
        return None


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
