"""Errors from the sync client carry no unrelated asyncio context.

On Python 3.14, asyncio.get_event_loop() raises RuntimeError when no loop is
set, which is the normal case in a script. The sync client must not raise the
operation's error while handling that RuntimeError, or the traceback ends with
"During handling of the above exception, another exception occurred".
"""

import asyncio

import pytest

from datalab_sdk import DatalabClient
from datalab_sdk.exceptions import DatalabNotFoundError


def _no_current_loop():
    raise RuntimeError("There is no current event loop in thread 'MainThread'.")


async def _not_found():
    raise DatalabNotFoundError("Request not found", 404)


def test_an_error_with_no_event_loop_has_no_asyncio_context(monkeypatch):
    monkeypatch.setattr(asyncio, "get_event_loop", _no_current_loop)
    client = DatalabClient(api_key="k")
    with pytest.raises(DatalabNotFoundError) as info:
        client._run_async(_not_found())
    assert info.value.__context__ is None
    assert info.value.__cause__ is None


def test_a_public_method_error_with_no_event_loop_has_no_asyncio_context(
    monkeypatch,
):
    monkeypatch.setattr(asyncio, "get_event_loop", _no_current_loop)
    client = DatalabClient(api_key="k")

    async def presets():
        raise DatalabNotFoundError("Request not found", 404)

    monkeypatch.setattr(client._async_client, "get_redaction_presets", presets)
    with pytest.raises(DatalabNotFoundError) as info:
        client.get_redaction_presets()
    assert info.value.__context__ is None


def test_an_installed_idle_loop_runs_the_operation_and_stays_installed():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:

        async def which_loop():
            return asyncio.get_running_loop()

        assert DatalabClient(api_key="k")._run_async(which_loop()) is loop
        assert not loop.is_closed()
        assert asyncio.get_event_loop() is loop
    finally:
        loop.close()
        asyncio.set_event_loop(None)


def test_a_closed_loop_runs_the_operation_on_a_new_loop():
    async def which_loop():
        return asyncio.get_running_loop()

    for operation in ("result", "error"):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.close()
        try:
            if operation == "result":
                ran_on = DatalabClient(api_key="k")._run_async(which_loop())
                assert ran_on is not loop
            else:
                with pytest.raises(DatalabNotFoundError) as info:
                    DatalabClient(api_key="k")._run_async(_not_found())
                assert info.value.__context__ is None
        finally:
            asyncio.set_event_loop(None)


def test_no_event_loop_under_warnings_as_errors_runs_the_operation(monkeypatch):
    # Python 3.12 and 3.13 warn here; -W error::DeprecationWarning raises it.
    def warning_as_error():
        raise DeprecationWarning("There is no current event loop")

    monkeypatch.setattr(asyncio, "get_event_loop", warning_as_error)

    async def answer():
        return 42

    client = DatalabClient(api_key="k")
    assert client._run_async(answer()) == 42
    with pytest.raises(DatalabNotFoundError) as info:
        client._run_async(_not_found())
    assert info.value.__context__ is None
