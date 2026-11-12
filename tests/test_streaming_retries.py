"""Exercise streamed result retries over real local HTTP connections."""

import asyncio
import json
from contextlib import asynccontextmanager

import ijson
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from datalab_sdk import AsyncDatalabClient, ConvertOptions
from datalab_sdk import client as client_module
from datalab_sdk.exceptions import DatalabAPIError


# --- Local HTTP fixture helpers ---
@pytest.fixture(autouse=True)
def _no_retry_delay(monkeypatch):
    """Keep native transport regressions independent of backoff duration."""
    monkeypatch.setattr(
        AsyncDatalabClient._poll_get_streaming.retry, "wait", lambda _: 0
    )


@asynccontextmanager
async def _streaming_server(
    failure,
    destination,
    *,
    signed=False,
    body_started=None,
    status=200,
    persistent=False,
):
    """Keep transport failures reproducible without using a hosted provider."""
    counts = {"posts": 0, "polls": 0, "downloads": 0}
    complete = {"status": "complete", "success": True, "markdown": "full result"}

    async def submit(request):
        counts["posts"] += 1
        await request.read()
        return web.json_response(
            {"success": True, "request_id": "local", "request_check_url": "/check"}
        )

    async def result(request):
        counts["polls"] += 1
        assert destination.read_text() == "previous output"
        if signed:
            return web.json_response(
                {
                    "status": "complete",
                    "success": True,
                    "result_url": str(server.make_url("/signed")),
                }
            )
        if counts["polls"] == 1 or persistent:
            if failure == "http400":
                return web.json_response({"detail": "invalid request"}, status=400)
            if failure == "invalid_json":
                return web.Response(text='{"status":', content_type="application/json")
            response = web.StreamResponse(
                status=status,
                headers={
                    "Content-Type": "application/json",
                    "Content-Length": "1000",
                    "Retry-After": "7",
                },
            )
            await response.prepare(request)
            await response.write(b'{"status":"complete","success":true,"markdown":"')
            if body_started is not None:
                body_started.set()
                await asyncio.sleep(0.2)
            if request.transport is not None:
                request.transport.close()
            return response
        return web.json_response(complete)

    async def download(request):
        counts["downloads"] += 1
        assert "X-Api-Key" not in request.headers
        assert destination.read_text() == "previous output"
        if counts["downloads"] == 1:
            response = web.StreamResponse(headers={"Content-Length": "1000"})
            await response.prepare(request)
            await response.write(b'{"markdown":"partial')
            request.transport.close()
            return response
        return web.json_response(complete)

    app = web.Application()
    app.router.add_post("/api/v1/convert", submit)
    app.router.add_get("/check", result)
    app.router.add_get("/signed", download)
    async with TestServer(app) as server:
        yield str(server.make_url("")), counts, complete


# --- End local HTTP fixture helpers ---


@pytest.mark.asyncio
async def test_convert_retries_stream_body_transport_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(client_module.tempfile, "tempdir", str(tmp_path))
    destination = tmp_path / "result.json"
    destination.write_text("previous output")
    async with _streaming_server("disconnect", destination) as (url, counts, complete):
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=url
        ) as client:
            result = await client.convert(
                file_url="https://example.invalid/local.pdf",
                options=ConvertOptions(mode="balanced"),
                stream_response_to=destination,
                max_polls=1,
                poll_interval=0,
            )
    assert result.success
    assert counts == {"posts": 1, "polls": 2, "downloads": 0}
    assert json.loads(destination.read_text()) == complete
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [403, 404, 429, 503])
async def test_interrupted_error_body_preserves_retry_policy(
    tmp_path, monkeypatch, status
):
    monkeypatch.setattr(client_module.tempfile, "tempdir", str(tmp_path))
    destination = tmp_path / "result.json"
    destination.write_text("previous output")
    retry_errors = []
    retry_waits = []

    def record_retry(state):
        retry_errors.append(state.outcome.exception())
        retry_waits.append(client_module._wait_retry_after_or_backoff(state))

    monkeypatch.setattr(
        AsyncDatalabClient._poll_get_streaming.retry, "before_sleep", record_retry
    )
    async with _streaming_server("disconnect", destination, status=status) as (
        url,
        counts,
        complete,
    ):
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=url
        ) as client:
            conversion = client.convert(
                file_url="https://example.invalid/local.pdf",
                options=ConvertOptions(mode="balanced"),
                stream_response_to=destination,
                max_polls=1,
                poll_interval=0,
            )
            if status in (403, 404):
                with pytest.raises(DatalabAPIError) as error:
                    await conversion
                assert error.value.status_code == status
            else:
                await conversion

    assert counts["posts"] == 1
    if status in (403, 404):
        assert counts["polls"] == 1
        assert destination.read_text() == "previous output"
        assert retry_errors == []
    else:
        assert counts["polls"] == 2
        assert json.loads(destination.read_text()) == complete
        assert [error.status_code for error in retry_errors] == [status]
        assert retry_waits == [7]
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.asyncio
async def test_persistent_stream_failure_stops_at_existing_attempt_limit(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(client_module.tempfile, "tempdir", str(tmp_path))
    destination = tmp_path / "result.json"
    destination.write_text("previous output")
    async with _streaming_server("disconnect", destination, persistent=True) as (
        url,
        counts,
        _,
    ):
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=url
        ) as client:
            with pytest.raises(DatalabAPIError):
                await client.convert(
                    file_url="https://example.invalid/local.pdf",
                    options=ConvertOptions(mode="balanced"),
                    stream_response_to=destination,
                    max_polls=1,
                    poll_interval=0,
                )
    assert counts == {"posts": 1, "polls": 10, "downloads": 0}
    assert destination.read_text() == "previous output"
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.asyncio
async def test_convert_retries_interrupted_signed_stream(tmp_path, monkeypatch):
    monkeypatch.setattr(client_module.tempfile, "tempdir", str(tmp_path))
    destination = tmp_path / "result.json"
    destination.write_text("previous output")
    async with _streaming_server("disconnect", destination, signed=True) as (
        url,
        counts,
        complete,
    ):
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=url
        ) as client:
            result = await client.convert(
                file_url="https://example.invalid/local.pdf",
                options=ConvertOptions(mode="balanced"),
                stream_response_to=destination,
                max_polls=1,
                poll_interval=0,
            )
    assert result.success
    assert counts == {"posts": 1, "polls": 2, "downloads": 2}
    assert json.loads(destination.read_text()) == complete
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,error", [("http400", DatalabAPIError), ("invalid_json", ijson.JSONError)]
)
async def test_stream_rejects_nonretryable_response_without_replacing_output(
    tmp_path, monkeypatch, failure, error
):
    monkeypatch.setattr(client_module.tempfile, "tempdir", str(tmp_path))
    destination = tmp_path / "result.json"
    destination.write_text("previous output")
    async with _streaming_server(failure, destination) as (url, counts, _):
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=url
        ) as client:
            with pytest.raises(error):
                await client.convert(
                    file_url="https://example.invalid/local.pdf",
                    options=ConvertOptions(mode="balanced"),
                    stream_response_to=destination,
                    max_polls=1,
                    poll_interval=0,
                )
    assert counts == {"posts": 1, "polls": 1, "downloads": 0}
    assert destination.read_text() == "previous output"
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.asyncio
async def test_cancelled_stream_is_not_retried(tmp_path, monkeypatch):
    monkeypatch.setattr(client_module.tempfile, "tempdir", str(tmp_path))
    destination = tmp_path / "result.json"
    destination.write_text("previous output")
    body_started = asyncio.Event()
    async with _streaming_server(
        "disconnect", destination, body_started=body_started
    ) as (url, counts, _):
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=url
        ) as client:
            task = asyncio.create_task(
                client.convert(
                    file_url="https://example.invalid/local.pdf",
                    options=ConvertOptions(mode="balanced"),
                    stream_response_to=destination,
                    max_polls=1,
                    poll_interval=0,
                )
            )
            try:
                await asyncio.wait_for(body_started.wait(), timeout=2)
            finally:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
    assert counts == {"posts": 1, "polls": 1, "downloads": 0}
    assert destination.read_text() == "previous output"
    assert list(tmp_path.iterdir()) == [destination]
