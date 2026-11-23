"""Exercise poll and result-download retries over real local HTTP connections."""

import asyncio
import json
from contextlib import asynccontextmanager

import ijson
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from datalab_sdk import AsyncDatalabClient, ConvertOptions
from datalab_sdk import client as client_module
from datalab_sdk.exceptions import DatalabAPIError, DatalabTimeoutError


# --- Local HTTP fixture helpers ---
@pytest.fixture(autouse=True)
def _no_retry_delay(monkeypatch):
    """Keep native transport regressions independent of backoff duration."""
    for method in (
        AsyncDatalabClient._poll_get_streaming,
        AsyncDatalabClient._fetch_signed_result,
        AsyncDatalabClient._poll_get_with_retry,
    ):
        monkeypatch.setattr(method.retry, "wait", lambda _: 0)


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


@pytest.mark.asyncio
async def test_stream_body_timeout_is_not_retried(tmp_path, monkeypatch):
    """A timeout while the body streams fails once, as DatalabTimeoutError.

    The timeout limits the whole request, so a result too large to stream
    within it would time out on every attempt. Before, this surfaced as a
    raw CancelledError.
    """
    monkeypatch.setattr(client_module.tempfile, "tempdir", str(tmp_path))
    destination = tmp_path / "result.json"
    counts = {"polls": 0}
    complete = {"status": "complete", "success": True, "markdown": "full result"}

    async def submit(request):
        await request.read()
        return web.json_response(
            {"success": True, "request_id": "local", "request_check_url": "/check"}
        )

    async def result(request):
        counts["polls"] += 1
        if counts["polls"] == 1:
            response = web.StreamResponse(
                headers={"Content-Type": "application/json", "Content-Length": "1000"}
            )
            await response.prepare(request)
            await response.write(b'{"status":"complete","success":true,"markdown":"')
            await asyncio.sleep(2)
            return response
        return web.json_response(complete)

    app = web.Application()
    app.router.add_post("/api/v1/convert", submit)
    app.router.add_get("/check", result)
    retry_errors = []
    monkeypatch.setattr(
        AsyncDatalabClient._poll_get_streaming.retry,
        "before_sleep",
        lambda state: retry_errors.append(state.outcome.exception()),
    )
    async with TestServer(app) as server:
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=str(server.make_url("")), timeout=0.5
        ) as client:
            with pytest.raises(DatalabTimeoutError, match="Result transfer timed out"):
                await client.convert(
                    file_url="https://example.invalid/local.pdf",
                    stream_response_to=destination,
                    max_polls=1,
                    poll_interval=0,
                )
    assert counts["polls"] == 1
    assert retry_errors == []
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_stalled_processing_body_is_retried(tmp_path, monkeypatch):
    """A body that stalls before the request is complete is an ordinary poll
    timeout: the request has not finished, so the SDK polls again."""
    monkeypatch.setattr(client_module.tempfile, "tempdir", str(tmp_path))
    destination = tmp_path / "result.json"
    counts = {"polls": 0}
    complete = {"status": "complete", "success": True, "markdown": "full result"}

    async def submit(request):
        await request.read()
        return web.json_response(
            {"success": True, "request_id": "local", "request_check_url": "/check"}
        )

    async def result(request):
        counts["polls"] += 1
        if counts["polls"] == 1:
            response = web.StreamResponse(
                headers={"Content-Type": "application/json", "Content-Length": "1000"}
            )
            await response.prepare(request)
            await response.write(b'{"status":"processing","progress_percent":')
            await asyncio.sleep(2)
            return response
        return web.json_response(complete)

    app = web.Application()
    app.router.add_post("/api/v1/convert", submit)
    app.router.add_get("/check", result)
    async with TestServer(app) as server:
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=str(server.make_url("")), timeout=0.5
        ) as client:
            result = await client.convert(
                file_url="https://example.invalid/local.pdf",
                stream_response_to=destination,
                max_polls=1,
                poll_interval=0,
            )
    assert result.success
    assert counts["polls"] == 2
    assert json.loads(destination.read_text()) == complete


@pytest.mark.asyncio
async def test_stalled_inline_result_is_retried_as_a_poll_timeout(monkeypatch):
    """Without stream_response_to, the SDK reads the whole poll body before it
    sees status, so a body timeout is an ordinary poll timeout and is retried,
    even when the result had finished. This is a known gap: a finished inline
    result too large to read within timeout is read up to 10 times. It is not
    a DatalabTransferTimeoutError, which means the request finished."""
    counts = {"polls": 0}
    complete = {"status": "complete", "success": True, "markdown": "full result"}
    retry_errors = []
    monkeypatch.setattr(
        AsyncDatalabClient._poll_get_with_retry.retry,
        "before_sleep",
        lambda state: retry_errors.append(state.outcome.exception()),
    )

    async def submit(request):
        await request.read()
        return web.json_response(
            {"success": True, "request_id": "local", "request_check_url": "/check"}
        )

    async def result(request):
        counts["polls"] += 1
        if counts["polls"] == 1:
            response = web.StreamResponse(
                headers={"Content-Type": "application/json", "Content-Length": "1000"}
            )
            await response.prepare(request)
            await response.write(b'{"status":"complete","success":true,"markdown":"')
            await asyncio.sleep(2)
            return response
        return web.json_response(complete)

    app = web.Application()
    app.router.add_post("/api/v1/convert", submit)
    app.router.add_get("/check", result)
    async with TestServer(app) as server:
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=str(server.make_url("")), timeout=0.5
        ) as client:
            result = await client.convert(
                file_url="https://example.invalid/local.pdf",
                max_polls=1,
                poll_interval=0,
            )
    assert result.markdown == "full result"
    assert counts["polls"] == 2
    assert [type(e) for e in retry_errors] == [DatalabTimeoutError]


@asynccontextmanager
async def _signed_download_server(first_download):
    """Serve a signed result whose first download fails as first_download says."""
    counts = {"downloads": 0}

    async def submit(request):
        await request.read()
        return web.json_response(
            {"success": True, "request_id": "local", "request_check_url": "/check"}
        )

    async def result(request):
        return web.json_response(
            {
                "status": "complete",
                "success": True,
                "page_count": 3,
                "result_url": str(server.make_url("/signed")),
            }
        )

    async def download(request):
        counts["downloads"] += 1
        if counts["downloads"] == 1:
            if first_download == "disconnect":
                response = web.StreamResponse(headers={"Content-Length": "1000"})
                await response.prepare(request)
                await response.write(b'{"markdown":"partial')
                request.transport.close()
                return response
            if first_download == "cut_403":
                response = web.StreamResponse(
                    status=403, headers={"Content-Length": "1000"}
                )
                await response.prepare(request)
                await response.write(b"<Error>")
                request.transport.close()
                return response
            if first_download == "503_retry_after":
                return web.Response(status=503, headers={"Retry-After": "7"})
            return web.Response(status=first_download)
        return web.json_response({"markdown": "full result", "page_count": 1})

    app = web.Application()
    app.router.add_post("/api/v1/convert", submit)
    app.router.add_get("/check", result)
    app.router.add_get("/signed", download)
    async with TestServer(app) as server:
        yield str(server.make_url("")), counts


@pytest.mark.asyncio
@pytest.mark.parametrize("first_download", ["disconnect", 503])
async def test_non_streamed_signed_download_is_retried(first_download):
    async with _signed_download_server(first_download) as (url, counts):
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=url
        ) as client:
            result = await client.convert(
                file_url="https://example.invalid/local.pdf",
                max_polls=1,
                poll_interval=0,
            )
    assert counts["downloads"] == 2
    assert result.markdown == "full result"
    # The poll response's fields still take priority over the download.
    assert result.page_count == 3
    assert result.request_id == "local"


@pytest.mark.asyncio
async def test_non_streamed_expired_signed_url_is_not_retried():
    async with _signed_download_server(403) as (url, counts):
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=url
        ) as client:
            with pytest.raises(DatalabAPIError) as error:
                await client.convert(
                    file_url="https://example.invalid/local.pdf",
                    max_polls=1,
                    poll_interval=0,
                )
    assert error.value.status_code == 403
    assert counts["downloads"] == 1


@pytest.mark.asyncio
async def test_stalled_error_body_keeps_its_status(tmp_path, monkeypatch):
    """A 404 whose body stalls past the timeout fails at once, not as a timeout."""
    monkeypatch.setattr(client_module.tempfile, "tempdir", str(tmp_path))
    counts = {"polls": 0}

    async def submit(request):
        await request.read()
        return web.json_response(
            {"success": True, "request_id": "local", "request_check_url": "/check"}
        )

    async def result(request):
        counts["polls"] += 1
        response = web.StreamResponse(
            status=404,
            headers={"Content-Type": "application/json", "Content-Length": "1000"},
        )
        await response.prepare(request)
        await response.write(b'{"detail":')
        await asyncio.sleep(2)
        return response

    app = web.Application()
    app.router.add_post("/api/v1/convert", submit)
    app.router.add_get("/check", result)
    async with TestServer(app) as server:
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=str(server.make_url("")), timeout=0.5
        ) as client:
            with pytest.raises(DatalabAPIError) as error:
                await client.convert(
                    file_url="https://example.invalid/local.pdf",
                    stream_response_to=tmp_path / "result.json",
                    max_polls=1,
                    poll_interval=0,
                )
    assert error.value.status_code == 404
    assert counts["polls"] == 1


@pytest.mark.asyncio
async def test_signed_download_with_cut_off_403_body_is_not_retried():
    async with _signed_download_server("cut_403") as (url, counts):
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=url
        ) as client:
            with pytest.raises(DatalabAPIError) as error:
                await client.convert(
                    file_url="https://example.invalid/local.pdf",
                    max_polls=1,
                    poll_interval=0,
                )
    assert error.value.status_code == 403
    assert counts["downloads"] == 1


@pytest.mark.asyncio
async def test_signed_download_honors_retry_after(monkeypatch):
    waits = []
    monkeypatch.setattr(
        AsyncDatalabClient._fetch_signed_result.retry,
        "before_sleep",
        lambda state: waits.append(client_module._wait_retry_after_or_backoff(state)),
    )
    async with _signed_download_server("503_retry_after") as (url, counts):
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=url
        ) as client:
            result = await client.convert(
                file_url="https://example.invalid/local.pdf",
                max_polls=1,
                poll_interval=0,
            )
    assert result.markdown == "full result"
    assert counts["downloads"] == 2
    assert waits == [7]


@pytest.mark.asyncio
async def test_non_streamed_signed_download_timeout_is_not_retried():
    counts = {"downloads": 0}

    async def submit(request):
        await request.read()
        return web.json_response(
            {"success": True, "request_id": "local", "request_check_url": "/check"}
        )

    async def result(request):
        return web.json_response(
            {
                "status": "complete",
                "success": True,
                "result_url": str(server.make_url("/signed")),
            }
        )

    async def download(request):
        counts["downloads"] += 1
        response = web.StreamResponse(headers={"Content-Length": "1000"})
        await response.prepare(request)
        await response.write(b'{"markdown":"')
        await asyncio.sleep(2)
        return response

    app = web.Application()
    app.router.add_post("/api/v1/convert", submit)
    app.router.add_get("/check", result)
    app.router.add_get("/signed", download)
    async with TestServer(app) as server:
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=str(server.make_url("")), timeout=0.5
        ) as client:
            with pytest.raises(DatalabTimeoutError):
                await client.convert(
                    file_url="https://example.invalid/local.pdf",
                    max_polls=1,
                    poll_interval=0,
                )
    assert counts["downloads"] == 1
