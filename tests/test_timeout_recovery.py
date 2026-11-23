"""Timeout errors say what happened to the request, and get_result() resumes it."""

import asyncio
import json
from contextlib import asynccontextmanager

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from datalab_sdk import (
    AgentOptions,
    AgentResult,
    AsyncDatalabClient,
    ConversionResult,
    ConvertOptions,
    DatalabAPIError,
    DatalabPollingTimeoutError,
    DatalabTimeoutError,
    DatalabTransferTimeoutError,
    DatalabValidationError,
    FileResult,
    FormFillingOptions,
)
from datalab_sdk import client as client_module


@pytest.fixture(autouse=True)
def _no_retry_delay(monkeypatch):
    for method in (
        AsyncDatalabClient._poll_get_streaming,
        AsyncDatalabClient._fetch_signed_result,
        AsyncDatalabClient._poll_get_with_retry,
    ):
        monkeypatch.setattr(method.retry, "wait", lambda _: 0)


@asynccontextmanager
async def _server(
    *, polls_until_complete=1, signed_delay=None, submit_delay=None, failed=False
):
    """A local API: /api/v1/{route} submits, /api/v1/{route}/{id} polls."""
    counts = {"posts": 0, "polls": 0, "downloads": 0, "paths": []}

    async def submit(request):
        counts["posts"] += 1
        await request.read()
        if submit_delay:
            await asyncio.sleep(submit_delay)
        route = request.match_info["route"]
        return web.json_response(
            {
                "success": True,
                "request_id": "req-1",
                "request_check_url": str(server.make_url(f"/api/v1/{route}/req-1")),
            }
        )

    async def poll(request):
        counts["polls"] += 1
        counts["paths"].append(request.path)
        if failed:
            return web.json_response(
                {"status": "failed", "success": False, "error": "bad document"}
            )
        if counts["polls"] < polls_until_complete:
            return web.json_response(
                {"status": "processing", "success": True, "progress_percent": 40}
            )
        if signed_delay is not None:
            return web.json_response(
                {
                    "status": "complete",
                    "success": True,
                    "result_url": str(server.make_url("/signed")),
                }
            )
        return web.json_response(
            {
                "status": "complete",
                "success": True,
                "markdown": "# done",
                "output_shape": "markdown",
                "page_count": 2,
            }
        )

    async def download(request):
        counts["downloads"] += 1
        response = web.StreamResponse(headers={"Content-Length": "1000"})
        await response.prepare(request)
        await response.write(b'{"markdown":"')
        await asyncio.sleep(signed_delay)
        return response

    app = web.Application()
    app.router.add_post("/api/v1/{route}", submit)
    app.router.add_get("/api/v1/{route}/{request_id}", poll)
    app.router.add_get("/signed", download)
    async with TestServer(app) as server:
        yield str(server.make_url("")), counts


# --- Part 1: a slow signed download is not retried ---


@pytest.mark.asyncio
@pytest.mark.parametrize("streamed", [True, False])
async def test_slow_signed_download_makes_one_poll_and_one_download(
    tmp_path, monkeypatch, streamed
):
    monkeypatch.setattr(client_module.tempfile, "tempdir", str(tmp_path))
    destination = tmp_path / "result.json"
    async with _server(signed_delay=2) as (url, counts):
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=url, timeout=0.5
        ) as client:
            with pytest.raises(DatalabTransferTimeoutError) as info:
                await client.convert(
                    file_url="https://example.invalid/local.pdf",
                    stream_response_to=destination if streamed else None,
                    max_polls=1,
                    poll_interval=0,
                )
    assert (counts["posts"], counts["polls"], counts["downloads"]) == (1, 1, 1)
    assert info.value.request_id == "req-1"
    assert info.value.check_url.endswith("/api/v1/convert/req-1")
    assert not destination.exists()
    assert list(tmp_path.iterdir()) == []


def test_timeout_errors_are_timeout_errors():
    assert issubclass(DatalabTransferTimeoutError, DatalabTimeoutError)
    assert issubclass(DatalabPollingTimeoutError, DatalabTimeoutError)


# --- Part 2: errors carry the request, and get_result() resumes it ---


@pytest.mark.asyncio
async def test_polling_timeout_says_the_request_is_still_running():
    async with _server(polls_until_complete=3) as (url, counts):
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=url
        ) as client:
            with pytest.raises(DatalabPollingTimeoutError) as info:
                await client.convert(
                    file_url="https://example.invalid/local.pdf",
                    max_polls=1,
                    poll_interval=0,
                )
            error = info.value
            assert error.request_id == "req-1"
            assert error.check_url.endswith("/api/v1/convert/req-1")
            assert error.status == "processing"
            assert error.progress_percent == 40

            result = await client.get_result(
                error.check_url, max_polls=5, poll_interval=0
            )
    assert isinstance(result, ConversionResult)
    assert result.markdown == "# done"
    assert result.request_id == "req-1"
    assert counts["posts"] == 1
    assert counts["polls"] == 3


@pytest.mark.asyncio
async def test_get_result_by_request_id_returns_the_endpoint_result_type():
    async with _server() as (url, counts):
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=url
        ) as client:
            result = await client.get_result("req-1", endpoint="agent", poll_interval=0)
    assert isinstance(result, AgentResult)
    assert result.output_shape == "markdown"
    assert result.request_id == "req-1"
    assert counts["paths"] == ["/api/v1/agent/req-1"]
    assert counts["posts"] == 0


@pytest.mark.asyncio
async def test_get_result_streams_to_a_file(tmp_path, monkeypatch):
    monkeypatch.setattr(client_module.tempfile, "tempdir", str(tmp_path))
    destination = tmp_path / "result.json"
    async with _server() as (url, _):
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=url
        ) as client:
            result = await client.get_result(
                "req-1",
                endpoint="convert",
                stream_response_to=destination,
                poll_interval=0,
            )
    assert isinstance(result, FileResult)
    assert result.request_id == "req-1"
    assert result.page_count == 2
    assert json.loads(destination.read_text())["markdown"] == "# done"


@pytest.mark.asyncio
async def test_get_result_sends_a_check_url_to_the_client_base_url():
    """The API key never goes to the host named in a pasted check URL."""
    async with _server() as (url, counts):
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=url
        ) as client:
            result = await client.get_result(
                "https://elsewhere.invalid/api/v1/convert/req-1", poll_interval=0
            )
    assert result.success
    assert counts["paths"] == ["/api/v1/convert/req-1"]


@pytest.mark.asyncio
async def test_failed_request_error_carries_the_request_id():
    async with _server(failed=True) as (url, _):
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=url
        ) as client:
            with pytest.raises(DatalabAPIError, match="bad document") as info:
                await client.convert(
                    file_url="https://example.invalid/local.pdf", poll_interval=0
                )
    assert info.value.request_id == "req-1"


@pytest.mark.asyncio
async def test_submit_timeout_has_no_request_id():
    async with _server(submit_delay=2) as (url, counts):
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=url, timeout=0.5
        ) as client:
            with pytest.raises(DatalabTimeoutError) as info:
                await client.convert(file_url="https://example.invalid/local.pdf")
    assert type(info.value) is DatalabTimeoutError
    assert info.value.request_id is None
    assert counts["polls"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "args,kwargs,match",
    [
        (("req-1",), {}, "Pass endpoint"),
        (("req-1",), {"endpoint": "pipelines"}, "does not support"),
        (("/api/v1/convert/req-1",), {"endpoint": "convert"}, "Pass endpoint only"),
        (("https://www.datalab.to/other/req-1",), {}, "Not a request check URL"),
        (("req-1",), {"endpoint": "redact", "stream_response_to": "x.json"}, "stream"),
        (("/api/v1/marker/extraction/gen_schemas/req-1",), {}, "does not support"),
    ],
)
async def test_get_result_rejects_bad_arguments(args, kwargs, match):
    async with AsyncDatalabClient(api_key="local-test-only") as client:
        with pytest.raises(DatalabValidationError, match=match):
            await client.get_result(*args, **kwargs)


@pytest.mark.asyncio
async def test_pipeline_poll_error_carries_the_execution_id():
    """A run that was accepted must not look like a failed submission."""

    async def run(request):
        await request.read()
        return web.json_response({"execution_id": "pex_1", "status": "running"})

    async def execution(request):
        await asyncio.sleep(2)
        return web.json_response({"execution_id": "pex_1", "status": "running"})

    app = web.Application()
    app.router.add_post("/api/v1/pipelines/{pipeline_id}/run", run)
    app.router.add_get("/api/v1/pipelines/executions/{execution_id}", execution)
    async with TestServer(app) as server:
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=str(server.make_url("")), timeout=0.5
        ) as client:
            with pytest.raises(DatalabTimeoutError) as info:
                await client.run_pipeline(
                    "pl_1",
                    file_url="https://example.invalid/local.pdf",
                    max_polls=3,
                    poll_interval=0,
                )
    assert info.value.request_id == "pex_1"


# --- get_result() returns what the submitting method returns ---

_PDF = __import__("base64").b64encode(b"%PDF-1.4 redacted").decode()


@asynccontextmanager
async def _route_server(body):
    """Submit at /api/v1/{route}; every poll returns body."""

    async def submit(request):
        await request.read()
        route = request.match_info["route"]
        return web.json_response(
            {
                "success": True,
                "request_id": "req-1",
                "request_check_url": str(server.make_url(f"/api/v1/{route}/req-1")),
            }
        )

    async def poll(request):
        return web.json_response(body)

    app = web.Application()
    app.router.add_post("/api/v1/{route}", submit)
    app.router.add_get("/api/v1/{route}/{request_id}", poll)
    async with TestServer(app) as server:
        yield str(server.make_url(""))


_URL = "https://example.invalid/local.pdf"
_DONE = {"status": "complete", "success": True}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "route,body,submit",
    [
        (
            "convert",
            {**_DONE, "output_format": "json", "json": {"a": 1}},
            lambda c, out: c.convert(
                file_url=_URL,
                options=ConvertOptions(output_format="json"),
                save_output=out,
            ),
        ),
        (
            "track-changes",
            {**_DONE, "output_format": "markdown,html,chunks", "markdown": "# x"},
            lambda c, out: c.track_changes(file_url=_URL, save_output=out),
        ),
        (
            "agent",
            {**_DONE, "output_shape": "markdown", "markdown": "# x"},
            lambda c, out: c.run_agent(
                file_url=_URL, options=AgentOptions(processor="jats"), save_output=out
            ),
        ),
        (
            "create-document",
            {**_DONE, "output_format": "docx", "output_base64": _PDF},
            lambda c, out: c.create_document("# x", save_output=out),
        ),
        (
            "fill",
            {**_DONE, "output_format": "pdf", "output_base64": _PDF},
            lambda c, out: c.fill(
                file_url=_URL,
                options=FormFillingOptions(field_data={"name": {"value": "x"}}),
                save_output=out,
            ),
        ),
        (
            "form-fields",
            {**_DONE, "pages": [], "json": {"fields": []}},
            lambda c, out: c.form_fields(file_url=_URL, save_output=out),
        ),
        # A redacted PDF that failed verification is still saved.
        (
            "redact",
            {**_DONE, "redacted_document": _PDF, "verdict": "FAIL"},
            lambda c, out: c.redact(file_url=_URL, save_output=out),
        ),
    ],
)
async def test_get_result_matches_the_submitting_method(tmp_path, route, body, submit):
    async with _route_server(body) as url:
        async with AsyncDatalabClient(
            api_key="local-test-only", base_url=url
        ) as client:
            submitted = await submit(client, tmp_path / "submitted" / "out")
            resumed = await client.get_result(
                f"{url}/api/v1/{route}/req-1",
                save_output=tmp_path / "resumed" / "out",
                poll_interval=0,
            )
    assert resumed == submitted

    def saved(folder):
        return {
            p.relative_to(folder): p.read_bytes()
            for p in folder.rglob("*")
            if p.is_file()
        }

    assert saved(tmp_path / "submitted")
    assert saved(tmp_path / "submitted") == saved(tmp_path / "resumed")
