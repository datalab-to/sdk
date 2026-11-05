"""Regression checks for public redaction and bounded storage uploads."""

import asyncio
import base64
import io
from unittest.mock import AsyncMock, patch

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from datalab_sdk import AsyncDatalabClient, DatalabClient, RedactOptions
from datalab_sdk.exceptions import DatalabFileError, DatalabValidationError


@pytest.mark.asyncio
@pytest.mark.parametrize("failed", [False, True])
async def test_redaction_wire_contract_and_verification(tmp_path, failed):
    received = {}
    pdf = b"%PDF-1.4 redacted"

    async def submit(request):
        received.update(await request.post())
        return web.json_response(
            {
                "success": True,
                "request_id": "red_1",
                "request_check_url": "/api/v1/redact/red_1",
            }
        )

    async def result(request):
        return web.json_response(
            {
                "status": "complete",
                "success": not failed,
                "verdict": "FAIL" if failed else "PASS",
                "withheld": False,
                "failing_checks": [{"id": "review"}] if failed else [],
                "output_base64": base64.b64encode(pdf).decode(),
                "pages_rewritten": [0],
                "targets_removed_count": 2,
            }
        )

    async def presets(request):
        return web.json_response({"presets": [{"id": "personal_information"}]})

    app = web.Application()
    app.router.add_post("/api/v1/redact", submit)
    app.router.add_get("/api/v1/redact/presets", presets)
    app.router.add_get("/api/v1/redact/red_1", result)
    async with TestServer(app) as server:
        async with AsyncDatalabClient(
            api_key="secret", base_url=str(server.make_url(""))
        ) as client:
            progress = []
            redacted = await client.redact(
                file_url="https://example.org/input.pdf",
                options=RedactOptions(redaction_spec={"goal": "Remove identifiers"}),
                save_output=tmp_path / "redacted",
                on_progress=progress.append,
            )
            assert await client.get_redaction_presets() == {
                "presets": [{"id": "personal_information"}]
            }
    assert received["preset"] == "personal_information"
    assert received["redaction_spec"] == '{\n  "goal": "Remove identifiers"\n}'
    assert set(received) == {"file_url", "preset", "redaction_spec", "skip_cache"}
    assert redacted.request_id == "red_1"
    assert redacted.success is not failed
    assert redacted.verdict == ("FAIL" if failed else "PASS")
    assert redacted.failing_checks == ([{"id": "review"}] if failed else [])
    assert redacted.pages_rewritten == [0]
    assert (tmp_path / "redacted.pdf").read_bytes() == pdf
    assert progress == ([] if failed else [100])


@pytest.mark.asyncio
async def test_upload_streams_and_bounds_concurrency_in_order(tmp_path):
    paths = [tmp_path / f"{index}.pdf" for index in range(7)]
    for path in paths:
        path.write_bytes((path.name * 100).encode())
    active = 0
    peak = 0
    connections = set()
    received = {}

    async def sign(request):
        data = await request.json()
        name = data["filename"]
        assert data["processing_location"] == "eu"
        return web.json_response(
            {
                "file_id": name,
                "upload_url": str(server.make_url(f"/storage/{name}")),
                "reference": f"datalab://{name}",
            }
        )

    async def upload(request):
        nonlocal active, peak
        assert "X-Api-Key" not in request.headers
        assert request.content_type == "application/pdf"
        connections.add(request.transport)
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        received[request.match_info["name"]] = await request.read()
        active -= 1
        return web.Response()

    async def confirm(request):
        return web.json_response({"created": None})

    app = web.Application()
    app.router.add_post("/api/v1/files/upload", sign)
    app.router.add_put("/storage/{name}", upload)
    app.router.add_get("/api/v1/files/{file_id}/confirm", confirm)
    async with TestServer(app) as server:
        async with AsyncDatalabClient(
            api_key="secret", base_url=str(server.make_url(""))
        ) as client:
            # File uploads must use file handles rather than copying whole files.
            original_put = __import__("aiohttp").ClientSession.put

            def check_put(session, url, **kwargs):
                assert isinstance(kwargs["data"], io.BufferedIOBase)
                assert session.timeout.total == client.timeout
                return original_put(session, url, **kwargs)

            with patch("aiohttp.ClientSession.put", new=check_put):
                result = await client.upload_files(
                    paths, processing_location="eu", max_concurrency=2
                )
    assert [item.original_filename for item in result] == [p.name for p in paths]
    assert received == {path.name: path.read_bytes() for path in paths}
    assert peak == 2
    assert len(connections) <= 2


@pytest.mark.asyncio
async def test_upload_failure_cancels_other_workers(tmp_path):
    started = asyncio.Event()
    stopped = asyncio.Event()
    client = AsyncDatalabClient(api_key="k")

    async def upload(path, *args, **kwargs):
        if path == "fail":
            await started.wait()
            raise DatalabFileError("broken upload")
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    with patch.object(client, "_upload_single_file", new=upload):
        with pytest.raises(DatalabFileError, match="broken upload"):
            await client.upload_files(["fail", "waiting", "never"], max_concurrency=2)
    assert stopped.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
async def test_upload_rejects_invalid_concurrency(limit):
    client = AsyncDatalabClient(api_key="k")
    with pytest.raises(DatalabValidationError, match="max_concurrency"):
        await client.upload_files([], max_concurrency=limit)


def test_sync_operation_runtime_error_propagates_once():
    client = DatalabClient(api_key="k")
    calls = 0

    async def fail():
        nonlocal calls
        calls += 1
        raise RuntimeError("operation failed")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        with pytest.raises(RuntimeError, match="^operation failed$"):
            client._run_async(fail())
    finally:
        loop.close()
        asyncio.set_event_loop(None)
    assert calls == 1


@pytest.mark.asyncio
async def test_sync_client_in_async_loop_gives_actionable_error():
    with pytest.raises(RuntimeError, match="use AsyncDatalabClient"):
        DatalabClient(api_key="k").get_redaction_presets()


def test_sync_redaction_forwards_options_and_progress():
    client = DatalabClient(api_key="k")
    options = RedactOptions(preset="financial_details")

    def progress(value):
        return None

    with patch.object(client._async_client, "redact", new_callable=AsyncMock) as redact:
        client.redact(
            file_url="https://example.org/a.pdf", options=options, on_progress=progress
        )
    assert redact.await_args.kwargs["options"] is options
    assert redact.await_args.kwargs["on_progress"] is progress


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "verdict,has_pdf,upstream_success,expected_success",
    [
        ("PASS", True, True, True),
        ("FAIL", True, True, False),
        ("FAIL", False, False, False),
        ("PASS", True, False, False),
        (None, True, True, False),
    ],
)
async def test_regional_redaction_normalizes_signed_worker_result(
    tmp_path, verdict, has_pdf, upstream_success, expected_success
):
    pdf = b"%PDF-1.4 regional output"
    received = {}
    progress = []

    async def submit(request):
        received.update(await request.post())
        assert request.headers["X-Api-Key"] == "secret"
        return web.json_response(
            {"success": True, "request_id": "regional", "request_check_url": "/check"}
        )

    async def check(request):
        return web.json_response(
            {
                "status": "complete",
                "success": upstream_success,
                "result_url": str(server.make_url("/signed")),
                "expires_in": 600,
                "page_count": 2,
                "progress_percent": 100,
            }
        )

    async def signed(request):
        assert "X-Api-Key" not in request.headers
        return web.json_response(
            {
                "success": upstream_success,
                "output_shape": "redact",
                "verdict": verdict,
                "redacted_document": base64.b64encode(pdf).decode()
                if has_pdf
                else None,
                "verifier_metrics": [
                    {
                        "id": "pii",
                        "status": "fail" if verdict != "PASS" else "pass",
                        "severity": "blocking",
                        "detail": "private document text",
                    },
                    {"id": "advice", "status": "fail", "severity": "advisory"},
                ],
                "targets_removed_count": 3,
                "targets_not_found_count": 1,
                "targets_removed": [{"text": "private document text"}],
                "pages_rewritten": [0],
                "error": "private document text",
            }
        )

    app = web.Application()
    app.router.add_post("/api/v1/redact", submit)
    app.router.add_get("/check", check)
    app.router.add_get("/signed", signed)
    async with TestServer(app) as server:
        async with AsyncDatalabClient(
            api_key="secret", base_url=str(server.make_url(""))
        ) as client:
            result = await client.redact(
                file_url="https://example.org/input.pdf",
                options=RedactOptions(processing_location="eu"),
                save_output=tmp_path / "regional.pdf",
                on_progress=progress.append,
            )
    assert received["processing_location"] == "eu"
    assert result.request_id == "regional"
    assert result.result_url.endswith("/signed")
    assert result.expires_in == 600
    assert result.page_count == 2
    assert result.status == "complete"
    assert result.verdict == verdict
    assert result.success is expected_success
    assert result.withheld is not has_pdf
    assert result.failing_checks == ([{"id": "pii"}] if verdict != "PASS" else [])
    assert result.targets_removed_count == 3
    assert result.pages_rewritten == [0]
    assert "private document text" not in repr(result)
    assert progress == ([100] if expected_success else [])
    assert result.progress_percent == (100 if expected_success else None)
    if has_pdf:
        assert (tmp_path / "regional.pdf").read_bytes() == pdf
    else:
        assert not (tmp_path / "regional.pdf").exists()


def test_redaction_helpers_do_not_offer_unverified_streaming():
    import inspect

    for method in (AsyncDatalabClient.redact, DatalabClient.redact):
        assert "stream_response_to" not in inspect.signature(method).parameters


@pytest.mark.parametrize("worker_shape", [False, True])
def test_redaction_rejects_contradictory_verdict_and_counts_legacy_targets(
    worker_shape,
):
    data = {
        "status": "complete",
        "success": True,
        "verdict": "PASS",
        "progress_percent": 100,
        "targets_removed": [{"text": "private name"}],
        "targets_not_found": ["private address", "private number"],
    }
    if worker_shape:
        data.update(
            redacted_document="cGRm",
            verifier_metrics=[{"id": "pii", "severity": "blocking", "status": "fail"}],
        )
    else:
        data.update(output_base64="cGRm", failing_checks=[{"id": "pii"}])
    result = AsyncDatalabClient._normalize_redaction_result(data)
    assert result["success"] is False
    assert result["progress_percent"] is None
    assert result["withheld"] is False
    assert result["failing_checks"] == [{"id": "pii"}]
    assert result["targets_removed_count"] == 1
    assert result["targets_not_found_count"] == 2
    assert "private name" not in repr(result)
    assert "private address" not in repr(result)
