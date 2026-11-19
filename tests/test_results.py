"""
Tests for signed result URLs, result fields and saved output
"""

import json
from unittest.mock import AsyncMock, patch

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from datalab_sdk import AsyncDatalabClient
from datalab_sdk.exceptions import (
    DatalabValidationError,
)
from datalab_sdk.models import ConversionResult, ConvertOptions, ExtractOptions


async def _serve(handlers):
    app = web.Application()
    for method, path, handler in handlers:
        app.router.add_route(method, path, handler)
    server = TestServer(app)
    await server.start_server()
    return server


def _base_url(server):
    return f"http://{server.host}:{server.port}"


@pytest.mark.asyncio
async def test_poll_result_downloads_signed_result_url():
    """The SDK fetches the full result when the API returns result_url"""
    seen_headers = {}

    async def signed(request):
        seen_headers.update(request.headers)
        return web.json_response(
            {"markdown": "# Full", "page_count": 1, "success": True}
        )

    server = await _serve([("GET", "/signed", signed)])
    try:
        async with AsyncDatalabClient(api_key="secret-key") as client:
            with patch.object(
                client, "_poll_get_with_retry", new_callable=AsyncMock
            ) as poll_get:
                poll_get.return_value = {
                    "status": "complete",
                    "success": True,
                    "result_url": f"{_base_url(server)}/signed",
                    "expires_in": 600,
                    "page_count": 2,
                    "markdown": None,
                }
                data = await client._poll_result("https://api/check", max_polls=1)
    finally:
        await server.close()

    assert data["markdown"] == "# Full"
    # Fields that the API response sets take priority.
    assert data["page_count"] == 2
    assert data["expires_in"] == 600
    # The signed URL must not receive the API key.
    assert "X-Api-Key" not in seen_headers


@pytest.mark.asyncio
async def test_streaming_downloads_signed_result_url(temp_dir):
    async def check(request):
        return web.json_response(
            {
                "status": "complete",
                "success": True,
                "result_url": f"{_base_url(server)}/signed",
            }
        )

    async def signed(request):
        return web.json_response({"status": "complete", "markdown": "# Full"})

    server = await _serve([("GET", "/check", check), ("GET", "/signed", signed)])
    out = temp_dir / "result.json"
    try:
        async with AsyncDatalabClient(api_key="k", base_url=_base_url(server)) as c:
            result = await c._poll_result("/check", max_polls=1, stream_response_to=out)
    finally:
        await server.close()
    assert result.status == "complete"
    assert json.loads(out.read_text())["markdown"] == "# Full"


_POLL_FIELDS = {
    "page_count": 4,
    "parse_quality_score": 4.25,
    "runtime": 1.5,
    "versions": {"marker": "1.2.3", "models": ["a", "b"]},
    "cost_breakdown": {
        "final_cost_cents": 12,
        "list_cost_cents": 16,
        "details": {"pages": 4},
    },
}


async def _streamed_convert(temp_dir, check_body, signed_body=None):
    async def submit(request):
        await request.read()
        return web.json_response(
            {"success": True, "request_id": "req_123", "request_check_url": "/check"}
        )

    async def check(request):
        body = dict(check_body)
        if signed_body is not None:
            body["result_url"] = f"{_base_url(server)}/signed"
        return web.json_response(body)

    async def signed(request):
        return web.json_response(signed_body)

    server = await _serve(
        [
            ("POST", "/api/v1/convert", submit),
            ("GET", "/check", check),
            ("GET", "/signed", signed),
        ]
    )
    out = temp_dir / "result.json"
    try:
        async with AsyncDatalabClient(api_key="k", base_url=_base_url(server)) as c:
            result = await c.convert(
                file_url="https://example.invalid/doc.pdf",
                stream_response_to=out,
                max_polls=1,
                poll_interval=0,
            )
    finally:
        await server.close()
    return result, json.loads(out.read_text())


def _assert_poll_fields(result):
    assert result.request_id == "req_123"
    for key, value in _POLL_FIELDS.items():
        assert getattr(result, key) == value
    assert isinstance(result.parse_quality_score, float)


@pytest.mark.asyncio
async def test_streamed_signed_result_keeps_poll_fields(temp_dir):
    """FileResult takes its fields from the poll response, not the download.

    The poll response for a signed result has the persisted fields (cost,
    page count, quality score); runtime can still be None, and it has no
    versions. The download is copied to disk without parsing.
    """
    poll_body = {
        "status": "complete",
        "success": True,
        "page_count": 4,
        "parse_quality_score": 4.25,
        "runtime": None,
        "cost_breakdown": _POLL_FIELDS["cost_breakdown"],
    }
    signed_body = {
        "markdown": "# Full",
        "parse_quality_score": 1.0,
        "runtime": 1.5,
        "versions": _POLL_FIELDS["versions"],
        "inferenced_pages": 4,
        "total_pages": 4,
    }
    result, saved = await _streamed_convert(temp_dir, poll_body, signed_body)
    assert result.request_id == "req_123"
    assert result.page_count == 4
    assert result.parse_quality_score == 4.25
    assert result.cost_breakdown == _POLL_FIELDS["cost_breakdown"]
    assert result.runtime is None
    assert result.versions is None
    assert saved == signed_body


@pytest.mark.asyncio
async def test_streamed_signed_result_from_an_early_poll(temp_dir):
    """An early poll can lack cost and quality; those fields stay None."""
    poll_body = {"status": "complete", "success": True, "page_count": 4}
    signed_body = {"markdown": "# Full", "runtime": 1.5, "total_pages": 4}
    result, _ = await _streamed_convert(temp_dir, poll_body, signed_body)
    assert result.request_id == "req_123"
    assert result.page_count == 4
    assert result.cost_breakdown is None
    assert result.parse_quality_score is None
    assert result.runtime is None


@pytest.mark.asyncio
async def test_streamed_signed_result_that_is_not_json_is_still_saved(temp_dir):
    async def submit(request):
        await request.read()
        return web.json_response(
            {"success": True, "request_id": "req_123", "request_check_url": "/check"}
        )

    async def check(request):
        return web.json_response(
            {
                "status": "complete",
                "success": True,
                "page_count": 2,
                "result_url": f"{_base_url(server)}/signed",
            }
        )

    async def signed(request):
        return web.Response(text="not json")

    server = await _serve(
        [
            ("POST", "/api/v1/convert", submit),
            ("GET", "/check", check),
            ("GET", "/signed", signed),
        ]
    )
    out = temp_dir / "result.json"
    try:
        async with AsyncDatalabClient(api_key="k", base_url=_base_url(server)) as c:
            result = await c.convert(
                file_url="https://example.invalid/doc.pdf",
                stream_response_to=out,
                max_polls=1,
                poll_interval=0,
            )
    finally:
        await server.close()
    assert out.read_text() == "not json"
    assert result.page_count == 2
    assert result.versions is None


@pytest.mark.asyncio
async def test_streamed_result_accepts_integers_above_64_bits(temp_dir):
    body = {
        "status": "complete",
        "success": True,
        "json": {"id": 9223372036854775808},
        **_POLL_FIELDS,
    }
    result, saved = await _streamed_convert(temp_dir, body)
    _assert_poll_fields(result)
    assert saved["json"]["id"] == 9223372036854775808


@pytest.mark.asyncio
async def test_streamed_result_reads_top_level_fields_only(temp_dir):
    body = {
        "status": "complete",
        "success": True,
        # Nested keys with the same names must not replace the top-level ones.
        "json": {"page_count": 99, "cost_breakdown": {"x": 1}, "children": []},
        "markdown": "# Full",
        **_POLL_FIELDS,
    }
    result, saved = await _streamed_convert(temp_dir, body)
    _assert_poll_fields(result)
    assert saved == body


@pytest.mark.asyncio
async def test_result_fields_and_request_id_are_kept(temp_dir):
    pdf = temp_dir / "doc.pdf"
    pdf.write_bytes(b"%PDF-1.4\n%%EOF\n")
    async with AsyncDatalabClient(api_key="k") as client:
        with (
            patch.object(client, "_make_request", new_callable=AsyncMock) as req,
            patch.object(client, "_poll_result", new_callable=AsyncMock) as poll,
        ):
            req.return_value = {
                "success": True,
                "request_id": "req_123",
                "request_check_url": "https://x/check",
                "checkpoint_reused": True,
            }
            poll.return_value = {
                "status": "complete",
                "success": True,
                "extraction_schema_json": '{"a": 1}',
                "extraction_score_average": 4.5,
                "extraction_scoring_status": "complete",
                "extraction_mode": "balanced",
                "segmentation_schema": None,
                "result_url": None,
            }
            result = await client.extract(
                pdf, options=ExtractOptions(page_schema='{"properties": {}}')
            )

    assert isinstance(result, ConversionResult)
    assert result.request_id == "req_123"
    assert result.checkpoint_reused is True
    assert result.extraction_score_average == 4.5
    assert result.extraction_scoring_status == "complete"
    assert result.extraction_mode == "balanced"


def test_save_output_writes_extraction_results(temp_dir):
    """save_output used an invalid suffix for extraction results before"""
    result = ConversionResult(
        success=True,
        output_format="markdown",
        markdown="# Doc",
        extraction_schema_json='{"a": 1}',
    )
    result.save_output(temp_dir / "result")
    assert (temp_dir / "result_extraction_results.json").read_text() == '{"a": 1}'
    assert (temp_dir / "result.md").exists()


@pytest.mark.asyncio
async def test_save_output_and_stream_are_exclusive(temp_dir):
    async with AsyncDatalabClient(api_key="k") as client:
        with pytest.raises(DatalabValidationError):
            await client.convert(
                temp_dir / "x.pdf",
                options=ConvertOptions(),
                save_output=temp_dir / "a",
                stream_response_to=temp_dir / "b.json",
            )
