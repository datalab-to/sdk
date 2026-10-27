"""
Tests for HTTP error mapping and retries
"""

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from datalab_sdk import AsyncDatalabClient
from datalab_sdk.exceptions import (
    DatalabAPIError,
    DatalabAuthenticationError,
    DatalabNotFoundError,
    DatalabPaymentRequiredError,
    DatalabRateLimitError,
    DatalabServerError,
    DatalabValidationError,
)


async def _serve(handlers):
    app = web.Application()
    for method, path, handler in handlers:
        app.router.add_route(method, path, handler)
    server = TestServer(app)
    await server.start_server()
    return server


def _base_url(server):
    return f"http://{server.host}:{server.port}"


@pytest.mark.parametrize(
    "status,exc_cls",
    [
        (401, DatalabAuthenticationError),
        (403, DatalabAuthenticationError),
        (402, DatalabPaymentRequiredError),
        (404, DatalabNotFoundError),
        (400, DatalabAPIError),
    ],
)
@pytest.mark.asyncio
async def test_error_keeps_api_message_and_maps_status(status, exc_cls):
    async def handler(request):
        return web.json_response({"detail": f"message {status}"}, status=status)

    server = await _serve([("GET", "/x", handler)])
    try:
        async with AsyncDatalabClient(api_key="k", base_url=_base_url(server)) as c:
            with pytest.raises(exc_cls) as info:
                await c._make_request("GET", "/x")
    finally:
        await server.close()

    assert isinstance(info.value, DatalabAPIError)
    assert str(info.value) == f"message {status}"
    assert info.value.status_code == status
    assert info.value.response_data == {"detail": f"message {status}"}


@pytest.mark.asyncio
async def test_validation_error_list_becomes_readable_message():
    async def handler(request):
        return web.json_response(
            {"detail": [{"loc": ["body", "field_data"], "msg": "Field required"}]},
            status=422,
        )

    server = await _serve([("POST", "/x", handler)])
    try:
        async with AsyncDatalabClient(api_key="k", base_url=_base_url(server)) as c:
            with pytest.raises(DatalabAPIError, match="field_data: Field required"):
                await c._make_request("POST", "/x")
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_non_json_error_body_keeps_status():
    async def handler(request):
        return web.Response(text="bad gateway", status=502)

    server = await _serve([("GET", "/x", handler)])
    try:
        async with AsyncDatalabClient(api_key="k", base_url=_base_url(server)) as c:
            with pytest.raises(DatalabServerError) as info:
                await c._make_request("GET", "/x")
    finally:
        await server.close()
    assert info.value.status_code == 502
    assert "502" in str(info.value)


@pytest.mark.parametrize("status", [429, 503])
@pytest.mark.asyncio
async def test_submit_retries_429_and_503_with_retry_after(status):
    calls = {"n": 0}

    async def handler(request):
        calls["n"] += 1
        if calls["n"] == 1:
            return web.json_response(
                {"detail": "retry"}, status=status, headers={"Retry-After": "0"}
            )
        return web.json_response({"success": True, "request_id": "r"})

    server = await _serve([("POST", "/submit", handler)])
    try:
        async with AsyncDatalabClient(api_key="k", base_url=_base_url(server)) as c:
            result = await c._submit_with_retry("/submit", json={})
    finally:
        await server.close()
    assert calls["n"] == 2
    assert result["request_id"] == "r"


@pytest.mark.asyncio
async def test_submit_does_not_retry_402():
    calls = {"n": 0}

    async def handler(request):
        calls["n"] += 1
        return web.json_response({"detail": "over limit"}, status=402)

    server = await _serve([("POST", "/submit", handler)])
    try:
        async with AsyncDatalabClient(api_key="k", base_url=_base_url(server)) as c:
            with pytest.raises(DatalabPaymentRequiredError, match="over limit"):
                await c._submit_with_retry("/submit", json={})
    finally:
        await server.close()
    assert calls["n"] == 1


def test_rate_limit_error_is_api_error():
    assert issubclass(DatalabRateLimitError, DatalabAPIError)


def test_validation_error_is_value_error():
    """Old `except ValueError` handlers still catch input errors"""
    assert issubclass(DatalabValidationError, ValueError)
