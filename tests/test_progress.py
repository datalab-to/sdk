"""Progress callbacks follow existing polls, including streaming and signed results."""

import inspect
import json
from unittest.mock import AsyncMock, patch

import pytest

from datalab_sdk import AsyncDatalabClient, DatalabClient, ExtractOptions
from datalab_sdk.exceptions import DatalabAPIError, DatalabTimeoutError


@pytest.mark.asyncio
async def test_convert_reports_increasing_percentages_without_extra_requests():
    updates = []
    client = AsyncDatalabClient(api_key="test")
    responses = [
        {"status": "processing", "progress_percent": p}
        for p in [None, 0, 12, 12, 8, True, "25", -1, 101, 100, 48]
    ] + [
        {
            "status": "complete",
            "success": True,
            "progress_percent": 100,
            "markdown": "done",
        }
    ]
    with (
        patch.object(
            client,
            "_submit_with_retry",
            AsyncMock(
                return_value={
                    "success": True,
                    "request_id": "req",
                    "request_check_url": "/poll/req",
                }
            ),
        ) as submit,
        patch.object(
            client, "_poll_get_with_retry", AsyncMock(side_effect=responses)
        ) as poll,
    ):
        result = await client.convert(
            file_url="https://example.com/test.pdf",
            poll_interval=0,
            on_progress=updates.append,
        )
    assert updates == [0, 12, 48, 100]
    assert result.progress_percent == 100
    assert result.request_id == "req"
    assert submit.await_count == 1
    assert poll.await_count == len(responses)
    # The callback is local; it is never serialized into the API request.
    assert "on_progress" not in str(submit.call_args)


@pytest.mark.asyncio
async def test_async_callback_and_signed_download_complete_before_100():
    updates = []
    client = AsyncDatalabClient(api_key="test")

    async def callback(percent):
        updates.append(percent)

    async def download(url):
        assert updates == [27]
        return {"markdown": "downloaded"}

    with (
        patch.object(
            client,
            "_poll_get_with_retry",
            AsyncMock(
                side_effect=[
                    {"status": "processing", "progress_percent": 27},
                    {
                        "status": "complete",
                        "success": True,
                        "result_url": "https://storage.example/result",
                    },
                ]
            ),
        ),
        patch.object(client, "_fetch_signed_result", side_effect=download),
    ):
        result = await client._poll_result(
            "/poll", poll_interval=0, on_progress=callback
        )
    assert updates == [27, 100]
    assert result["markdown"] == "downloaded"


@pytest.mark.asyncio
async def test_download_failure_never_reports_completion():
    updates = []
    client = AsyncDatalabClient(api_key="test")
    with (
        patch.object(
            client,
            "_poll_get_with_retry",
            AsyncMock(
                return_value={
                    "status": "complete",
                    "success": True,
                    "progress_percent": 100,
                    "result_url": "https://storage.example/result",
                }
            ),
        ),
        patch.object(
            client,
            "_fetch_signed_result",
            AsyncMock(side_effect=DatalabAPIError("download failed")),
        ),
    ):
        with pytest.raises(DatalabAPIError, match="download failed"):
            await client._poll_result("/poll", on_progress=updates.append)
    assert updates == []


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["failed", "timeout", "callback"])
async def test_no_completion_on_processing_failure_timeout_or_callback_error(terminal):
    updates = []
    client = AsyncDatalabClient(api_key="test")

    def callback(percent):
        updates.append(percent)
        if terminal == "callback":
            raise ValueError("display failed")

    response = {"status": "processing", "progress_percent": 35}
    if terminal == "failed":
        response.update(status="failed", success=False, error="bad document")
    error = {
        "failed": DatalabAPIError,
        "timeout": DatalabTimeoutError,
        "callback": ValueError,
    }[terminal]
    with patch.object(
        client, "_poll_get_with_retry", AsyncMock(return_value=response)
    ) as poll:
        with pytest.raises(error):
            await client._poll_result(
                "/poll", max_polls=1, poll_interval=0, on_progress=callback
            )
    assert updates == [35]
    assert poll.await_count == 1


@pytest.mark.asyncio
async def test_pipeline_submission_and_polling_share_monotonic_callback():
    updates = []
    client = AsyncDatalabClient(api_key="test")
    initial = {"execution_id": "pex_test", "status": "running", "progress_percent": 20}
    with (
        patch.object(client, "_submit_with_retry", AsyncMock(return_value=initial)),
        patch.object(
            client,
            "_make_request",
            AsyncMock(
                side_effect=[
                    {**initial, "progress_percent": 10},
                    {**initial, "progress_percent": 50},
                    {**initial, "status": "completed", "progress_percent": 100},
                ]
            ),
        ) as poll,
    ):
        result = await client.run_pipeline(
            "pl_test",
            file_url="https://example.com/test.pdf",
            max_polls=4,
            poll_interval=0,
            on_progress=updates.append,
        )
    assert updates == [20, 50, 100]
    assert result.progress_percent == 100
    assert poll.await_count == 3


@pytest.mark.asyncio
async def test_pipeline_single_poll_preserves_progress_and_failed_status():
    client = AsyncDatalabClient(api_key="test")
    updates = []
    with patch.object(
        client,
        "_make_request",
        AsyncMock(
            return_value={
                "execution_id": "pex_test",
                "status": "failed",
                "progress_percent": 63,
                "steps": [
                    {
                        "step_index": 0,
                        "step_type": "convert",
                        "status": "failed",
                        "progress_percent": 63,
                    }
                ],
            }
        ),
    ):
        result = await client.get_pipeline_execution(
            "pex_test", on_progress=updates.append
        )
    assert result.status == "failed"
    assert result.progress_percent == result.steps[0].progress_percent == 63
    assert updates == [63]


def test_sync_extract_callback_uses_same_polling_flow():
    updates = []
    client = DatalabClient(api_key="test")
    with (
        patch.object(
            client._async_client,
            "_submit_with_retry",
            AsyncMock(return_value={"success": True, "request_check_url": "/poll"}),
        ),
        patch.object(
            client._async_client,
            "_poll_get_with_retry",
            AsyncMock(
                side_effect=[
                    {"status": "processing", "progress_percent": 18},
                    {"status": "complete", "success": True, "progress_percent": 100},
                ]
            ),
        ),
    ):
        result = client.extract(
            file_url="https://example.com/test.pdf",
            options=ExtractOptions(page_schema='{"type":"object"}'),
            poll_interval=0,
            on_progress=updates.append,
        )
    assert updates == [18, 100]
    assert result.progress_percent == 100


@pytest.mark.asyncio
async def test_streaming_reads_percent_without_loading_output_and_delays_completion(
    tmp_path,
):
    client = AsyncDatalabClient(api_key="test")
    updates = []
    output = tmp_path / "result.json"

    class Content:
        def __init__(self, data):
            self.data = json.dumps(data).encode()

        async def iter_any(self):
            for start in range(0, len(self.data), 7):
                yield self.data[start : start + 7]

    class Response:
        status = 200

        def __init__(self, data):
            self.content = Content(data)

        def release(self):
            pass

    async def download(url, path):
        assert updates == [42]
        assert not output.exists()
        path.write_text('{"markdown":"large result"}')

    client._session = type("Session", (), {})()
    client._session.get = AsyncMock(
        side_effect=[
            Response({"status": "processing", "progress_percent": 42}),
            Response(
                {
                    "status": "complete",
                    "success": True,
                    "progress_percent": 100,
                    "result_url": "https://storage.example/result",
                }
            ),
        ]
    )
    with (
        patch.object(client, "_ensure_session", AsyncMock()),
        patch.object(client, "_download_signed_url_to_file", side_effect=download),
    ):
        result = await client._poll_result(
            "/poll",
            poll_interval=0,
            stream_response_to=output,
            on_progress=updates.append,
        )
    assert updates == [42, 100]
    assert result.progress_percent == 100
    assert json.loads(output.read_text()) == {"markdown": "large result"}


def test_sync_and_async_processing_signatures_match():
    for method in [
        "convert",
        "extract",
        "segment",
        "run_agent",
        "track_changes",
        "create_document",
        "ocr",
        "fill",
        "form_fields",
        "run_pipeline",
        "get_pipeline_execution",
    ]:
        for cls in [AsyncDatalabClient, DatalabClient]:
            param = inspect.signature(getattr(cls, method)).parameters["on_progress"]
            assert param.default is None
