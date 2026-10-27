"""
Tests for the endpoints and options that the SDK added for API parity
"""

import warnings
from unittest.mock import AsyncMock, patch

import pytest

from datalab_sdk import AsyncDatalabClient, DatalabClient
from datalab_sdk.models import (
    CustomProcessorOptions,
    ExtractionSchemaSuggestions,
    FormFieldsResult,
    AgentOptions,
    AgentResult,
    FormFillingOptions,
    PipelineProcessor,
)
from datalab_sdk.exceptions import DatalabValidationError
from datalab_sdk.settings import settings

INITIAL = {
    "success": True,
    "request_id": "req_1",
    "request_check_url": "https://api.datalab.to/api/v1/x/req_1",
}


@pytest.fixture
def pdf(temp_dir):
    path = temp_dir / "doc.pdf"
    path.write_bytes(b"%PDF-1.4\n%%EOF\n")
    return path


def test_agent_options_send_only_set_values():
    form = AgentOptions(processor="cp_1").to_form_data()
    assert set(form) == {"processor", "return_images", "skip_cache"}


@pytest.mark.asyncio
async def test_run_agent_needs_processor(pdf):
    async with AsyncDatalabClient(api_key="k") as client:
        with pytest.raises(DatalabValidationError):
            await client.run_agent(pdf, options=AgentOptions())


@pytest.mark.asyncio
async def test_run_agent_builds_result(pdf, temp_dir):
    async with AsyncDatalabClient(api_key="k") as client:
        with (
            patch.object(client, "_make_request", new_callable=AsyncMock) as req,
            patch.object(client, "_poll_result", new_callable=AsyncMock) as poll,
        ):
            req.return_value = INITIAL
            poll.return_value = {
                "status": "complete",
                "success": True,
                "output_shape": "jats",
                "finished": True,
                "turns": 7,
                "jats_xml": "<article/>",
                "page_count": 3,
            }
            result = await client.run_agent(
                pdf,
                options=AgentOptions(processor="cp_abc@2.0"),
                save_output=temp_dir / "out" / "doc",
            )
    assert req.await_args.args[1] == "/api/v1/agent"
    assert isinstance(result, AgentResult)
    assert result.jats_xml == "<article/>"
    assert result.turns == 7
    assert result.request_id == "req_1"
    assert (temp_dir / "out" / "doc.xml").read_text() == "<article/>"


@pytest.mark.asyncio
async def test_form_fields_builds_result(pdf):
    async with AsyncDatalabClient(api_key="k") as client:
        with (
            patch.object(client, "_make_request", new_callable=AsyncMock) as req,
            patch.object(client, "_poll_result", new_callable=AsyncMock) as poll,
        ):
            req.return_value = INITIAL
            poll.return_value = {
                "status": "complete",
                "success": True,
                "pages": [
                    {
                        "page": 0,
                        "fields": [
                            {
                                "field_type": "signature",
                                "bbox": [0.1, 0.2, 0.3, 0.4],
                                "label": "Sign",
                            }
                        ],
                        "error": "",
                    }
                ],
                "page_count": 1,
            }
            result = await client.form_fields(pdf)
    assert req.await_args.args[1] == "/api/v1/form-fields"
    assert isinstance(result, FormFieldsResult)
    assert result.pages[0]["fields"][0]["field_type"] == "signature"


@pytest.mark.asyncio
async def test_generate_extraction_schemas_sends_json():
    async with AsyncDatalabClient(api_key="k") as client:
        with (
            patch.object(client, "_make_request", new_callable=AsyncMock) as req,
            patch.object(client, "_poll_result", new_callable=AsyncMock) as poll,
        ):
            req.return_value = INITIAL
            poll.return_value = {
                "status": "complete",
                "success": True,
                "suggestions": {"simple_schema": "{}"},
            }
            result = await client.generate_extraction_schemas("ckpt_1")
    method, endpoint = req.await_args.args
    assert (method, endpoint) == ("POST", "/api/v1/marker/extraction/gen_schemas")
    assert req.await_args.kwargs["json"] == {"checkpoint_id": "ckpt_1"}
    assert isinstance(result, ExtractionSchemaSuggestions)
    assert result.suggestions["simple_schema"] == "{}"


@pytest.mark.asyncio
async def test_create_document_sends_processing_location():
    async with AsyncDatalabClient(api_key="k") as client:
        with (
            patch.object(client, "_make_request", new_callable=AsyncMock) as req,
            patch.object(client, "_poll_result", new_callable=AsyncMock) as poll,
        ):
            req.return_value = INITIAL
            poll.return_value = {"status": "complete", "success": True}
            result = await client.create_document("# Hi", processing_location="eu")
    assert req.await_args.kwargs["json"] == {
        "markdown": "# Hi",
        "output_format": "docx",
        "processing_location": "eu",
    }
    assert result.request_id == "req_1"


@pytest.mark.asyncio
async def test_delete_result():
    async with AsyncDatalabClient(api_key="k") as client:
        with patch.object(client, "_make_request", new_callable=AsyncMock) as req:
            req.return_value = {"success": True, "message": "deleted"}
            await client.delete_result("req_1")
            assert req.await_args.args == ("DELETE", "/api/v1/results/req_1")


@pytest.mark.asyncio
async def test_create_pipeline_reuse_draft_and_new_fields():
    async with AsyncDatalabClient(api_key="k") as client:
        with patch.object(client, "_make_request", new_callable=AsyncMock) as req:
            req.return_value = {
                "pipeline_id": "pl_1",
                "steps": [],
                "reused": True,
                "draft_differs": True,
            }
            config = await client.create_pipeline(
                [PipelineProcessor(type="convert")], reuse_draft=True
            )
    assert req.await_args.kwargs["json"]["reuse_draft"] is True
    assert config.reused is True and config.draft_differs is True


def test_pipeline_execution_keeps_new_step_fields():
    execution = AsyncDatalabClient._build_pipeline_execution(
        {
            "execution_id": "pex_1",
            "steps": [
                {
                    "step_index": 1,
                    "step_type": "eval",
                    "status": "completed",
                    "source_step_type": "convert",
                    "checkpoint_reused": False,
                }
            ],
        }
    )
    assert execution.steps[0].source_step_type == "convert"
    assert execution.steps[0].checkpoint_reused is False


@pytest.mark.asyncio
async def test_upload_sends_processing_location(pdf):
    async with AsyncDatalabClient(api_key="k") as client:
        with (
            patch.object(client, "_make_request", new_callable=AsyncMock) as req,
            patch("aiohttp.ClientSession.put") as put,
        ):
            put.return_value.__aenter__.return_value.raise_for_status = lambda: None
            req.side_effect = [
                {
                    "file_id": 1,
                    "upload_url": "https://u",
                    "reference": "datalab://file-a",
                },
                {"created": None},
            ]
            meta = await client.upload_files(pdf, processing_location="eu")
    assert req.await_args_list[0].kwargs["json"]["processing_location"] == "eu"
    assert meta.reference == "datalab://file-a"


@pytest.mark.asyncio
async def test_custom_processor_warns_deprecated(pdf):
    async with AsyncDatalabClient(api_key="k") as client:
        with (
            patch.object(client, "_make_request", new_callable=AsyncMock) as req,
            patch.object(client, "_poll_result", new_callable=AsyncMock) as poll,
        ):
            req.return_value = INITIAL
            poll.return_value = {"status": "complete", "success": True}
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                await client.run_custom_processor(
                    pdf, options=CustomProcessorOptions(pipeline_id="cp_1")
                )
    assert any("deprecated" in str(w.message) for w in caught)


@pytest.mark.asyncio
async def test_workflow_methods_warn_removed():
    async with AsyncDatalabClient(api_key="k") as client:
        with patch.object(client, "_make_request", new_callable=AsyncMock) as req:
            req.return_value = {"workflows": []}
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                await client.list_workflows()
    assert any(
        issubclass(w.category, DeprecationWarning) and "pipelines" in str(w.message)
        for w in caught
    )


def test_fill_options_drop_max_pages_with_warning():
    options = FormFillingOptions(field_data={"a": {"value": "1"}}, max_pages=2)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        form = options.to_form_data()
    assert "max_pages" not in form
    assert any("page_range" in str(w.message) for w in caught)


def test_sync_run_agent_forwards(pdf):
    client = DatalabClient(api_key="k")
    with patch.object(client._async_client, "run_agent", new_callable=AsyncMock) as run:
        run.return_value = AgentResult(status="complete", success=True)
        client.run_agent(pdf, options=AgentOptions(processor="cp_1"), max_polls=3)
    assert run.await_args.kwargs["max_polls"] == 3


def test_version_matches_package_metadata():
    from importlib.metadata import version

    assert settings.VERSION == version("datalab-python-sdk")
