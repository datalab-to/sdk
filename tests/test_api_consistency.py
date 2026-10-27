"""
Check that the SDK sends the form field names that the Datalab API accepts.

The API_FIELDS table lists the public form parameters of each API endpoint.
Keep it in sync with the API. A test fails when the SDK sends a field that the
endpoint does not accept, or when the SDK has no way to send a public field.
"""

import dataclasses
import inspect
from unittest.mock import AsyncMock, patch

import pytest

from datalab_sdk import AsyncDatalabClient, DatalabClient
from datalab_sdk.models import (
    AgentOptions,
    ConvertOptions,
    CustomProcessorOptions,
    ExtractOptions,
    FormFieldsOptions,
    FormFillingOptions,
    OCROptions,
    SegmentOptions,
    TrackChangesOptions,
)

# Public form parameters per endpoint, from the Datalab API.
API_FIELDS = {
    "/api/v1/convert": {
        "file",
        "file_url",
        "mode",
        "max_pages",
        "page_range",
        "merge_cross_page",
        "paginate",
        "add_block_ids",
        "disable_html_prettify",
        "exact_transcription",
        "include_markdown_in_chunks",
        "disable_image_extraction",
        "disable_image_captions",
        "word_bboxes",
        "fence_synthetic_captions",
        "output_format",
        "token_efficient_markdown",
        "skip_cache",
        "save_checkpoint",
        "additional_config",
        "extras",
        "webhook_url",
        "processing_location",
    },
    "/api/v1/extract": {
        "file",
        "file_url",
        "checkpoint_id",
        "page_schema",
        "schema_id",
        "schema_version",
        "mode",
        "max_pages",
        "page_range",
        "output_format",
        "save_checkpoint",
        "skip_cache",
        "webhook_url",
        "processing_location",
        "extraction_mode",
    },
    "/api/v1/segment": {
        "file",
        "file_url",
        "checkpoint_id",
        "segmentation_schema",
        "mode",
        "max_pages",
        "page_range",
        "save_checkpoint",
        "skip_cache",
        "webhook_url",
        "processing_location",
    },
    "/api/v1/custom-processor": {
        "file",
        "file_url",
        "pipeline_id",
        "version",
        "mode",
        "max_pages",
        "page_range",
        "output_format",
        "paginate",
        "add_block_ids",
        "disable_html_prettify",
        "exact_transcription",
        "include_markdown_in_chunks",
        "disable_image_extraction",
        "disable_image_captions",
        "skip_cache",
        "webhook_url",
        "processing_location",
    },
    "/api/v1/track-changes": {
        "file",
        "file_url",
        "output_format",
        "max_pages",
        "page_range",
        "paginate",
        "skip_cache",
        "webhook_url",
        "processing_location",
    },
    "/api/v1/form-fields": {
        "file",
        "file_url",
        "max_pages",
        "page_range",
        "skip_cache",
        "webhook_url",
        "processing_location",
    },
    "/api/v1/fill": {
        "file",
        "file_url",
        "field_data",
        "context",
        "confidence_threshold",
        "page_range",
        "skip_cache",
        "processing_location",
    },
    "/api/v1/agent": {
        "file",
        "file_url",
        "processor",
        "verification_effort",
        "return_images",
        "mode",
        "skip_cache",
        "webhook_url",
        "processing_location",
    },
    # Deprecated endpoint. The API ignores 'langs', so the SDK does not send it.
    "/api/v1/ocr": {
        "file",
        "max_pages",
        "page_range",
        "skip_cache",
        "processing_location",
    },
    "/api/v1/pipelines/{pipeline_id}/run": {
        "file",
        "file_url",
        "page_range",
        "output_format",
        "skip_cache",
        "checkpoint_id",
        "webhook_url",
        "processing_location",
        "version",
    },
}

# Fields that the SDK sends as a separate argument, not through an option.
FILE_FIELDS = {"file", "file_url"}


def _fill_all(options):
    """Set every None field to a placeholder, so to_form_data sends it."""
    for f in dataclasses.fields(options):
        if getattr(options, f.name) is None:
            if "Dict" in str(f.type):
                value = {"key": True}
            elif "int" in str(f.type):
                value = 1
            else:
                value = "x"
            setattr(options, f.name, value)
    return options


def _form_names(form_data):
    return [field[0]["name"] for field in form_data._fields]


# (client method, endpoint, options class, constructor kwargs, fields to
# clear because the API does not accept them together with the others)
INFERENCE_CASES = [
    ("convert", "/api/v1/convert", ConvertOptions, {}, ()),
    (
        "extract",
        "/api/v1/extract",
        ExtractOptions,
        {"page_schema": "{}"},
        ("checkpoint_id", "schema_id", "schema_version"),
    ),
    (
        "extract",
        "/api/v1/extract",
        ExtractOptions,
        {"schema_id": "sch_1"},
        ("checkpoint_id", "page_schema"),
    ),
    (
        "segment",
        "/api/v1/segment",
        SegmentOptions,
        {"segmentation_schema": "{}"},
        ("checkpoint_id",),
    ),
    (
        "_run_custom_processor",
        "/api/v1/custom-processor",
        CustomProcessorOptions,
        {"pipeline_id": "cp_1"},
        (),
    ),
    ("track_changes", "/api/v1/track-changes", TrackChangesOptions, {}, ()),
    ("form_fields", "/api/v1/form-fields", FormFieldsOptions, {}, ()),
    ("fill", "/api/v1/fill", FormFillingOptions, {}, ("max_pages",)),
    ("run_agent", "/api/v1/agent", AgentOptions, {"processor": "cp_1"}, ()),
    ("ocr", "/api/v1/ocr", OCROptions, {}, ()),
]


@pytest.mark.parametrize("method,endpoint,options_cls,kwargs,cleared", INFERENCE_CASES)
@pytest.mark.asyncio
async def test_sent_fields_match_api(
    method, endpoint, options_cls, kwargs, cleared, temp_dir, recwarn
):
    """Each method sends only API fields, and can send every public API field"""
    pdf = temp_dir / "doc.pdf"
    pdf.write_bytes(b"%PDF-1.4\n%%EOF\n")

    options = _fill_all(options_cls(**kwargs))
    for name in cleared:
        setattr(options, name, "" if name == "page_schema" else None)

    async with AsyncDatalabClient(api_key="k") as client:
        with (
            patch.object(
                client, "_submit_with_retry", new_callable=AsyncMock
            ) as submit,
            patch.object(client, "_poll_result", new_callable=AsyncMock) as poll,
        ):
            submit.return_value = {
                "success": True,
                "request_id": "r1",
                "request_check_url": "https://x/check",
            }
            poll.return_value = {"status": "complete", "success": True}
            await getattr(client, method)(pdf, options=options)

    called_endpoint = submit.await_args.args[0]
    assert called_endpoint == endpoint
    sent = _form_names(submit.await_args.kwargs["data"])
    assert len(sent) == len(set(sent)), f"duplicate fields: {sent}"

    api_fields = API_FIELDS[endpoint]
    unknown = set(sent) - api_fields
    assert not unknown, f"{method} sends fields the API does not accept: {unknown}"

    # The test sets every option, so the SDK must send every public field.
    # file_url is the other file input. The cleared fields are not sent.
    expected = api_fields - {"file_url"} - set(cleared)
    missing = expected - set(sent)
    assert not missing, f"{method} cannot send API fields: {missing}"


@pytest.mark.asyncio
async def test_extract_and_segment_send_checkpoint_id():
    async with AsyncDatalabClient(api_key="k") as client:
        with (
            patch.object(
                client, "_submit_with_retry", new_callable=AsyncMock
            ) as submit,
            patch.object(client, "_poll_result", new_callable=AsyncMock) as poll,
        ):
            submit.return_value = {
                "success": True,
                "request_id": "r1",
                "request_check_url": "https://x/check",
            }
            poll.return_value = {"status": "complete", "success": True}
            await client.extract(
                options=ExtractOptions(page_schema="{}", checkpoint_id="ckpt_1")
            )
            names = _form_names(submit.await_args.kwargs["data"])
            assert "checkpoint_id" in names and "file" not in names
            await client.segment(
                options=SegmentOptions(segmentation_schema="{}", checkpoint_id="ckpt_1")
            )
            names = _form_names(submit.await_args.kwargs["data"])
            assert "checkpoint_id" in names and "file" not in names


@pytest.mark.asyncio
async def test_run_pipeline_fields_match_api(temp_dir):
    pdf = temp_dir / "doc.pdf"
    pdf.write_bytes(b"%PDF-1.4\n%%EOF\n")
    async with AsyncDatalabClient(api_key="k") as client:
        with patch.object(
            client, "_submit_with_retry", new_callable=AsyncMock
        ) as submit:
            submit.return_value = {
                "execution_id": "pex_1",
                "pipeline_id": "pl_1",
                "status": "pending",
                "steps": [],
            }
            await client.run_pipeline(
                "pl_1",
                file_path=pdf,
                page_range="0-1",
                output_format="json",
                skip_cache=True,
                webhook_url="https://hook",
                version=2,
                checkpoint_id="ckpt_1",
                processing_location="eu",
            )
    assert submit.await_args.args[0] == "/api/v1/pipelines/pl_1/run"
    sent = _form_names(submit.await_args.kwargs["data"])
    api_fields = API_FIELDS["/api/v1/pipelines/{pipeline_id}/run"]
    assert set(sent) <= api_fields
    assert api_fields - set(sent) == {"file_url"}


def test_convert_additional_config_flags_are_nested():
    """keep_* flags go inside additional_config, not at the top level"""
    form = ConvertOptions(
        keep_spreadsheet_formatting=True,
        keep_pageheader_in_output=True,
        keep_pagefooter_in_output=True,
        additional_config={"other": 1},
    ).to_form_data()
    assert "keep_pageheader_in_output" not in form
    import json

    config = json.loads(form["additional_config"][1])
    assert config == {
        "other": 1,
        "keep_spreadsheet_formatting": True,
        "keep_pageheader_in_output": True,
        "keep_pagefooter_in_output": True,
    }


def _public_methods(cls):
    return {
        name: member
        for name, member in inspect.getmembers(cls, inspect.isfunction)
        if not name.startswith("_") and name not in ("close", "get_form_params")
    }


def test_sync_client_has_every_async_method_with_same_parameters():
    async_methods = _public_methods(AsyncDatalabClient)
    sync_methods = _public_methods(DatalabClient)

    missing = set(async_methods) - set(sync_methods)
    assert not missing, f"DatalabClient is missing: {sorted(missing)}"

    for name, async_fn in async_methods.items():
        async_params = list(inspect.signature(async_fn).parameters)
        sync_params = list(inspect.signature(sync_methods[name]).parameters)
        assert async_params == sync_params, f"parameter mismatch in {name}"


def test_extract_mode_is_not_sent_by_default():
    """The API picks the parse mode that matches extraction_mode"""
    form = ExtractOptions(page_schema="{}").to_form_data()
    assert "mode" not in form
    form = ExtractOptions(page_schema="{}", mode="fast").to_form_data()
    assert form["mode"] == (None, "fast")
