"""
Tests that CLI flags map to the SDK options
"""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from datalab_sdk.cli import cli
from datalab_sdk.models import (
    AgentOptions,
    ConvertOptions,
    ExtractOptions,
    FormFieldsOptions,
    FormFillingOptions,
)

RESULTS = [
    {
        "success": True,
        "file_path": "/tmp/a.pdf",
        "output_path": "/tmp/out/a",
        "error": None,
        "page_count": 1,
    }
]


def _run(args, tmp_path):
    """Run the CLI and return the options passed to process_files_async"""
    doc = tmp_path / "a.pdf"
    doc.write_bytes(b"%PDF-1.4\n%%EOF\n")
    with (
        patch("datalab_sdk.cli.process_files_async", new=MagicMock()) as process,
        patch("datalab_sdk.cli.asyncio.run", return_value=RESULTS),
    ):
        result = CliRunner().invoke(
            cli,
            [args[0], str(doc), "--api_key", "k", "-o", str(tmp_path / "out")]
            + args[1:],
        )
    return result, process


def test_convert_flags_reach_options(tmp_path):
    result, process = _run(
        [
            "convert",
            "--mode",
            "lite",
            "--format",
            "markdown,html",
            "--skip_cache",
            "--merge_cross_page",
            "--word_bboxes",
            "--extras",
            "table_cell_bboxes",
            "--keep_pageheader_in_output",
            "--additional_config",
            '{"keep_pagefooter_in_output": true}',
            "--webhook_url",
            "https://hook",
        ],
        tmp_path,
    )
    assert result.exit_code == 0, result.output
    options = process.call_args.kwargs["options"]
    assert isinstance(options, ConvertOptions)
    assert options.mode == "lite"
    assert options.output_format == "markdown,html"
    assert options.skip_cache is True
    assert options.merge_cross_page and options.word_bboxes
    assert options.extras == "table_cell_bboxes"
    assert options.keep_pageheader_in_output is True
    assert options.additional_config == {"keep_pagefooter_in_output": True}
    assert options.webhook_url == "https://hook"


def test_skip_cache_is_a_flag(tmp_path):
    """--skip_cache took a value before, so any text turned it on"""
    result, process = _run(["convert"], tmp_path)
    assert result.exit_code == 0, result.output
    assert process.call_args.kwargs["options"].skip_cache is False


def test_convert_rejects_bad_format(tmp_path):
    result, _ = _run(["convert", "--format", "markdown,pdf"], tmp_path)
    assert result.exit_code != 0
    assert "Each format must be one of" in result.output


def test_extract_with_schema_id_and_extraction_mode(tmp_path):
    result, process = _run(
        [
            "extract",
            "--schema_id",
            "sch_1",
            "--schema_version",
            "2",
            "--extraction_mode",
            "turbo",
        ],
        tmp_path,
    )
    assert result.exit_code == 0, result.output
    options = process.call_args.kwargs["options"]
    assert isinstance(options, ExtractOptions)
    assert options.schema_id == "sch_1" and options.schema_version == 2
    assert options.extraction_mode == "turbo"
    assert options.page_schema == ""


def test_extract_needs_one_schema(tmp_path):
    result, _ = _run(["extract"], tmp_path)
    assert result.exit_code != 0
    assert "--page_schema or --schema_id" in result.output


def test_extract_page_schema_from_file(tmp_path):
    schema = tmp_path / "schema.json"
    schema.write_text('{"properties": {"a": {"type": "string"}}}')
    result, process = _run(["extract", "--page_schema", str(schema)], tmp_path)
    assert result.exit_code == 0, result.output
    assert '"properties"' in process.call_args.kwargs["options"].page_schema


@pytest.mark.parametrize(
    "args,cls,method",
    [
        (["agent", "--processor", "cp_1@2.0"], AgentOptions, "run_agent"),
        (["form-fields"], FormFieldsOptions, "form_fields"),
        (
            ["fill", "--field_data", '{"name": {"value": "A"}}'],
            FormFillingOptions,
            "fill",
        ),
    ],
)
def test_new_commands(args, cls, method, tmp_path):
    result, process = _run(args, tmp_path)
    assert result.exit_code == 0, result.output
    assert isinstance(process.call_args.kwargs["options"], cls)
    assert process.call_args.args[2] == method
