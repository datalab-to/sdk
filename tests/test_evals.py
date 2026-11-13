"""
Tests for the Eval methods (open beta; the API may change): request payloads,
response parsing, the exactly-one-of validation, wait_for_eval, and the CLI.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from datalab_sdk import AsyncDatalabClient, DatalabClient
from datalab_sdk.cli import cli
from datalab_sdk.exceptions import DatalabTimeoutError, DatalabValidationError
from datalab_sdk.models import (
    Eval,
    EvalDifferences,
    EvalList,
    EvalVariant,
    EvalVariantResult,
)

EVAL_RESPONSE = {
    "eval_id": 1,
    "pipeline_id": "pl_1",
    "pipeline_name": "Invoices",
    "pipeline_version": 2,
    "compared_step": "convert",
    "collection_id": None,
    "file_ids": [10, 11],
    "status": "running",
    "variants": [
        {
            "name": "Fast",
            "steps": {"convert": {"output_mode": "fast"}},
            "status": "running",
            "share": None,
            "won": 0,
            "decided": 0,
            "documents_compared": 0,
            "documents_total": 2,
            "convert_settings": {"output_mode": "fast", "merge_cross_page": False},
            "extract_settings": {},
        }
    ],
    "created": "2026-01-01T00:00:00",
}

EXTRACT_EVAL_RESPONSE = {
    "eval_id": 2,
    "pipeline_id": "pl_2",
    "pipeline_name": "Contracts",
    "pipeline_version": 1,
    "compared_step": "extract",
    "collection_id": 5,
    "file_ids": [20],
    "status": "completed",
    "files": [
        {
            "file_id": 20,
            "file_name": "a.pdf",
            "comparison": "completed",
            "comparison_error": None,
        }
    ],
    "variants": [
        {
            "name": "Balanced",
            "steps": {"extract": {"extraction_mode": "balanced"}},
            "status": "completed",
            "share": 0.8,
            "won": 8,
            "decided": 10,
            "documents_compared": 1,
            "documents_total": 1,
            "convert_settings": {"output_mode": "accurate", "merge_cross_page": False},
            "extract_settings": {"extraction_mode": "balanced"},
            "undecided": 1,
            "rate_per_1000_pages_cents": 2000.0,
            "seconds_per_page": 3.5,
            "documents": [
                {
                    "file_name": "a.pdf",
                    "file_id": 20,
                    "status": "completed",
                    "lookup_key": "lk_1",
                    "request_id": 99,
                    "share": 0.8,
                    "won": 8,
                    "decided": 10,
                    "undecided": 1,
                }
            ],
        }
    ],
    "created": "2026-01-01T00:00:00",
}


@pytest.mark.asyncio
async def test_create_eval_sends_file_ids_and_variants():
    async with AsyncDatalabClient(api_key="k") as client:
        with patch.object(client, "_make_request", new_callable=AsyncMock) as req:
            req.return_value = EVAL_RESPONSE
            result = await client.create_eval(
                "pl_1",
                file_ids=[10, 11],
                variants=[
                    EvalVariant(
                        name="Fast", steps={"convert": {"output_mode": "fast"}}
                    ),
                    EvalVariant(
                        name="Accurate", steps={"convert": {"output_mode": "accurate"}}
                    ),
                ],
                version=0,
            )
    method, endpoint = req.await_args.args
    assert (method, endpoint) == ("POST", "/api/v1/pipelines/pl_1/evals")
    assert req.await_args.kwargs["json"] == {
        "file_ids": [10, 11],
        "variants": [
            {"name": "Fast", "steps": {"convert": {"output_mode": "fast"}}},
            {"name": "Accurate", "steps": {"convert": {"output_mode": "accurate"}}},
        ],
        "version": 0,
    }
    assert isinstance(result, Eval)
    assert result.eval_id == 1
    assert result.variants[0].name == "Fast"


@pytest.mark.asyncio
async def test_create_eval_sends_collection_id():
    async with AsyncDatalabClient(api_key="k") as client:
        with patch.object(client, "_make_request", new_callable=AsyncMock) as req:
            req.return_value = EVAL_RESPONSE
            await client.create_eval("pl_1", collection_id=5)
    assert req.await_args.kwargs["json"] == {"collection_id": 5}


@pytest.mark.asyncio
async def test_create_eval_needs_exactly_one_of_file_ids_or_collection_id():
    async with AsyncDatalabClient(api_key="k") as client:
        with pytest.raises(DatalabValidationError):
            await client.create_eval("pl_1")
        with pytest.raises(DatalabValidationError):
            await client.create_eval("pl_1", file_ids=[1], collection_id=2)


@pytest.mark.asyncio
async def test_get_eval_parses_extract_eval_with_documents_and_files():
    async with AsyncDatalabClient(api_key="k") as client:
        with patch.object(client, "_make_request", new_callable=AsyncMock) as req:
            req.return_value = EXTRACT_EVAL_RESPONSE
            result = await client.get_eval(2)
    assert req.await_args.args == ("GET", "/api/v1/evals/2")
    assert isinstance(result, Eval)
    assert result.compared_step == "extract"
    assert result.files[0].file_name == "a.pdf"
    variant = result.variants[0]
    assert isinstance(variant, EvalVariantResult)
    assert variant.extract_settings == {"extraction_mode": "balanced"}
    assert variant.share == 0.8
    assert variant.rate_per_1000_pages_cents == 2000.0
    assert variant.documents[0].lookup_key == "lk_1"
    assert variant.documents[0].undecided == 1


@pytest.mark.asyncio
async def test_list_evals_sends_query_params_and_parses_results():
    async with AsyncDatalabClient(api_key="k") as client:
        with patch.object(client, "_make_request", new_callable=AsyncMock) as req:
            req.return_value = {"evals": [EVAL_RESPONSE], "total": 1}
            result = await client.list_evals(
                pipeline_id="pl_1", collection_id=5, limit=10, offset=2
            )
    method, endpoint = req.await_args.args
    assert method == "GET"
    assert (
        endpoint == "/api/v1/evals?limit=10&offset=2&pipeline_id=pl_1&collection_id=5"
    )
    assert isinstance(result, EvalList)
    assert result.total == 1
    assert isinstance(result.evals[0], Eval)


@pytest.mark.asyncio
async def test_get_eval_differences_parses_question_and_description():
    async with AsyncDatalabClient(api_key="k") as client:
        with patch.object(client, "_make_request", new_callable=AsyncMock) as req:
            req.return_value = {
                "file_id": 20,
                "variant_names": ["Fast", "Accurate"],
                "differences": [
                    {
                        "page": 0,
                        "before": "",
                        "after": "",
                        "readings": ["Acme Corp", "Acme Corporation"],
                        "winners": [1],
                        "verdict": "decided",
                        "decided_by": "judge",
                        "weight": 1.0,
                        "question": "vendor_name",
                        "boxes": [[0.1, 0.2, 0.3, 0.4]],
                        "description": "The name of the vendor on the invoice",
                    }
                ],
                "fields": [
                    {
                        "path": "vendor_name",
                        "description": "The vendor",
                        "values": ["Acme Corp", "Acme Corporation"],
                    },
                    {"path": "total", "description": "", "values": ["100", "100"]},
                ],
            }
            result = await client.get_eval_differences(2, 20)
    assert req.await_args.args == ("GET", "/api/v1/evals/2/files/20/differences")
    assert isinstance(result, EvalDifferences)
    assert result.differences[0].question == "vendor_name"
    assert result.differences[0].description == "The name of the vendor on the invoice"
    assert result.differences[0].winners == [1]
    # Every compared field, equal or not, with each variant's value.
    assert [f.path for f in result.fields] == ["vendor_name", "total"]
    assert result.fields[1].values == ["100", "100"]


@pytest.mark.asyncio
async def test_wait_for_eval_polls_until_completed():
    async with AsyncDatalabClient(api_key="k") as client:
        running = dict(EVAL_RESPONSE)
        completed = {**EVAL_RESPONSE, "status": "completed"}
        with (
            patch.object(client, "get_eval", new_callable=AsyncMock) as get_eval,
            patch("asyncio.sleep", new_callable=AsyncMock) as sleep,
        ):
            get_eval.side_effect = [
                client._build_eval(running),
                client._build_eval(running),
                client._build_eval(completed),
            ]
            result = await client.wait_for_eval(1, poll_interval=5, timeout=60)
    assert result.status == "completed"
    assert get_eval.await_count == 3
    assert sleep.await_count == 2


@pytest.mark.asyncio
async def test_wait_for_eval_times_out():
    async with AsyncDatalabClient(api_key="k") as client:
        with (
            patch.object(client, "get_eval", new_callable=AsyncMock) as get_eval,
            patch("asyncio.sleep", new_callable=AsyncMock),
        ):
            get_eval.return_value = client._build_eval(
                EVAL_RESPONSE
            )  # always "running"
            with pytest.raises(DatalabTimeoutError):
                await client.wait_for_eval(1, poll_interval=10, timeout=15)


def test_sync_create_eval_forwards():
    client = DatalabClient(api_key="k")
    with patch.object(
        client._async_client, "create_eval", new_callable=AsyncMock
    ) as create:
        create.return_value = client._async_client._build_eval(EVAL_RESPONSE)
        client.create_eval("pl_1", file_ids=[1], version=0)
    assert create.await_args.kwargs == {
        "file_ids": [1],
        "collection_id": None,
        "variants": None,
        "version": 0,
    }
    assert create.await_args.args == ("pl_1",)


def test_sync_wait_for_eval_forwards():
    client = DatalabClient(api_key="k")
    with patch.object(
        client._async_client, "wait_for_eval", new_callable=AsyncMock
    ) as wait:
        wait.return_value = client._async_client._build_eval(EVAL_RESPONSE)
        client.wait_for_eval(1, poll_interval=3, timeout=30)
    assert wait.await_args.args == (1,)
    assert wait.await_args.kwargs == {"poll_interval": 3, "timeout": 30}


class TestEvalCLI:
    def test_eval_run_uploads_files_and_creates_eval(self, tmp_path):
        doc = tmp_path / "a.pdf"
        doc.write_bytes(b"%PDF-1.4\n%%EOF\n")
        uploaded = MagicMock(file_id=42)
        with patch("datalab_sdk.cli.DatalabClient") as client_cls:
            client = client_cls.return_value
            client.upload_files.return_value = [uploaded]
            client.create_eval.return_value = MagicMock(eval_id=7, status="running")
            result = CliRunner().invoke(
                cli,
                ["eval", "run", str(doc), "--pipeline", "pl_1", "--api_key", "k"],
            )
        assert result.exit_code == 0, result.output
        assert "Eval 7 started" in result.output
        client.upload_files.assert_called_once_with([str(doc)])
        assert client.create_eval.call_args.kwargs["file_ids"] == [42]
        assert client.create_eval.call_args.kwargs["collection_id"] is None

    def test_eval_run_needs_paths_or_collection_not_both(self, tmp_path):
        doc = tmp_path / "a.pdf"
        doc.write_bytes(b"%PDF-1.4\n%%EOF\n")
        result = CliRunner().invoke(
            cli,
            [
                "eval",
                "run",
                str(doc),
                "--pipeline",
                "pl_1",
                "--collection_id",
                "5",
                "--api_key",
                "k",
            ],
        )
        assert result.exit_code != 0
        assert "Give either FILE paths or --collection_id" in result.output

    def test_eval_run_with_wait_prints_summary(self, tmp_path):
        from datalab_sdk.models import Eval as EvalModel
        from datalab_sdk.models import EvalVariantResult

        doc = tmp_path / "a.pdf"
        doc.write_bytes(b"%PDF-1.4\n%%EOF\n")
        with patch("datalab_sdk.cli.DatalabClient") as client_cls:
            client = client_cls.return_value
            client.upload_files.return_value = [MagicMock(file_id=1)]
            client.create_eval.return_value = MagicMock(eval_id=7, status="running")
            client.wait_for_eval.return_value = EvalModel(
                eval_id=7,
                pipeline_id="pl_1",
                status="completed",
                compared_step="convert",
                variants=[
                    EvalVariantResult(
                        name="Accurate",
                        steps={},
                        status="completed",
                        share=0.9,
                        decided=10,
                        won=9,
                        rate_per_1000_pages_cents=1000.0,
                        seconds_per_page=2.0,
                    )
                ],
            )
            result = CliRunner().invoke(
                cli,
                [
                    "eval",
                    "run",
                    str(doc),
                    "--pipeline",
                    "pl_1",
                    "--api_key",
                    "k",
                    "--wait",
                ],
            )
        assert result.exit_code == 0, result.output
        assert "Eval 7: completed" in result.output
        assert "Accurate: share=90%" in result.output

    def test_eval_get_prints_summary(self):
        # Use the real Eval dataclass for a faithful summary line.
        from datalab_sdk.models import Eval as EvalModel

        with patch("datalab_sdk.cli.DatalabClient") as client_cls:
            client = client_cls.return_value
            client.get_eval.return_value = EvalModel(
                eval_id=7,
                pipeline_id="pl_1",
                status="completed",
                variants=[],
            )
            result = CliRunner().invoke(cli, ["eval", "get", "7", "--api_key", "k"])
        assert result.exit_code == 0, result.output
        assert "Eval 7: completed" in result.output
        client.get_eval.assert_called_once_with(7)

    def test_eval_list_prints_count(self):
        from datalab_sdk.models import Eval as EvalModel
        from datalab_sdk.models import EvalList as EvalListModel

        with patch("datalab_sdk.cli.DatalabClient") as client_cls:
            client = client_cls.return_value
            client.list_evals.return_value = EvalListModel(
                evals=[EvalModel(eval_id=1, pipeline_id="pl_1", status="completed")],
                total=1,
            )
            result = CliRunner().invoke(cli, ["eval", "list", "--api_key", "k"])
        assert result.exit_code == 0, result.output
        assert "1 Eval(s):" in result.output
        assert "1: completed (pipeline pl_1)" in result.output

    def test_eval_differences_prints_places(self):
        from datalab_sdk.models import EvalDifference, EvalDifferences

        with patch("datalab_sdk.cli.DatalabClient") as client_cls:
            client = client_cls.return_value
            client.get_eval_differences.return_value = EvalDifferences(
                file_id=20,
                variant_names=["Fast", "Accurate"],
                differences=[
                    EvalDifference(
                        page=0,
                        before="",
                        after="",
                        question="vendor_name",
                        winners=[1],
                        verdict="decided",
                    )
                ],
            )
            result = CliRunner().invoke(
                cli, ["eval", "differences", "7", "20", "--api_key", "k"]
            )
        assert result.exit_code == 0, result.output
        assert "File 20: variants Fast, Accurate" in result.output
        assert "vendor_name" in result.output
        client.get_eval_differences.assert_called_once_with(7, 20)
