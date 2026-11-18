"""
Tests for the comparison methods (open beta; the API may change): request
payloads, response parsing, the exactly-one-of validation, wait_for_comparison,
the CLI, and the deprecated eval names of SDK 0.10.0.
"""

import warnings
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from datalab_sdk import AsyncDatalabClient, DatalabClient
from datalab_sdk.cli import cli
from datalab_sdk.exceptions import DatalabTimeoutError, DatalabValidationError
from datalab_sdk.models import (
    Comparison,
    ComparisonDifferences,
    ComparisonList,
    ComparisonVariant,
    ComparisonVariantResult,
)

RESPONSE = {
    "comparison_id": 1,
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

EXTRACT_RESPONSE = {
    "comparison_id": 2,
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
async def test_create_comparison_sends_file_ids_and_variants():
    async with AsyncDatalabClient(api_key="k") as client:
        with patch.object(client, "_make_request", new_callable=AsyncMock) as req:
            req.return_value = RESPONSE
            result = await client.create_comparison(
                "pl_1",
                file_ids=[10, 11],
                variants=[
                    ComparisonVariant(
                        name="Fast", steps={"convert": {"output_mode": "fast"}}
                    ),
                    ComparisonVariant(
                        name="Accurate", steps={"convert": {"output_mode": "accurate"}}
                    ),
                ],
                version=0,
            )
    method, endpoint = req.await_args.args
    assert (method, endpoint) == ("POST", "/api/v1/pipelines/pl_1/comparisons")
    assert req.await_args.kwargs["json"] == {
        "file_ids": [10, 11],
        "variants": [
            {"name": "Fast", "steps": {"convert": {"output_mode": "fast"}}},
            {"name": "Accurate", "steps": {"convert": {"output_mode": "accurate"}}},
        ],
        "version": 0,
    }
    assert isinstance(result, Comparison)
    assert result.comparison_id == 1
    assert result.variants[0].name == "Fast"


@pytest.mark.asyncio
async def test_create_comparison_sends_collection_id():
    async with AsyncDatalabClient(api_key="k") as client:
        with patch.object(client, "_make_request", new_callable=AsyncMock) as req:
            req.return_value = RESPONSE
            await client.create_comparison("pl_1", collection_id=5)
    assert req.await_args.kwargs["json"] == {"collection_id": 5}


@pytest.mark.asyncio
async def test_create_comparison_needs_exactly_one_of_file_ids_or_collection_id():
    async with AsyncDatalabClient(api_key="k") as client:
        with pytest.raises(DatalabValidationError):
            await client.create_comparison("pl_1")
        with pytest.raises(DatalabValidationError):
            await client.create_comparison("pl_1", file_ids=[1], collection_id=2)


@pytest.mark.asyncio
async def test_get_comparison_parses_extract_comparison_with_documents_and_files():
    async with AsyncDatalabClient(api_key="k") as client:
        with patch.object(client, "_make_request", new_callable=AsyncMock) as req:
            req.return_value = EXTRACT_RESPONSE
            result = await client.get_comparison(2)
    assert req.await_args.args == ("GET", "/api/v1/comparisons/2")
    assert isinstance(result, Comparison)
    assert result.compared_step == "extract"
    assert result.files[0].file_name == "a.pdf"
    variant = result.variants[0]
    assert isinstance(variant, ComparisonVariantResult)
    assert variant.extract_settings == {"extraction_mode": "balanced"}
    assert variant.share == 0.8
    assert variant.rate_per_1000_pages_cents == 2000.0
    assert variant.documents[0].lookup_key == "lk_1"
    assert variant.documents[0].undecided == 1


@pytest.mark.asyncio
async def test_list_comparisons_sends_query_params_and_parses_results():
    async with AsyncDatalabClient(api_key="k") as client:
        with patch.object(client, "_make_request", new_callable=AsyncMock) as req:
            req.return_value = {"comparisons": [RESPONSE], "total": 1}
            result = await client.list_comparisons(
                pipeline_id="pl_1", collection_id=5, limit=10, offset=2
            )
    method, endpoint = req.await_args.args
    assert method == "GET"
    assert (
        endpoint
        == "/api/v1/comparisons?limit=10&offset=2&pipeline_id=pl_1&collection_id=5"
    )
    assert isinstance(result, ComparisonList)
    assert result.total == 1
    assert isinstance(result.comparisons[0], Comparison)


@pytest.mark.asyncio
async def test_get_comparison_differences_parses_question_and_description():
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
            result = await client.get_comparison_differences(2, 20)
    assert req.await_args.args == ("GET", "/api/v1/comparisons/2/files/20/differences")
    assert isinstance(result, ComparisonDifferences)
    assert result.differences[0].question == "vendor_name"
    assert result.differences[0].description == "The name of the vendor on the invoice"
    assert result.differences[0].winners == [1]
    # Every compared field, equal or not, with each variant's value.
    assert [f.path for f in result.fields] == ["vendor_name", "total"]
    assert result.fields[1].values == ["100", "100"]


@pytest.mark.asyncio
async def test_wait_for_comparison_polls_until_completed():
    async with AsyncDatalabClient(api_key="k") as client:
        running = dict(RESPONSE)
        completed = {**RESPONSE, "status": "completed"}
        with (
            patch.object(
                client, "get_comparison", new_callable=AsyncMock
            ) as get_comparison,
            patch("asyncio.sleep", new_callable=AsyncMock) as sleep,
        ):
            get_comparison.side_effect = [
                client._build_comparison(running),
                client._build_comparison(running),
                client._build_comparison(completed),
            ]
            result = await client.wait_for_comparison(1, poll_interval=5, timeout=60)
    assert result.status == "completed"
    assert get_comparison.await_count == 3
    assert sleep.await_count == 2


@pytest.mark.asyncio
async def test_wait_for_comparison_times_out():
    async with AsyncDatalabClient(api_key="k") as client:
        with (
            patch.object(
                client, "get_comparison", new_callable=AsyncMock
            ) as get_comparison,
            patch("asyncio.sleep", new_callable=AsyncMock),
        ):
            get_comparison.return_value = client._build_comparison(
                RESPONSE
            )  # always "running"
            with pytest.raises(DatalabTimeoutError):
                await client.wait_for_comparison(1, poll_interval=10, timeout=15)


def test_sync_create_comparison_forwards():
    client = DatalabClient(api_key="k")
    with patch.object(
        client._async_client, "create_comparison", new_callable=AsyncMock
    ) as create:
        create.return_value = client._async_client._build_comparison(RESPONSE)
        client.create_comparison("pl_1", file_ids=[1], version=0)
    assert create.await_args.kwargs == {
        "file_ids": [1],
        "collection_id": None,
        "variants": None,
        "version": 0,
    }
    assert create.await_args.args == ("pl_1",)


def test_sync_wait_for_comparison_forwards():
    client = DatalabClient(api_key="k")
    with patch.object(
        client._async_client, "wait_for_comparison", new_callable=AsyncMock
    ) as wait:
        wait.return_value = client._async_client._build_comparison(RESPONSE)
        client.wait_for_comparison(1, poll_interval=3, timeout=30)
    assert wait.await_args.args == (1,)
    assert wait.await_args.kwargs == {"poll_interval": 3, "timeout": 30}


class TestCompareCLI:
    def test_compare_run_uploads_files_and_creates_comparison(self, tmp_path):
        doc = tmp_path / "a.pdf"
        doc.write_bytes(b"%PDF-1.4\n%%EOF\n")
        uploaded = MagicMock(file_id=42)
        with patch("datalab_sdk.cli.DatalabClient") as client_cls:
            client = client_cls.return_value
            client.upload_files.return_value = [uploaded]
            client.create_comparison.return_value = MagicMock(
                comparison_id=7, status="running"
            )
            result = CliRunner().invoke(
                cli,
                ["compare", "run", str(doc), "--pipeline", "pl_1", "--api_key", "k"],
            )
        assert result.exit_code == 0, result.output
        assert "Comparison 7 started" in result.output
        client.upload_files.assert_called_once_with([str(doc)])
        assert client.create_comparison.call_args.kwargs["file_ids"] == [42]
        assert client.create_comparison.call_args.kwargs["collection_id"] is None

    def test_compare_run_needs_paths_or_collection_not_both(self, tmp_path):
        doc = tmp_path / "a.pdf"
        doc.write_bytes(b"%PDF-1.4\n%%EOF\n")
        result = CliRunner().invoke(
            cli,
            [
                "compare",
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

    def test_compare_run_with_wait_prints_summary(self, tmp_path):
        from datalab_sdk.models import Comparison as ComparisonModel
        from datalab_sdk.models import ComparisonVariantResult

        doc = tmp_path / "a.pdf"
        doc.write_bytes(b"%PDF-1.4\n%%EOF\n")
        with patch("datalab_sdk.cli.DatalabClient") as client_cls:
            client = client_cls.return_value
            client.upload_files.return_value = [MagicMock(file_id=1)]
            client.create_comparison.return_value = MagicMock(
                comparison_id=7, status="running"
            )
            client.wait_for_comparison.return_value = ComparisonModel(
                comparison_id=7,
                pipeline_id="pl_1",
                status="completed",
                compared_step="convert",
                variants=[
                    ComparisonVariantResult(
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
                    "compare",
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
        assert "Comparison 7: completed" in result.output
        assert "Accurate: share=90%" in result.output

    def test_compare_get_prints_summary(self):
        # Use the real Comparison dataclass for a faithful summary line.
        from datalab_sdk.models import Comparison as ComparisonModel

        with patch("datalab_sdk.cli.DatalabClient") as client_cls:
            client = client_cls.return_value
            client.get_comparison.return_value = ComparisonModel(
                comparison_id=7,
                pipeline_id="pl_1",
                status="completed",
                variants=[],
            )
            result = CliRunner().invoke(cli, ["compare", "get", "7", "--api_key", "k"])
        assert result.exit_code == 0, result.output
        assert "Comparison 7: completed" in result.output
        client.get_comparison.assert_called_once_with(7)

    def test_compare_list_prints_count(self):
        from datalab_sdk.models import Comparison as ComparisonModel
        from datalab_sdk.models import ComparisonList as ComparisonListModel

        with patch("datalab_sdk.cli.DatalabClient") as client_cls:
            client = client_cls.return_value
            client.list_comparisons.return_value = ComparisonListModel(
                comparisons=[
                    ComparisonModel(
                        comparison_id=1, pipeline_id="pl_1", status="completed"
                    )
                ],
                total=1,
            )
            result = CliRunner().invoke(cli, ["compare", "list", "--api_key", "k"])
        assert result.exit_code == 0, result.output
        assert "1 comparison(s):" in result.output
        assert "1: completed (pipeline pl_1)" in result.output

    def test_compare_differences_prints_places(self):
        from datalab_sdk.models import ComparisonDifference, ComparisonDifferences

        with patch("datalab_sdk.cli.DatalabClient") as client_cls:
            client = client_cls.return_value
            client.get_comparison_differences.return_value = ComparisonDifferences(
                file_id=20,
                variant_names=["Fast", "Accurate"],
                differences=[
                    ComparisonDifference(
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
                cli, ["compare", "differences", "7", "20", "--api_key", "k"]
            )
        assert result.exit_code == 0, result.output
        assert "File 20: variants Fast, Accurate" in result.output
        assert "vendor_name" in result.output
        client.get_comparison_differences.assert_called_once_with(7, 20)


# --- The deprecated eval names of SDK 0.10.0 ---

LEGACY_RESPONSE = {
    "eval_id": 1,
    **{k: v for k, v in RESPONSE.items() if k != "comparison_id"},
}


def test_the_eval_model_names_are_aliases_of_the_comparison_models():
    import datalab_sdk
    from datalab_sdk import models

    assert datalab_sdk.Eval is Comparison
    assert datalab_sdk.EvalVariant is ComparisonVariant
    assert models.EvalList is ComparisonList
    assert models.EvalDifferences is ComparisonDifferences
    comparison = Comparison(comparison_id=3, pipeline_id="pl_1", status="running")
    assert comparison.eval_id == 3
    assert ComparisonList(comparisons=[comparison], total=1).evals == [comparison]


def test_a_response_with_eval_id_parses():
    comparison = AsyncDatalabClient._build_comparison(LEGACY_RESPONSE)
    assert comparison.comparison_id == 1 and comparison.eval_id == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("old", "new", "args"),
    [
        ("create_eval", "create_comparison", ("pl_1",)),
        ("get_eval", "get_comparison", (1,)),
        ("list_evals", "list_comparisons", ()),
        ("get_eval_differences", "get_comparison_differences", (1, 20)),
        ("wait_for_eval", "wait_for_comparison", (1,)),
    ],
)
async def test_the_async_eval_methods_warn_and_call_the_comparison_methods(
    old, new, args
):
    async with AsyncDatalabClient(api_key="k") as client:
        with patch.object(client, new, new_callable=AsyncMock) as method:
            method.return_value = "result"
            kwargs = {"file_ids": [1]} if old == "create_eval" else {}
            with pytest.warns(DeprecationWarning, match=f"Use {new}"):
                assert await getattr(client, old)(*args, **kwargs) == "result"
    assert method.await_args.args == args


@pytest.mark.parametrize(
    ("old", "new", "args"),
    [
        ("create_eval", "create_comparison", ("pl_1",)),
        ("get_eval", "get_comparison", (1,)),
        ("list_evals", "list_comparisons", ()),
        ("get_eval_differences", "get_comparison_differences", (1, 20)),
        ("wait_for_eval", "wait_for_comparison", (1,)),
    ],
)
def test_the_sync_eval_methods_warn_and_call_the_comparison_methods(old, new, args):
    client = DatalabClient(api_key="k")
    with patch.object(client._async_client, new, new_callable=AsyncMock) as method:
        method.return_value = "result"
        kwargs = {"file_ids": [1]} if old == "create_eval" else {}
        with pytest.warns(DeprecationWarning, match=f"Use {new}"):
            assert getattr(client, old)(*args, **kwargs) == "result"
    assert method.await_args.args == args


def test_the_comparison_methods_do_not_warn():
    client = DatalabClient(api_key="k")
    with (
        patch.object(
            client._async_client, "_make_request", new_callable=AsyncMock
        ) as req,
        warnings.catch_warnings(),
    ):
        warnings.simplefilter("error", DeprecationWarning)
        req.return_value = RESPONSE
        assert client.get_comparison(1).comparison_id == 1


class TestDeprecatedEvalCLI:
    def test_eval_is_hidden_from_the_help(self):
        result = CliRunner().invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "compare" in result.output
        assert " eval " not in result.output

    def test_eval_get_prints_a_note_and_runs_compare_get(self):
        with patch("datalab_sdk.cli.DatalabClient") as client_cls:
            client = client_cls.return_value
            client.get_comparison.return_value = Comparison(
                comparison_id=7, pipeline_id="pl_1", status="completed"
            )
            result = CliRunner().invoke(cli, ["eval", "get", "7", "--api_key", "k"])
        assert result.exit_code == 0, result.output
        assert "`datalab eval` is deprecated. Use `datalab compare`" in result.output
        assert "Comparison 7: completed" in result.output
        client.get_comparison.assert_called_once_with(7)

    def test_eval_json_keeps_the_old_keys(self):
        import json

        with patch("datalab_sdk.cli.DatalabClient") as client_cls:
            client = client_cls.return_value
            client.list_comparisons.return_value = ComparisonList(
                comparisons=[
                    Comparison(comparison_id=1, pipeline_id="pl_1", status="completed")
                ],
                total=1,
            )
            old = CliRunner().invoke(cli, ["eval", "list", "--json", "--api_key", "k"])
            new = CliRunner().invoke(
                cli, ["compare", "list", "--json", "--api_key", "k"]
            )
        assert old.exit_code == 0 and new.exit_code == 0
        old_json = json.loads(old.stdout)
        assert old_json["evals"][0]["eval_id"] == 1 and "comparisons" not in old_json
        assert json.loads(new.stdout)["comparisons"][0]["comparison_id"] == 1
