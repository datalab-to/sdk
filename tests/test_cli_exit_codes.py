"""The CLI exits nonzero when a file or a comparison fails, and its summary
names an output folder only when something was saved."""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from datalab_sdk.cli import cli
from datalab_sdk.models import Comparison, ComparisonFile
from datalab_sdk.settings import settings
from tests.test_cli_output_layout import FakeConvertAPI, _write


@pytest.fixture
def api(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setattr(settings, "DATALAB_API_KEY", "local-test-only")
    server = FakeConvertAPI()
    yield server
    server.close()


def _convert(api, path, out):
    return CliRunner().invoke(
        cli,
        ["convert", str(path), "--base_url", api.url, "--output_dir", str(out)],
        catch_exceptions=False,
    )


def test_a_successful_run_exits_zero_and_names_the_output_folder(api, tmp_path):
    doc = _write(tmp_path / "data" / "good.pdf", "MARKER-good")
    out = tmp_path / "out"
    result = _convert(api, doc, out)
    assert result.exit_code == 0, result.output
    assert f"Output saved to: {out}" in result.output


def test_a_failed_single_file_exits_nonzero_and_saves_nothing(api, tmp_path):
    doc = _write(tmp_path / "data" / "bad.pdf", "MARKER-FAIL")
    out = tmp_path / "out"
    result = _convert(api, doc, out)
    assert result.exit_code == 1, result.output
    assert "bad file" in result.output
    assert "Output saved to" not in result.output
    assert "No output was saved" in result.output
    # The run leaves no empty output folder behind.
    assert not out.exists()


def test_a_batch_with_some_failures_exits_nonzero(api, tmp_path):
    root = tmp_path / "data"
    _write(root / "good.pdf", "MARKER-good")
    _write(root / "bad.pdf", "MARKER-FAIL")
    out = tmp_path / "out"
    result = _convert(api, root, out)
    assert result.exit_code == 1, result.output
    assert "Successfully processed: 1 files" in result.output
    assert "Failed: 1 files" in result.output
    assert f"Output saved to: {out}" in result.output
    assert (out / "good" / "good.md").exists()


def test_a_failed_run_keeps_an_output_folder_that_already_existed(api, tmp_path):
    doc = _write(tmp_path / "data" / "bad.pdf", "MARKER-FAIL")
    out = tmp_path / "out"
    out.mkdir()
    result = _convert(api, doc, out)
    assert result.exit_code == 1
    assert out.is_dir()


def _comparison(status, file_statuses):
    return Comparison(
        comparison_id=7,
        pipeline_id="pl_1",
        status=status,
        compared_step="convert",
        variants=[],
        files=[
            ComparisonFile(
                file_id=i,
                file_name=f"{file_status}_{i}.pdf",
                comparison=file_status,
                comparison_error="a variant crashed"
                if file_status == "failed"
                else None,
            )
            for i, file_status in enumerate(file_statuses)
        ],
    )


@pytest.mark.parametrize(
    "status,file_statuses,exit_code,message",
    [
        ("completed", ["completed"], 0, None),
        ("completed", ["completed", "skipped"], 0, None),
        ("failed", ["failed"], 1, "Comparison 7 failed"),
        ("completed", ["completed", "failed"], 1, "1 of 2 files failed"),
    ],
)
@pytest.mark.parametrize("command", ["compare", "eval"])
def test_compare_run_wait_exits_nonzero_when_the_comparison_or_a_file_fails(
    tmp_path, status, file_statuses, exit_code, message, command
):
    doc = tmp_path / "a.pdf"
    doc.write_bytes(b"%PDF-1.4\n%%EOF\n")
    with patch("datalab_sdk.cli.DatalabClient") as client_cls:
        client = client_cls.return_value
        client.upload_files.return_value = [MagicMock(file_id=1)]
        client.create_comparison.return_value = MagicMock(
            comparison_id=7, status="running"
        )
        client.wait_for_comparison.return_value = _comparison(status, file_statuses)
        for extra in ([], ["--json"]):
            result = CliRunner().invoke(
                cli,
                [command, "run", str(doc), "--pipeline", "pl_1", "--api_key", "k"]
                + ["--wait"]
                + extra,
            )
            assert result.exit_code == exit_code, result.output
            if message and not extra:
                assert message in result.output
                assert ".pdf: a variant crashed" in result.output


def test_a_failed_run_removes_every_output_folder_it_made(api, tmp_path):
    doc = _write(tmp_path / "data" / "bad.pdf", "MARKER-FAIL")
    (tmp_path / "results").mkdir()
    result = _convert(api, doc, tmp_path / "results" / "nested" / "run1")
    assert result.exit_code == 1
    assert list((tmp_path / "results").iterdir()) == []


def test_folders_are_removed_when_the_run_stops_early(api, tmp_path, monkeypatch):
    from datalab_sdk import cli as cli_module

    root = tmp_path / "data"
    _write(root / "a.pdf", "MARKER-a")
    _write(root / "b.pdf", "MARKER-b")

    async def interrupted(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli_module, "process_files_async", interrupted)
    out = tmp_path / "out"
    result = CliRunner().invoke(
        cli, ["convert", str(root), "--base_url", api.url, "--output_dir", str(out)]
    )
    assert result.exit_code != 0
    assert not out.exists()


def test_a_partial_save_is_reported_as_saved(api, tmp_path, monkeypatch):
    from pathlib import Path

    from datalab_sdk.models import ConversionResult

    def save_markdown_then_fail(self, output_path, save_images=True):
        Path(output_path).with_suffix(".md").write_text(self.markdown)
        raise OSError("disk full")

    monkeypatch.setattr(ConversionResult, "save_output", save_markdown_then_fail)
    doc = _write(tmp_path / "data" / "doc.pdf", "MARKER-doc")
    out = tmp_path / "out"
    result = _convert(api, doc, out)
    assert result.exit_code == 1
    assert f"Output saved to: {out}" in result.output
    assert (out / "doc" / "doc.md").exists()


def test_a_success_that_wrote_nothing_is_not_reported_as_saved(
    api, tmp_path, monkeypatch
):
    from datalab_sdk.models import ConversionResult

    monkeypatch.setattr(
        ConversionResult, "save_output", lambda self, path, save_images=True: None
    )
    doc = _write(tmp_path / "data" / "doc.pdf", "MARKER-doc")
    out = tmp_path / "out"
    result = _convert(api, doc, out)
    assert result.exit_code == 0
    assert "No output was saved" in result.output
    assert not out.exists()


def test_a_failed_rerun_does_not_report_an_earlier_runs_results_as_saved(api, tmp_path):
    root = tmp_path / "data"
    doc = _write(root / "doc.pdf", "MARKER-doc")
    out = tmp_path / "out"
    assert _convert(api, doc, out).exit_code == 0
    _write(doc, "MARKER-FAIL")
    result = _convert(api, doc, out)
    assert result.exit_code == 1
    assert "No output was saved" in result.output
    # The earlier run's results stay.
    assert (out / "doc" / "doc.md").read_text() == "# MARKER-doc"


def test_a_folder_whose_marker_failed_is_removed(api, tmp_path, monkeypatch):
    from pathlib import Path

    from datalab_sdk import cli as cli_module

    real_write_text = Path.write_text

    def failing_write_text(self, *args, **kwargs):
        if self.name == cli_module.OUTPUT_MARKER:
            raise OSError("read-only")
        return real_write_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", failing_write_text)
    doc = _write(tmp_path / "data" / "doc.pdf", "MARKER-doc")
    out = tmp_path / "out"
    result = _convert(api, doc, out)
    assert result.exit_code == 1
    assert "Can't create the output folder" in result.output
    assert api.submitted == []
    assert not out.exists()


def test_the_full_output_path_is_printed_at_the_start_and_in_the_summary(
    api, tmp_path, monkeypatch
):
    _write(tmp_path / "data" / "good.pdf", "MARKER-good")
    monkeypatch.chdir(tmp_path)
    result = _convert(api, "data/good.pdf", "results")
    assert result.exit_code == 0, result.output
    full = (tmp_path / "results").absolute()
    lines = result.output.splitlines()
    assert lines.index(f"Saving results to: {full}") < lines.index(
        f"Output saved to: {full}"
    )
