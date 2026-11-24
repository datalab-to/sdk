"""Where the CLI saves results, and which files it picks up.

Each input gets its own output folder, named by its path relative to the
input root, with the extension kept, so inputs with the same stem can't
overwrite each other. A directory scan skips earlier output.
"""

import base64
import os
import json
import re
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest
from click.testing import CliRunner

from datalab_sdk.cli import cli
from datalab_sdk.exceptions import DatalabError
from datalab_sdk.settings import settings

PNG = base64.b64encode(b"\x89PNG\r\n\x1a\nnot-really").decode()


class FakeConvertAPI:
    """Converts each upload to markdown that names the upload's marker, with an image and HTML."""

    def __init__(self):
        self.submitted = []
        self.results = {}
        api = self

        class Handler(BaseHTTPRequestHandler):
            def _json(self, body):
                data = json.dumps(body).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                match = re.search(rb"MARKER-[\w-]+", body)
                marker = match.group().decode() if match else "NO-MARKER"
                api.submitted.append(marker)
                request_id = f"req-{len(api.submitted)}"
                api.results[request_id] = marker
                self._json(
                    {
                        "success": True,
                        "request_id": request_id,
                        "request_check_url": f"{api.url}/api/v1/convert/{request_id}",
                    }
                )

            def do_GET(self):
                marker = api.results[self.path.rsplit("/", 1)[1]]
                if "FAIL" in marker:
                    self._json(
                        {"status": "failed", "success": False, "error": "bad file"}
                    )
                    return
                self._json(
                    {
                        "status": "complete",
                        "success": True,
                        "output_format": "markdown,html",
                        "markdown": f"# {marker}",
                        "html": f"<h1>{marker}</h1>",
                        "images": {"figure.png": PNG},
                        "page_count": 1,
                    }
                )

            def log_message(self, *args):
                pass

        self.httpd = HTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


@pytest.fixture
def api(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setattr(settings, "DATALAB_API_KEY", "local-test-only")
    server = FakeConvertAPI()
    yield server
    server.close()


def _write(path: Path, marker: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(f"%PDF-1.4 {marker}\n%%EOF\n".encode())
    return path


def _invoke(api, *args):
    return CliRunner().invoke(
        cli,
        ["convert", *args, "--base_url", api.url, "--format", "markdown,html"],
        catch_exceptions=False,
    )


def _convert(api, *args):
    result = _invoke(api, *args)
    assert result.exit_code == 0, result.output
    return result


@pytest.fixture
def inputs(tmp_path):
    root = tmp_path / "data"
    _write(root / "a" / "report.pdf", "MARKER-a-pdf")
    _write(root / "b" / "report.pdf", "MARKER-b-pdf")
    _write(root / "report.docx", "MARKER-docx")
    return root


def test_inputs_with_the_same_stem_keep_their_own_results(api, inputs, tmp_path):
    out = tmp_path / "out"
    _convert(api, str(inputs), "--output_dir", str(out))

    for relative, marker in [
        ("a/report", "MARKER-a-pdf"),
        ("b/report", "MARKER-b-pdf"),
        ("report", "MARKER-docx"),
    ]:
        folder = out / relative
        assert (folder / "report.md").read_text() == f"# {marker}"
        assert (folder / "report.html").read_text() == f"<h1>{marker}</h1>"
        assert (folder / "figure.png").exists()


def test_a_single_file_saves_to_a_folder_named_after_its_stem(api, inputs, tmp_path):
    out = tmp_path / "out"
    _convert(api, str(inputs / "report.docx"), "--output_dir", str(out))
    assert (out / "report" / "report.md").read_text() == "# MARKER-docx"


def test_the_same_stem_in_one_folder_stops_the_run_before_submitting(api, tmp_path):
    root = tmp_path / "data"
    _write(root / "a" / "report.pdf", "MARKER-pdf")
    _write(root / "a" / "report.html", "MARKER-html")
    result = _invoke(api, str(root), "--output_dir", str(tmp_path / "out"))
    assert result.exit_code == 1
    assert "would save to the same output folder" in result.output
    assert "report.html" in result.output and "report.pdf" in result.output
    assert "--extensions" in result.output
    assert api.submitted == []

    # The suggested flag gets past it.
    _convert(
        api, str(root), "--output_dir", str(tmp_path / "out"), "--extensions", "pdf"
    )
    assert api.submitted == ["MARKER-pdf"]


def test_separate_runs_with_the_same_stem_dont_overwrite_each_other(api, tmp_path):
    # For example: for f in data/*; do datalab convert "$f" -o out; done
    root = tmp_path / "data"
    pdf = _write(root / "report.pdf", "MARKER-pdf")
    docx = _write(root / "report.docx", "MARKER-docx")
    out = tmp_path / "out"
    _convert(api, str(pdf), "--output_dir", str(out))
    result = _invoke(api, str(docx), "--output_dir", str(out))
    assert result.exit_code == 1
    assert "holds the results of report.pdf" in result.output
    assert "--extensions" in result.output
    assert api.submitted == ["MARKER-pdf"]
    assert (out / "report" / "report.md").read_text() == "# MARKER-pdf"


def test_a_rerun_of_the_same_file_reuses_its_folder(api, tmp_path):
    doc = _write(tmp_path / "data" / "report.pdf", "MARKER-first")
    out = tmp_path / "out"
    _convert(api, str(doc), "--output_dir", str(out))
    _write(doc, "MARKER-second")
    _convert(api, str(doc), "--output_dir", str(out))
    assert (out / "report" / "report.md").read_text() == "# MARKER-second"
    assert (out / "report" / ".datalab-output").read_text() == "report.pdf"


def test_an_empty_marker_can_be_reused(api, tmp_path):
    doc = _write(tmp_path / "data" / "report.pdf", "MARKER-doc")
    out = tmp_path / "out"
    (out / "report").mkdir(parents=True)
    (out / "report" / ".datalab-output").touch()
    _convert(api, str(doc), "--output_dir", str(out))
    assert (out / "report" / "report.md").read_text() == "# MARKER-doc"


def test_a_second_run_with_the_default_output_folder_skips_the_first_runs_output(
    api, inputs, monkeypatch
):
    monkeypatch.chdir(inputs)
    first = _convert(api, ".")
    second = _convert(api, ".")

    assert sorted(api.submitted) == sorted(
        ["MARKER-a-pdf", "MARKER-b-pdf", "MARKER-docx"] * 2
    )
    assert "Found 3 files" in first.output and "Found 3 files" in second.output
    # The default output folder is its own folder, not the input tree.
    assert (inputs / "output" / "a" / "report" / "report.md").exists()
    assert not (inputs / "a" / "report").exists()


def test_a_second_run_skips_output_saved_into_the_input_tree(api, inputs, tmp_path):
    # Output saved inside the input tree, then a run with another output folder.
    _convert(api, str(inputs), "--output_dir", str(inputs / "results"))
    _convert(api, str(inputs), "--output_dir", str(tmp_path / "elsewhere"))
    assert len(api.submitted) == 6


def test_output_inside_the_input_tree_is_skipped_even_as_the_output_root(api, inputs):
    _convert(api, str(inputs), "--output_dir", str(inputs))
    _convert(api, str(inputs), "--output_dir", str(inputs))
    assert len(api.submitted) == 6
    assert (inputs / "a" / "report" / "report.md").exists()


def test_output_saved_beside_the_inputs_is_skipped(api, inputs, monkeypatch):
    # Run inside the data folder with -o . for one file, then scan the folder.
    monkeypatch.chdir(inputs / "a")
    _convert(api, "report.pdf", "--output_dir", ".")
    assert (inputs / "a" / "report.pdf").is_file()
    _convert(api, str(inputs), "--output_dir", str(inputs.parent / "out"))
    assert len(api.submitted) == 4


def test_output_folders_are_checked_before_anything_is_submitted(tmp_path):
    from datalab_sdk._output_layout import OutputLayout

    root = tmp_path / "data"
    folders = OutputLayout.plan(
        [root / "a" / "report.pdf", root / "b" / "report.pdf"], root, tmp_path / "out"
    ).folders
    assert folders == [
        tmp_path / "out" / "a" / "report",
        tmp_path / "out" / "b" / "report",
    ]


@pytest.mark.parametrize("ignores_case", [True, False])
def test_names_that_differ_only_in_case(tmp_path, monkeypatch, ignores_case):
    from datalab_sdk import _output_layout

    monkeypatch.setattr(_output_layout, "ignores_case", lambda directory: ignores_case)
    root, out = tmp_path / "data", tmp_path / "out"
    files = [root / "Report.pdf", root / "report.pdf"]
    if ignores_case:
        with pytest.raises(DatalabError, match="ignores case.*another --output_dir"):
            _output_layout.OutputLayout.plan(files, root, out)
    else:
        assert _output_layout.OutputLayout.plan(files, root, out).folders == [
            out / "Report",
            out / "report",
        ]


def test_the_case_probe_matches_this_file_system(tmp_path):
    from datalab_sdk._output_layout import ignores_case

    (tmp_path / "Probe").touch()
    assert ignores_case(tmp_path) == (tmp_path / "probe").exists()
    assert ignores_case(tmp_path / "not" / "made" / "yet") == ignores_case(tmp_path)


@pytest.mark.parametrize(
    "names",
    [
        ["report.pdf", "report/appendix.pdf"],
        ["x.pdf", "x-y.pdf", "x/z.pdf"],
    ],
)
def test_a_folder_inside_another_inputs_folder_is_rejected(tmp_path, names):
    from datalab_sdk._output_layout import OutputLayout

    root = tmp_path / "data"
    with pytest.raises(DatalabError, match="inside the output folder"):
        OutputLayout.plan([root / n for n in names], root, tmp_path / "out")


def test_nested_output_stops_the_cli_before_submitting(api, tmp_path):
    root = tmp_path / "data"
    _write(root / "report.pdf", "MARKER-outer")
    _write(root / "report" / "appendix.pdf", "MARKER-inner")
    result = _invoke(api, str(root), "--output_dir", str(tmp_path / "out"))
    assert result.exit_code == 1
    assert "inside the output folder" in result.output
    assert api.submitted == []


def test_an_output_folder_inside_the_input_tree_still_scans_its_documents(api, inputs):
    # Documents already in the output root are inputs, not output.
    _convert(api, str(inputs), "--output_dir", str(inputs / "b"))
    assert len(api.submitted) == 3
    assert (inputs / "b" / "a" / "report" / "report.md").exists()


def test_an_existing_folder_that_is_not_output_stops_the_run_before_submitting(
    api, inputs, tmp_path
):
    out = tmp_path / "out"
    (out / "a" / "report").mkdir(parents=True)
    (out / "a" / "report" / "notes.txt").write_text("mine")
    result = _invoke(api, str(inputs), "--output_dir", str(out))
    assert result.exit_code == 1
    assert "is not a datalab output folder" in result.output
    assert api.submitted == []
    assert not (out / "a" / "report" / ".datalab-output").exists()


def test_a_blocked_output_path_stops_the_run_before_submitting(api, inputs, tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    (out / "a").write_text("a file where a folder must go")
    result = _invoke(api, str(inputs), "--output_dir", str(out))
    assert result.exit_code == 1
    assert "Can't create the output folder" in result.output
    assert api.submitted == []


def test_a_failed_request_leaves_no_output_folder(api, tmp_path):
    root = tmp_path / "data"
    _write(root / "x" / "bad.pdf", "MARKER-FAIL")
    _write(root / "good.pdf", "MARKER-good")
    out = tmp_path / "out"
    _invoke(api, str(root), "--output_dir", str(out))
    assert not (out / "x").exists()
    assert (out / "good" / "good.md").exists()


def test_a_partial_save_is_still_skipped_by_the_next_scan(api, inputs, monkeypatch):
    from datalab_sdk.models import ConversionResult

    def save_html_then_fail(self, output_path, save_images=True):
        Path(output_path).with_suffix(".html").write_text(self.html)
        raise OSError("disk full")

    with monkeypatch.context() as patched:
        patched.setattr(ConversionResult, "save_output", save_html_then_fail)
        _invoke(api, str(inputs), "--output_dir", str(inputs / "results"))
    assert (inputs / "results" / "a" / "report" / "report.html").exists()
    _convert(api, str(inputs), "--output_dir", str(inputs.parent / "elsewhere"))
    assert len(api.submitted) == 6


def test_a_directory_scan_skips_entries_that_are_not_regular_files(api, tmp_path):
    root = tmp_path / "data"
    _write(root / "real.pdf", "MARKER-real")
    os.mkfifo(root / "pending.pdf")
    (root / "dangling.pdf").symlink_to(root / "missing.pdf")
    _convert(api, str(root), "--output_dir", str(tmp_path / "out"))
    assert api.submitted == ["MARKER-real"]


def test_a_rerun_keeps_a_file_name_with_surrounding_spaces(api, tmp_path):
    doc = _write(tmp_path / "data" / " report.pdf", "MARKER-first")
    out = tmp_path / "out"
    _convert(api, str(doc), "--output_dir", str(out))
    _write(doc, "MARKER-second")
    _convert(api, str(doc), "--output_dir", str(out))
    assert (out / " report" / " report.md").read_text() == "# MARKER-second"


def test_ctrl_c_while_creating_folders_removes_them(api, inputs, tmp_path, monkeypatch):
    from datalab_sdk import _output_layout

    real_write_text = Path.write_text
    markers = []

    def interrupt_on_second_marker(self, *args, **kwargs):
        if self.name == _output_layout.OUTPUT_MARKER:
            markers.append(self)
            if len(markers) == 2:
                raise KeyboardInterrupt
        return real_write_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", interrupt_on_second_marker)
    out = tmp_path / "out"
    result = CliRunner().invoke(
        cli, ["convert", str(inputs), "--base_url", api.url, "--output_dir", str(out)]
    )
    assert result.exit_code != 0
    assert api.submitted == []
    assert not out.exists()


@pytest.mark.parametrize(
    "names,mentions_case",
    [(["doc.pdf", "doc.png"], False), (["Doc.png", "doc.pdf"], True)],
)
def test_the_case_reason_is_given_only_when_names_differ_by_case(
    tmp_path, monkeypatch, names, mentions_case
):
    from datalab_sdk import _output_layout

    monkeypatch.setattr(_output_layout, "ignores_case", lambda directory: True)
    root = tmp_path / "data"
    with pytest.raises(DatalabError, match="same output folder") as info:
        _output_layout.OutputLayout.plan(
            [root / n for n in names], root, tmp_path / "out"
        )
    assert ("ignores case" in str(info.value)) is mentions_case
