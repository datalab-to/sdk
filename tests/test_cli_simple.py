"""
Simple tests for the CLI module
"""

from unittest.mock import patch
import tempfile
import os
from click.testing import CliRunner

from datalab_sdk.cli import cli
from datalab_sdk.settings import settings


ASYNC_RETURN_VALUE = [
    {
        "success": True,
        "file_path": "/tmp/test1.pdf",
        "output_path": "/tmp/output/test1.txt",
        "error": None,
        "page_count": 2,
    },
    {
        "success": True,
        "file_path": "/tmp/test2.pdf",
        "output_path": "/tmp/output/test2.txt",
        "error": None,
        "page_count": 1,
    },
]


class TestConvertCommand:
    """Test the convert command"""

    @patch("datalab_sdk.cli.asyncio.run")
    def test_convert_successful_single_file(self, mock_client_class):
        """Test successful conversion of a single file"""
        # Mock the client
        mock_client_class.return_value = ASYNC_RETURN_VALUE

        runner = CliRunner()
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_file:
            try:
                result = runner.invoke(
                    cli,
                    [
                        "convert",
                        tmp_file.name,
                        "--api_key",
                        "test-key",
                        "--output_dir",
                        "/tmp/output",
                    ],
                )

                assert result.exit_code == 0
                assert "✅ Successfully processed" in result.output

                # Verify client was called correctly
                mock_client_class.assert_called_once()

            finally:
                os.unlink(tmp_file.name)

    @patch("datalab_sdk.cli.asyncio.run")
    def test_convert_with_env_var(self, mock_client_class):
        """Test convert command using environment variable for API key"""

        mock_client_class.return_value = ASYNC_RETURN_VALUE

        runner = CliRunner()
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_file:
            try:
                # Set environment variable
                settings.DATALAB_API_KEY = "env-api-key"
                result = runner.invoke(
                    cli, ["convert", tmp_file.name, "--output_dir", "/tmp/output"]
                )
                settings.DATALAB_API_KEY = None

                assert result.exit_code == 0
                assert "✅ Successfully processed" in result.output

            finally:
                os.unlink(tmp_file.name)

    @patch("datalab_sdk.cli.asyncio.run")
    def test_convert_missing_api_key(self, mock_client_class):
        """Test convert command with missing API key"""

        mock_client_class.return_value = ASYNC_RETURN_VALUE

        runner = CliRunner()
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_file:
            try:
                # Clear environment variable
                with patch.dict(os.environ, {}, clear=True):
                    result = runner.invoke(
                        cli, ["convert", tmp_file.name, "--output_dir", "/tmp/output"]
                    )

                    assert result.exit_code == 1
                    assert "You must either pass in an api key" in result.output

            finally:
                os.unlink(tmp_file.name)


class TestOCRCommand:
    """Test the OCR command"""

    @patch("datalab_sdk.cli.asyncio.run")
    def test_ocr_successful_single_file(self, mock_client_class):
        """Test successful OCR of a single file"""

        mock_client_class.return_value = ASYNC_RETURN_VALUE

        runner = CliRunner()
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_file:
            try:
                result = runner.invoke(
                    cli,
                    [
                        "ocr",
                        tmp_file.name,
                        "--api_key",
                        "test-key",
                        "--output_dir",
                        "/tmp/output",
                    ],
                )

                assert result.exit_code == 0
                assert "✅ Successfully processed: 2 files" in result.output

                # Verify client was called correctly
                mock_client_class.assert_called_once()

            finally:
                os.unlink(tmp_file.name)

    @patch("datalab_sdk.cli.asyncio.run")
    def test_ocr_with_max_pages(self, mock_asyncio_run):
        """Test OCR command with max_pages option"""
        # Mock the client
        mock_asyncio_run.return_value = ASYNC_RETURN_VALUE

        runner = CliRunner()
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_file:
            try:
                result = runner.invoke(
                    cli,
                    [
                        "ocr",
                        tmp_file.name,
                        "--api_key",
                        "test-key",
                        "--output_dir",
                        "/tmp/output",
                        "--max_pages",
                        "5",
                    ],
                )

                assert result.exit_code == 0
                assert "✅ Successfully processed: 2 files" in result.output

            finally:
                os.unlink(tmp_file.name)

    @patch("datalab_sdk.cli.asyncio.run")
    def test_ocr_multiple_files(self, mock_asyncio_run):
        """Test OCR of multiple files"""
        # Mock async processing results
        mock_asyncio_run.return_value = ASYNC_RETURN_VALUE

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmp_dir:
            with open(os.path.join(tmp_dir, "test1.pdf"), "w") as f:
                f.write("Dummy content for test1.pdf")
            with open(os.path.join(tmp_dir, "test2.pdf"), "w") as f:
                f.write("Dummy content for test2.pdf")
            result = runner.invoke(
                cli,
                [
                    "ocr",
                    tmp_dir,
                    "--api_key",
                    "test-key",
                    "--output_dir",
                    "/tmp/output",
                ],
            )

            assert result.exit_code == 0
            assert "📊 OCR Summary:" in result.output
            assert "✅ Successfully processed: 2 files" in result.output
