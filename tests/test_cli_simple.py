"""
Simple tests for the CLI module
"""

from unittest.mock import Mock, patch
import tempfile
import os
from click.testing import CliRunner

from datalab_sdk.cli import cli
from datalab_sdk.settings import settings


class TestConvertCommand:
    """Test the convert command"""

    @patch("datalab_sdk.cli.DatalabClient")
    def test_convert_successful_single_file(self, mock_client_class):
        """Test successful conversion of a single file"""
        # Mock the client
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock successful result
        mock_client.process.return_value = {
            "success": True,
            "file_path": "/tmp/test.pdf",
            "output_path": "/tmp/output/test.md",
            "error": None,
            "page_count": 5,
        }

        runner = CliRunner()
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_file:
            try:
                result = runner.invoke(
                    cli,
                    [
                        "convert",
                        tmp_file.name,
                        "--api-key",
                        "test-key",
                        "--output-dir",
                        "/tmp/output",
                    ],
                )

                assert result.exit_code == 0
                assert "✅ Successfully converted" in result.output

                # Verify client was called correctly
                mock_client_class.assert_called_once_with("test-key", None)

            finally:
                os.unlink(tmp_file.name)

    @patch("datalab_sdk.cli.DatalabClient")
    def test_convert_with_env_var(self, mock_client_class):
        """Test convert command using environment variable for API key"""
        # Mock the client
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_client.process.return_value = {
            "success": True,
            "file_path": "/tmp/test.pdf",
            "output_path": "/tmp/output/test.md",
            "error": None,
            "page_count": 1,
        }

        runner = CliRunner()
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_file:
            try:
                # Set environment variable
                settings.DATALAB_API_KEY = "env-api-key"
                result = runner.invoke(
                    cli, ["convert", tmp_file.name, "--output-dir", "/tmp/output"]
                )
                settings.DATALAB_API_KEY = None

                assert result.exit_code == 0
                assert "✅ Successfully converted" in result.output

                # Verify client was called with env var
                mock_client_class.assert_called_once_with("env-api-key", None)

            finally:
                os.unlink(tmp_file.name)

    def test_convert_missing_api_key(self):
        """Test convert command with missing API key"""
        runner = CliRunner()
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_file:
            try:
                # Clear environment variable
                with patch.dict(os.environ, {}, clear=True):
                    result = runner.invoke(
                        cli, ["convert", tmp_file.name, "--output-dir", "/tmp/output"]
                    )

                    assert result.exit_code == 1
                    assert "You must either pass in an api key" in str(result)

            finally:
                os.unlink(tmp_file.name)


class TestOCRCommand:
    """Test the OCR command"""

    @patch("datalab_sdk.cli.DatalabClient")
    def test_ocr_successful_single_file(self, mock_client_class):
        """Test successful OCR of a single file"""
        # Mock the client
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock successful result
        mock_client.process.return_value = {
            "success": True,
            "file_path": "/tmp/test.pdf",
            "output_path": "/tmp/output/test.txt",
            "error": None,
            "page_count": 3,
        }

        runner = CliRunner()
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_file:
            try:
                result = runner.invoke(
                    cli,
                    [
                        "ocr",
                        tmp_file.name,
                        "--api-key",
                        "test-key",
                        "--output-dir",
                        "/tmp/output",
                    ],
                )

                assert result.exit_code == 0
                assert "✅ Successfully performed OCR on" in result.output

                # Verify client was called correctly
                mock_client_class.assert_called_once_with("test-key", None)

                # Verify correct endpoint was used
                args, kwargs = mock_client.process.call_args
                assert args[2] == "/api/v1/ocr"  # endpoint

            finally:
                os.unlink(tmp_file.name)

    @patch("datalab_sdk.cli.DatalabClient")
    def test_ocr_with_max_pages(self, mock_client_class):
        """Test OCR command with max_pages option"""
        # Mock the client
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_client.process.return_value = {
            "success": True,
            "file_path": "/tmp/test.pdf",
            "output_path": "/tmp/output/test.txt",
            "error": None,
            "page_count": 5,
        }

        runner = CliRunner()
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_file:
            try:
                result = runner.invoke(
                    cli,
                    [
                        "ocr",
                        tmp_file.name,
                        "--api-key",
                        "test-key",
                        "--output-dir",
                        "/tmp/output",
                        "--max-pages",
                        "5",
                    ],
                )

                assert result.exit_code == 0
                assert "✅ Successfully performed OCR on" in result.output

                # Verify process was called with correct parameters
                args, kwargs = mock_client.process.call_args
                assert args[4] == 5  # max_pages

            finally:
                os.unlink(tmp_file.name)

    @patch("datalab_sdk.cli.DatalabClient")
    def test_ocr_multiple_files(self, mock_client_class):
        """Test OCR of multiple files"""
        # Mock the client
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock multiple file results
        mock_client.process.return_value = [
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

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = runner.invoke(
                cli,
                [
                    "ocr",
                    tmp_dir,
                    "--api-key",
                    "test-key",
                    "--output-dir",
                    "/tmp/output",
                ],
            )

            assert result.exit_code == 0
            assert "📊 OCR Summary:" in result.output
            assert "✅ Successfully processed: 2 files" in result.output
