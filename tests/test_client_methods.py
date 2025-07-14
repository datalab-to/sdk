"""
Tests for the convert and ocr methods of the client
"""

import pytest
from unittest.mock import patch, AsyncMock
import json

from datalab_sdk import DatalabClient, AsyncDatalabClient
from datalab_sdk.models import ProcessingOptions, ConversionResult, OCRResult
from datalab_sdk.exceptions import DatalabAPIError, DatalabFileError


class TestConvertMethod:
    """Test the convert method"""

    @pytest.mark.asyncio
    async def test_convert_basic_success(self, temp_dir):
        """Test basic successful conversion"""
        # Create test file
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        # Mock API responses
        mock_initial_response = {
            "success": True,
            "request_id": "test-request-id",
            "request_check_url": "https://api.datalab.to/api/v1/marker/test-request-id",
        }

        mock_result_response = {
            "success": True,
            "status": "complete",
            "output_format": "markdown",
            "markdown": "# Test Document\n\nThis is a test document.",
            "html": None,
            "json": None,
            "images": {},
            "metadata": {"pages": 1},
            "error": "",
            "page_count": 1,
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(
                client, "_make_request", new_callable=AsyncMock
            ) as mock_request:
                with patch.object(
                    client, "_poll_result", new_callable=AsyncMock
                ) as mock_poll:
                    # Setup mocks
                    mock_request.return_value = mock_initial_response
                    mock_poll.return_value = mock_result_response

                    # Test conversion
                    result = await client.convert(pdf_file)

                    # Verify result
                    assert isinstance(result, ConversionResult)
                    assert result.success is True
                    assert (
                        result.markdown == "# Test Document\n\nThis is a test document."
                    )
                    assert result.page_count == 1
                    assert result.output_format == "markdown"

    @pytest.mark.asyncio
    async def test_convert_with_save_output(self, temp_dir):
        """Test conversion with automatic saving"""
        # Create test file
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        # Mock API responses
        mock_initial_response = {
            "success": True,
            "request_id": "test-request-id",
            "request_check_url": "https://api.datalab.to/api/v1/marker/test-request-id",
        }

        mock_result_response = {
            "success": True,
            "status": "complete",
            "output_format": "markdown",
            "markdown": "# Test Document\n\nThis is a test document.",
            "html": None,
            "json": None,
            "images": {},
            "metadata": {"pages": 1},
            "error": "",
            "page_count": 1,
        }

        output_path = temp_dir / "output" / "result"

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(
                client, "_make_request", new_callable=AsyncMock
            ) as mock_request:
                with patch.object(
                    client, "_poll_result", new_callable=AsyncMock
                ) as mock_poll:
                    # Setup mocks
                    mock_request.return_value = mock_initial_response
                    mock_poll.return_value = mock_result_response

                    # Test conversion with save_output
                    result = await client.convert(pdf_file, save_output=output_path)

                    # Verify result
                    assert result.success is True

                    # Verify file was saved
                    assert (output_path.with_suffix(".md")).exists()
                    saved_content = (output_path.with_suffix(".md")).read_text()
                    assert (
                        saved_content == "# Test Document\n\nThis is a test document."
                    )

    def test_convert_sync_with_processing_options(self, temp_dir):
        """Test synchronous conversion with processing options"""
        # Create test file
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        # Create processing options
        options = ProcessingOptions(
            force_ocr=True, output_format="html", use_llm=True, max_pages=5
        )

        # Mock API responses
        mock_initial_response = {
            "success": True,
            "request_id": "test-request-id",
            "request_check_url": "https://api.datalab.to/api/v1/marker/test-request-id",
        }

        mock_result_response = {
            "success": True,
            "status": "complete",
            "output_format": "html",
            "markdown": None,
            "html": "<h1>Test Document</h1>",
            "json": None,
            "images": {},
            "metadata": {"pages": 1},
            "error": "",
            "page_count": 1,
        }

        client = DatalabClient(api_key="test-key")

        with patch.object(
            client._async_client, "_make_request", new_callable=AsyncMock
        ) as mock_request:
            with patch.object(
                client._async_client, "_poll_result", new_callable=AsyncMock
            ) as mock_poll:
                # Setup mocks
                mock_request.return_value = mock_initial_response
                mock_poll.return_value = mock_result_response

                # Test conversion
                result = client.convert(pdf_file, options=options)

                # Verify result
                assert isinstance(result, ConversionResult)
                assert result.success is True
                assert result.html == "<h1>Test Document</h1>"
                assert result.output_format == "html"


class TestOCRMethod:
    """Test the ocr method"""

    @pytest.mark.asyncio
    async def test_ocr_basic_success(self, temp_dir):
        """Test basic successful OCR"""
        # Create test file
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        # Mock API responses
        mock_initial_response = {
            "success": True,
            "request_id": "test-ocr-request-id",
            "request_check_url": "https://api.datalab.to/api/v1/ocr/test-ocr-request-id",
        }

        mock_result_response = {
            "success": True,
            "status": "complete",
            "pages": [
                {
                    "text_lines": [
                        {
                            "text": "Test Document",
                            "confidence": 0.99,
                            "bbox": [0, 0, 100, 20],
                            "polygon": [[0, 0], [100, 0], [100, 20], [0, 20]],
                        }
                    ],
                    "page": 1,
                    "image_bbox": [0, 0, 800, 600],
                }
            ],
            "error": "",
            "page_count": 1,
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(
                client, "_make_request", new_callable=AsyncMock
            ) as mock_request:
                with patch.object(
                    client, "_poll_result", new_callable=AsyncMock
                ) as mock_poll:
                    # Setup mocks
                    mock_request.return_value = mock_initial_response
                    mock_poll.return_value = mock_result_response

                    # Test OCR
                    result = await client.ocr(pdf_file)

                    # Verify result
                    assert isinstance(result, OCRResult)
                    assert result.success is True
                    assert len(result.pages) == 1
                    assert result.pages[0]["page"] == 1
                    assert len(result.pages[0]["text_lines"]) == 1
                    assert result.pages[0]["text_lines"][0]["text"] == "Test Document"
                    assert result.page_count == 1

    @pytest.mark.asyncio
    async def test_ocr_with_save_output(self, temp_dir):
        """Test OCR with automatic saving"""
        # Create test file
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        # Mock API responses
        mock_initial_response = {
            "success": True,
            "request_id": "test-ocr-request-id",
            "request_check_url": "https://api.datalab.to/api/v1/ocr/test-ocr-request-id",
        }

        mock_result_response = {
            "success": True,
            "status": "complete",
            "pages": [
                {
                    "text_lines": [
                        {"text": "Line 1", "confidence": 0.99},
                        {"text": "Line 2", "confidence": 0.98},
                    ],
                    "page": 1,
                    "image_bbox": [0, 0, 800, 600],
                }
            ],
            "error": "",
            "page_count": 1,
        }

        output_path = temp_dir / "output" / "ocr_result"

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(
                client, "_make_request", new_callable=AsyncMock
            ) as mock_request:
                with patch.object(
                    client, "_poll_result", new_callable=AsyncMock
                ) as mock_poll:
                    # Setup mocks
                    mock_request.return_value = mock_initial_response
                    mock_poll.return_value = mock_result_response

                    # Test OCR with save_output
                    result = await client.ocr(pdf_file, save_output=output_path)

                    # Verify result
                    assert result.success is True

                    # Verify text file was saved
                    text_file = output_path.with_suffix(".txt")
                    assert text_file.exists()
                    saved_text = text_file.read_text()
                    assert saved_text == "Line 1\nLine 2"

                    # Verify JSON file was saved
                    json_file = output_path.with_suffix(".ocr.json")
                    assert json_file.exists()
                    saved_json = json.loads(json_file.read_text())
                    assert saved_json["success"] is True
                    assert len(saved_json["pages"]) == 1

    def test_ocr_sync_with_max_pages(self, temp_dir):
        """Test synchronous OCR with max_pages parameter"""
        # Create test file
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        # Mock API responses
        mock_initial_response = {
            "success": True,
            "request_id": "test-ocr-request-id",
            "request_check_url": "https://api.datalab.to/api/v1/ocr/test-ocr-request-id",
        }

        mock_result_response = {
            "success": True,
            "status": "complete",
            "pages": [
                {
                    "text_lines": [{"text": "Page 1 content", "confidence": 0.99}],
                    "page": 1,
                    "image_bbox": [0, 0, 800, 600],
                },
                {
                    "text_lines": [{"text": "Page 2 content", "confidence": 0.98}],
                    "page": 2,
                    "image_bbox": [0, 0, 800, 600],
                },
            ],
            "error": "",
            "page_count": 2,
        }

        client = DatalabClient(api_key="test-key")

        with patch.object(
            client._async_client, "_make_request", new_callable=AsyncMock
        ) as mock_request:
            with patch.object(
                client._async_client, "_poll_result", new_callable=AsyncMock
            ) as mock_poll:
                # Setup mocks
                mock_request.return_value = mock_initial_response
                mock_poll.return_value = mock_result_response

                # Test OCR with max_pages
                result = client.ocr(pdf_file, max_pages=2)

                # Verify result
                assert isinstance(result, OCRResult)
                assert result.success is True
                assert len(result.pages) == 2
                assert result.page_count == 2

                # Verify text extraction
                all_text = result.get_text()
                assert "Page 1 content" in all_text
                assert "Page 2 content" in all_text


class TestClientErrorHandling:
    """Test error handling in client methods"""

    def test_convert_file_not_found(self, temp_dir):
        """Test convert with nonexistent file"""
        nonexistent_file = temp_dir / "nonexistent.pdf"

        client = DatalabClient(api_key="test-key")

        with pytest.raises(DatalabFileError, match="File not found"):
            client.convert(nonexistent_file)

    @pytest.mark.asyncio
    async def test_ocr_api_error(self, temp_dir):
        """Test OCR with API error"""
        # Create test file
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(
                client, "_make_request", new_callable=AsyncMock
            ) as mock_request:
                # Setup mock to raise API error
                mock_request.side_effect = DatalabAPIError(
                    "API rate limit exceeded", status_code=429
                )

                # Test that error is propagated
                with pytest.raises(DatalabAPIError, match="API rate limit exceeded"):
                    await client.ocr(pdf_file)

    def test_convert_unsuccessful_response(self, temp_dir):
        """Test convert with unsuccessful API response"""
        # Create test file
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        # Mock unsuccessful initial response
        mock_initial_response = {
            "success": False,
            "error": "Processing failed",
            "request_id": None,
            "request_check_url": None,
        }

        client = DatalabClient(api_key="test-key")

        with patch.object(
            client._async_client, "_make_request", new_callable=AsyncMock
        ) as mock_request:
            # Setup mock
            mock_request.return_value = mock_initial_response

            # Test that error is raised
            with pytest.raises(
                DatalabAPIError, match="Request failed: Processing failed"
            ):
                client.convert(pdf_file)
