"""
Tests for client methods
"""

import warnings
import pytest
from unittest.mock import patch, AsyncMock
import json

from datalab_sdk import DatalabClient, AsyncDatalabClient
from datalab_sdk.models import (
    ConversionResult,
    CreateDocumentResult,
    OCRResult,
    ConvertOptions,
    ExtractOptions,
    SegmentOptions,
    CustomPipelineOptions,
    TrackChangesOptions,
    OCROptions,
)
from datalab_sdk.exceptions import (
    DatalabAPIError,
    DatalabFileError,
    DatalabTimeoutError,
)


class TestConvertMethod:
    """Test the convert method"""

    @pytest.mark.asyncio
    async def test_convert_basic_success(self, temp_dir):
        """Test basic successful conversion"""
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        mock_initial_response = {
            "success": True,
            "request_id": "test-request-id",
            "request_check_url": "https://api.datalab.to/api/v1/convert/test-request-id",
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
                    mock_request.return_value = mock_initial_response
                    mock_poll.return_value = mock_result_response

                    result = await client.convert(pdf_file)

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
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        mock_initial_response = {
            "success": True,
            "request_id": "test-request-id",
            "request_check_url": "https://api.datalab.to/api/v1/convert/test-request-id",
        }

        mock_result_response = {
            "success": True,
            "status": "complete",
            "output_format": "markdown",
            "markdown": "# Test Document\n\nThis is a test document.",
            "html": None,
            "json": None,
            "chunks": {"some_content": True},
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
                    mock_request.return_value = mock_initial_response
                    mock_poll.return_value = mock_result_response

                    result = await client.convert(pdf_file, save_output=output_path)

                    assert result.success is True
                    assert (output_path.with_suffix(".md")).exists()
                    saved_content = (output_path.with_suffix(".md")).read_text()
                    assert (
                        saved_content == "# Test Document\n\nThis is a test document."
                    )

                    assert (output_path.with_suffix(".chunks.json")).exists()
                    saved_chunks = json.loads(
                        (output_path.with_suffix(".chunks.json")).read_text()
                    )
                    assert saved_chunks == {"some_content": True}

    def test_convert_sync_with_processing_options(self, temp_dir):
        """Test synchronous conversion with processing options"""
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        options = ConvertOptions(output_format="html", max_pages=5)

        mock_initial_response = {
            "success": True,
            "request_id": "test-request-id",
            "request_check_url": "https://api.datalab.to/api/v1/convert/test-request-id",
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
                mock_request.return_value = mock_initial_response
                mock_poll.return_value = mock_result_response

                result = client.convert(pdf_file, options=options)

                assert isinstance(result, ConversionResult)
                assert result.success is True
                assert result.html == "<h1>Test Document</h1>"
                assert result.output_format == "html"

    @pytest.mark.asyncio
    async def test_convert_async_respects_polling_params(self, temp_dir):
        """Verify convert passes max_polls and poll_interval to poller"""
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        mock_initial_response = {
            "success": True,
            "request_id": "rid-1",
            "request_check_url": "https://api.datalab.to/api/v1/convert/rid-1",
        }

        mock_result_response = {
            "success": True,
            "status": "complete",
            "output_format": "markdown",
            "markdown": "ok",
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(
                client, "_make_request", new_callable=AsyncMock
            ) as mock_req:
                with patch.object(
                    client, "_poll_result", new_callable=AsyncMock
                ) as mock_poll:
                    mock_req.return_value = mock_initial_response
                    mock_poll.return_value = mock_result_response

                    max_polls = 7
                    poll_interval = 3
                    await client.convert(
                        pdf_file, max_polls=max_polls, poll_interval=poll_interval
                    )

                    mock_poll.assert_awaited_once()
                    args, kwargs = mock_poll.await_args
                    assert args[0] == mock_initial_response["request_check_url"]
                    assert kwargs["max_polls"] == max_polls
                    assert kwargs["poll_interval"] == poll_interval


class TestExtractMethod:
    """Test the extract method"""

    @pytest.mark.asyncio
    async def test_extract_with_file(self, temp_dir):
        """Test extraction with file input"""
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        mock_initial_response = {
            "success": True,
            "request_id": "extract-id",
            "request_check_url": "https://api.datalab.to/api/v1/extract/extract-id",
        }

        mock_result_response = {
            "success": True,
            "status": "complete",
            "output_format": "markdown",
            "markdown": "# Extracted Data",
            "extraction_schema_json": '{"name": "John"}',
        }

        options = ExtractOptions(
            page_schema='{"properties": {"name": {"type": "string"}}}',
        )

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(
                client, "_make_request", new_callable=AsyncMock
            ) as mock_request:
                with patch.object(
                    client, "_poll_result", new_callable=AsyncMock
                ) as mock_poll:
                    mock_request.return_value = mock_initial_response
                    mock_poll.return_value = mock_result_response

                    result = await client.extract(pdf_file, options=options)

                    assert isinstance(result, ConversionResult)
                    assert result.success is True
                    assert result.extraction_schema_json == '{"name": "John"}'

    @pytest.mark.asyncio
    async def test_extract_with_checkpoint(self):
        """Test extraction with checkpoint_id (no file)"""
        mock_initial_response = {
            "success": True,
            "request_id": "extract-cp-id",
            "request_check_url": "https://api.datalab.to/api/v1/extract/extract-cp-id",
        }

        mock_result_response = {
            "success": True,
            "status": "complete",
            "output_format": "markdown",
            "extraction_schema_json": '{"name": "Jane"}',
        }

        options = ExtractOptions(
            page_schema='{"properties": {"name": {"type": "string"}}}',
            checkpoint_id="cp_abc123",
        )

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(
                client, "_make_request", new_callable=AsyncMock
            ) as mock_request:
                with patch.object(
                    client, "_poll_result", new_callable=AsyncMock
                ) as mock_poll:
                    mock_request.return_value = mock_initial_response
                    mock_poll.return_value = mock_result_response

                    result = await client.extract(options=options)

                    assert result.success is True
                    assert result.extraction_schema_json == '{"name": "Jane"}'

    @pytest.mark.asyncio
    async def test_extract_requires_options(self):
        """Test that extract raises error without options"""
        async with AsyncDatalabClient(api_key="test-key") as client:
            with pytest.raises(ValueError, match="options must be provided"):
                await client.extract(file_path="test.pdf")

    @pytest.mark.asyncio
    async def test_extract_requires_file_or_checkpoint(self):
        """Test that extract raises error without file or checkpoint"""
        options = ExtractOptions(
            page_schema='{"properties": {"name": {"type": "string"}}}',
        )
        async with AsyncDatalabClient(api_key="test-key") as client:
            with pytest.raises(ValueError, match="Either file_path/file_url or options.checkpoint_id"):
                await client.extract(options=options)


class TestSegmentMethod:
    """Test the segment method"""

    @pytest.mark.asyncio
    async def test_segment_with_file(self, temp_dir):
        """Test segmentation with file input"""
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        mock_initial_response = {
            "success": True,
            "request_id": "segment-id",
            "request_check_url": "https://api.datalab.to/api/v1/segment/segment-id",
        }

        mock_result_response = {
            "success": True,
            "status": "complete",
            "segmentation_results": {
                "segments": [
                    {"name": "Introduction", "page_range": "0-2", "confidence": "high"},
                ]
            },
        }

        options = SegmentOptions(
            segmentation_schema='{"segments": [{"name": "Introduction", "description": "Intro section"}]}',
        )

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(
                client, "_make_request", new_callable=AsyncMock
            ) as mock_request:
                with patch.object(
                    client, "_poll_result", new_callable=AsyncMock
                ) as mock_poll:
                    mock_request.return_value = mock_initial_response
                    mock_poll.return_value = mock_result_response

                    result = await client.segment(pdf_file, options=options)

                    assert isinstance(result, ConversionResult)
                    assert result.success is True
                    assert result.segmentation_results is not None
                    assert len(result.segmentation_results["segments"]) == 1


class TestCustomPipelineMethod:
    """Test the run_custom_pipeline method"""

    @pytest.mark.asyncio
    async def test_run_custom_pipeline(self, temp_dir):
        """Test custom pipeline execution"""
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        mock_initial_response = {
            "success": True,
            "request_id": "cp-id",
            "request_check_url": "https://api.datalab.to/api/v1/custom-pipeline/cp-id",
        }

        mock_result_response = {
            "success": True,
            "status": "complete",
            "output_format": "markdown",
            "markdown": "# Custom Output",
            "evaluation": {"eval_definition_name": "test", "evaluations": [], "total_items_evaluated": 0},
        }

        options = CustomPipelineOptions(
            pipeline_id="cp_abc12",
            run_eval=True,
        )

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(
                client, "_make_request", new_callable=AsyncMock
            ) as mock_request:
                with patch.object(
                    client, "_poll_result", new_callable=AsyncMock
                ) as mock_poll:
                    mock_request.return_value = mock_initial_response
                    mock_poll.return_value = mock_result_response

                    result = await client.run_custom_pipeline(pdf_file, options=options)

                    assert result.success is True
                    assert result.markdown == "# Custom Output"
                    assert result.evaluation is not None


class TestTrackChangesMethod:
    """Test the track_changes method"""

    @pytest.mark.asyncio
    async def test_track_changes(self, temp_dir):
        """Test track changes extraction"""
        docx_file = temp_dir / "test.docx"
        docx_file.write_bytes(b"PK\x03\x04test docx content")

        mock_initial_response = {
            "success": True,
            "request_id": "tc-id",
            "request_check_url": "https://api.datalab.to/api/v1/track-changes/tc-id",
        }

        mock_result_response = {
            "success": True,
            "status": "complete",
            "output_format": "markdown,html,chunks",
            "markdown": "# Document with <ins>inserted</ins> text",
            "html": "<h1>Document with <ins>inserted</ins> text</h1>",
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(
                client, "_make_request", new_callable=AsyncMock
            ) as mock_request:
                with patch.object(
                    client, "_poll_result", new_callable=AsyncMock
                ) as mock_poll:
                    mock_request.return_value = mock_initial_response
                    mock_poll.return_value = mock_result_response

                    result = await client.track_changes(docx_file)

                    assert result.success is True
                    assert "<ins>" in result.markdown


class TestCreateDocumentMethod:
    """Test the create_document method"""

    @pytest.mark.asyncio
    async def test_create_document(self):
        """Test document creation from markdown"""
        mock_initial_response = {
            "success": True,
            "request_id": "cd-id",
            "request_check_url": "https://api.datalab.to/api/v1/create-document/cd-id",
        }

        mock_result_response = {
            "success": True,
            "status": "complete",
            "output_format": "docx",
            "output_base64": "UEsDBBQ=",  # minimal base64
            "runtime": 1.5,
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(
                client, "_make_request", new_callable=AsyncMock
            ) as mock_request:
                with patch.object(
                    client, "_poll_result", new_callable=AsyncMock
                ) as mock_poll:
                    mock_request.return_value = mock_initial_response
                    mock_poll.return_value = mock_result_response

                    result = await client.create_document(
                        markdown="# Hello World"
                    )

                    assert isinstance(result, CreateDocumentResult)
                    assert result.success is True
                    assert result.output_format == "docx"
                    assert result.output_base64 is not None

                    # Verify JSON body was sent (not form data)
                    call_args = mock_request.call_args
                    assert call_args[1].get("json") is not None
                    assert call_args[1]["json"]["markdown"] == "# Hello World"


class TestOCRMethod:
    """Test the ocr method (deprecated)"""

    @pytest.mark.asyncio
    async def test_ocr_basic_success(self, temp_dir):
        """Test basic successful OCR"""
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

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
                    mock_request.return_value = mock_initial_response
                    mock_poll.return_value = mock_result_response

                    with warnings.catch_warnings(record=True) as w:
                        warnings.simplefilter("always")
                        result = await client.ocr(pdf_file)
                        assert len(w) == 1
                        assert issubclass(w[0].category, DeprecationWarning)
                        assert "deprecated" in str(w[0].message).lower()

                    assert isinstance(result, OCRResult)
                    assert result.success is True
                    assert len(result.pages) == 1
                    assert result.page_count == 1

    @pytest.mark.asyncio
    async def test_ocr_with_save_output(self, temp_dir):
        """Test OCR with automatic saving"""
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

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
                    mock_request.return_value = mock_initial_response
                    mock_poll.return_value = mock_result_response

                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", DeprecationWarning)
                        result = await client.ocr(pdf_file, save_output=output_path)

                    assert result.success is True

                    text_file = output_path.with_suffix(".txt")
                    assert text_file.exists()

                    json_file = output_path.with_suffix(".ocr.json")
                    assert json_file.exists()
                    saved_json = json.loads(json_file.read_text())
                    assert saved_json["success"] is True

    def test_ocr_sync_with_max_pages(self, temp_dir):
        """Test synchronous OCR with max_pages parameter"""
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

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
                mock_request.return_value = mock_initial_response
                mock_poll.return_value = mock_result_response

                options = OCROptions(max_pages=2)

                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", DeprecationWarning)
                    result = client.ocr(pdf_file, options=options)

                assert isinstance(result, OCRResult)
                assert result.success is True
                assert len(result.pages) == 2

    def test_sync_wrappers_forward_polling_params(self, temp_dir):
        """Ensure sync client forwards polling params to async client"""
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        client = DatalabClient(api_key="test-key")

        with patch.object(
            client._async_client, "convert", new_callable=AsyncMock
        ) as mock_conv:
            mock_conv.return_value = ConversionResult(
                success=True, output_format="markdown", markdown="ok"
            )

            client.convert(pdf_file, max_polls=5, poll_interval=9)

            _, conv_kwargs = mock_conv.await_args
            assert conv_kwargs["max_polls"] == 5
            assert conv_kwargs["poll_interval"] == 9


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
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(
                client, "_make_request", new_callable=AsyncMock
            ) as mock_request:
                mock_request.side_effect = DatalabAPIError(
                    "Bad request", status_code=400
                )

                with pytest.raises(DatalabAPIError, match="Bad request"):
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", DeprecationWarning)
                        await client.ocr(pdf_file)

    def test_convert_unsuccessful_response(self, temp_dir):
        """Test convert with unsuccessful API response"""
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

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
            mock_request.return_value = mock_initial_response

            with pytest.raises(
                DatalabAPIError, match="Request failed: Processing failed"
            ):
                client.convert(pdf_file)

    def test_convert_timeout_bubbles_up(self, temp_dir):
        """Polling timeout surfaces as DatalabTimeoutError for sync convert"""
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        mock_initial_response = {
            "success": True,
            "request_id": "rid-timeout",
            "request_check_url": "https://api.datalab.to/api/v1/convert/rid-timeout",
        }

        client = DatalabClient(api_key="test-key")
        with patch.object(
            client._async_client, "_make_request", new_callable=AsyncMock
        ) as mock_request:
            with patch.object(
                client._async_client, "_poll_result", new_callable=AsyncMock
            ) as mock_poll:
                mock_request.return_value = mock_initial_response
                mock_poll.side_effect = DatalabTimeoutError("Polling timed out")

                with pytest.raises(DatalabTimeoutError, match="Polling timed out"):
                    client.convert(pdf_file)


class TestPollingLoop:
    """Direct tests for the internal polling helper"""

    @pytest.mark.asyncio
    async def test_poll_result_times_out(self):
        async with AsyncDatalabClient(api_key="test-key") as client:
            with (
                patch.object(
                    client, "_make_request", new_callable=AsyncMock
                ) as mock_req,
                patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
            ):
                mock_req.return_value = {"status": "processing", "success": True}

                with pytest.raises(DatalabTimeoutError):
                    await client._poll_result(
                        "https://api.example.com/check", max_polls=3, poll_interval=0
                    )

                assert mock_req.await_count == 3
                assert mock_sleep.await_count >= 1

    @pytest.mark.asyncio
    async def test_poll_result_raises_on_failed_status(self):
        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(
                client, "_make_request", new_callable=AsyncMock
            ) as mock_req:
                mock_req.return_value = {
                    "status": "failed",
                    "success": False,
                    "error": "boom",
                }

                with pytest.raises(DatalabAPIError, match="Processing failed: boom"):
                    await client._poll_result(
                        "https://api.example.com/check", max_polls=1, poll_interval=0
                    )
