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
    CustomProcessorOptions,
    TrackChangesOptions,
    OCROptions,
    ExtractionSchema,
    PipelineStep,
    PipelineConfig,
    PipelineExecution,
    PipelineExecutionStepResult,
    CustomProcessor,
    CustomProcessorVersion,
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


class TestExtractWithSchemaId:
    """Test extract with schema_id support"""

    @pytest.mark.asyncio
    async def test_extract_with_schema_id(self, temp_dir):
        """Test extraction using a saved schema ID"""
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        mock_initial = {
            "success": True,
            "request_id": "ext-schema",
            "request_check_url": "https://api.datalab.to/api/v1/extract/ext-schema",
        }
        mock_result = {
            "success": True,
            "status": "complete",
            "output_format": "markdown",
            "extraction_schema_json": '{"name": "John"}',
        }

        options = ExtractOptions(schema_id="sch_k8Hx9mP2nQ4v")

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "_make_request", new_callable=AsyncMock) as mock_req:
                with patch.object(client, "_poll_result", new_callable=AsyncMock) as mock_poll:
                    mock_req.return_value = mock_initial
                    mock_poll.return_value = mock_result
                    result = await client.extract(pdf_file, options=options)
                    assert result.success is True

    @pytest.mark.asyncio
    async def test_extract_rejects_both_page_schema_and_schema_id(self):
        options = ExtractOptions(
            page_schema='{"properties": {"name": {"type": "string"}}}',
            schema_id="sch_k8Hx9mP2nQ4v",
        )
        async with AsyncDatalabClient(api_key="test-key") as client:
            with pytest.raises(ValueError, match="mutually exclusive"):
                await client.extract(file_path="test.pdf", options=options)

    @pytest.mark.asyncio
    async def test_extract_rejects_neither_page_schema_nor_schema_id(self):
        options = ExtractOptions()
        async with AsyncDatalabClient(api_key="test-key") as client:
            with pytest.raises(ValueError, match="Either page_schema or schema_id"):
                await client.extract(file_path="test.pdf", options=options)

    @pytest.mark.asyncio
    async def test_extract_rejects_schema_version_without_schema_id(self):
        options = ExtractOptions(
            page_schema='{"properties": {"name": {"type": "string"}}}',
            schema_version=2,
        )
        async with AsyncDatalabClient(api_key="test-key") as client:
            with pytest.raises(ValueError, match="schema_version can only be used with schema_id"):
                await client.extract(file_path="test.pdf", options=options)

    def test_extract_options_form_data_suppresses_empty_page_schema(self):
        """When schema_id is set and page_schema is empty, page_schema should not be in form data"""
        options = ExtractOptions(schema_id="sch_abc123")
        form_data = options.to_form_data()
        assert "schema_id" in form_data
        assert "page_schema" not in form_data


class TestCustomProcessorOptions:
    """Test CustomProcessorOptions new fields and alias"""

    def test_new_fields_have_defaults(self):
        options = CustomProcessorOptions(pipeline_id="cp_abc12")
        assert options.version is None
        assert options.paginate is False
        assert options.add_block_ids is False
        assert options.include_markdown_in_chunks is False
        assert options.disable_image_extraction is False
        assert options.disable_image_captions is False

    def test_processor_id_alias(self):
        options = CustomProcessorOptions(pipeline_id="cp_abc12")
        assert options.processor_id == "cp_abc12"
        options.processor_id = "cp_xyz99"
        assert options.pipeline_id == "cp_xyz99"

    def test_backward_compatible_alias(self):
        """CustomPipelineOptions should be the same class"""
        assert CustomPipelineOptions is CustomProcessorOptions

    def test_form_data_includes_new_fields(self):
        options = CustomProcessorOptions(
            pipeline_id="cp_abc12", version=3, paginate=True, disable_image_extraction=True,
        )
        form_data = options.to_form_data()
        assert "version" in form_data
        assert "paginate" in form_data
        assert "disable_image_extraction" in form_data


class TestConvertEvalRubricId:
    """Test eval_rubric_id on ConvertOptions"""

    def test_default_none(self):
        options = ConvertOptions()
        assert options.eval_rubric_id is None
        form_data = options.to_form_data()
        assert "eval_rubric_id" not in form_data

    def test_serialized_when_set(self):
        options = ConvertOptions(eval_rubric_id=42)
        form_data = options.to_form_data()
        assert "eval_rubric_id" in form_data


class TestRunCustomProcessorMethod:
    """Test run_custom_processor and deprecation of run_custom_pipeline"""

    @pytest.mark.asyncio
    async def test_run_custom_processor(self, temp_dir):
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        mock_initial = {
            "success": True,
            "request_id": "cp-id",
            "request_check_url": "https://api.datalab.to/api/v1/custom-processor/cp-id",
        }
        mock_result = {
            "success": True,
            "status": "complete",
            "output_format": "markdown",
            "markdown": "# Output",
        }

        options = CustomProcessorOptions(pipeline_id="cp_abc12")

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "_make_request", new_callable=AsyncMock) as mock_req:
                with patch.object(client, "_poll_result", new_callable=AsyncMock) as mock_poll:
                    mock_req.return_value = mock_initial
                    mock_poll.return_value = mock_result
                    result = await client.run_custom_processor(pdf_file, options=options)
                    assert result.success is True

    @pytest.mark.asyncio
    async def test_run_custom_pipeline_emits_deprecation(self, temp_dir):
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        mock_initial = {
            "success": True,
            "request_id": "cp-id",
            "request_check_url": "https://api.datalab.to/api/v1/custom-processor/cp-id",
        }
        mock_result = {
            "success": True,
            "status": "complete",
            "output_format": "markdown",
            "markdown": "# Output",
        }

        options = CustomProcessorOptions(pipeline_id="cp_abc12")

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "_make_request", new_callable=AsyncMock) as mock_req:
                with patch.object(client, "_poll_result", new_callable=AsyncMock) as mock_poll:
                    mock_req.return_value = mock_initial
                    mock_poll.return_value = mock_result

                    with warnings.catch_warnings(record=True) as w:
                        warnings.simplefilter("always")
                        result = await client.run_custom_pipeline(pdf_file, options=options)
                        assert len(w) == 1
                        assert issubclass(w[0].category, DeprecationWarning)
                        assert "run_custom_processor" in str(w[0].message)
                    assert result.success is True


class TestExtractionSchemaCRUD:
    """Test extraction schema CRUD methods"""

    @pytest.mark.asyncio
    async def test_create_extraction_schema(self):
        mock_response = {
            "id": 1, "schema_id": "sch_abc123", "name": "Invoice Schema",
            "description": "Extract invoice fields",
            "schema_json": {"properties": {"total": {"type": "number"}}},
            "version": 1, "version_history": None, "archived": False,
            "created": "2026-01-01T00:00:00", "updated": "2026-01-01T00:00:00",
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "_make_request", new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                result = await client.create_extraction_schema(
                    name="Invoice Schema",
                    schema_json={"properties": {"total": {"type": "number"}}},
                    description="Extract invoice fields",
                )
                assert isinstance(result, ExtractionSchema)
                assert result.schema_id == "sch_abc123"

    @pytest.mark.asyncio
    async def test_list_extraction_schemas(self):
        mock_response = {
            "schemas": [{
                "id": 1, "schema_id": "sch_abc123", "name": "Schema 1",
                "description": None, "schema_json": {"properties": {}},
                "version": 1, "version_history": None, "archived": False,
                "created": "2026-01-01T00:00:00", "updated": "2026-01-01T00:00:00",
            }],
            "total": 1,
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "_make_request", new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                result = await client.list_extraction_schemas(limit=10)
                assert result["total"] == 1
                assert isinstance(result["schemas"][0], ExtractionSchema)

    @pytest.mark.asyncio
    async def test_delete_extraction_schema(self):
        mock_response = {
            "id": 1, "schema_id": "sch_abc123", "name": "Archived",
            "description": None, "schema_json": {"properties": {}},
            "version": 1, "version_history": None, "archived": True,
            "created": "2026-01-01T00:00:00", "updated": "2026-01-04T00:00:00",
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "_make_request", new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                result = await client.delete_extraction_schema("sch_abc123")
                assert isinstance(result, ExtractionSchema)
                assert result.archived is True


class TestPipelineCRUD:
    """Test pipeline CRUD methods"""

    @pytest.mark.asyncio
    async def test_create_pipeline(self):
        mock_response = {
            "id": 1, "pipeline_id": "pl_abc123", "name": None,
            "steps": [{"type": "convert", "settings": {}}],
            "is_saved": False, "archived": False, "active_version": 0,
            "created": "2026-01-01T00:00:00", "updated": "2026-01-01T00:00:00",
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "_make_request", new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                result = await client.create_pipeline(
                    steps=[PipelineStep(type="convert")]
                )
                assert isinstance(result, PipelineConfig)
                assert result.pipeline_id == "pl_abc123"
                mock_req.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_list_pipelines(self):
        mock_response = {
            "pipelines": [{
                "id": 1, "pipeline_id": "pl_abc123", "name": "My Pipeline",
                "steps": [], "is_saved": True, "archived": False,
                "active_version": 1,
                "created": "2026-01-01T00:00:00", "updated": "2026-01-01T00:00:00",
            }],
            "total": 1,
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "_make_request", new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                result = await client.list_pipelines()
                assert result["total"] == 1
                assert isinstance(result["pipelines"][0], PipelineConfig)

    @pytest.mark.asyncio
    async def test_get_pipeline(self):
        mock_response = {
            "id": 1, "pipeline_id": "pl_abc123", "name": "Test",
            "steps": [{"type": "convert", "settings": {}}],
            "is_saved": True, "archived": False, "active_version": 1,
            "created": "2026-01-01T00:00:00", "updated": "2026-01-01T00:00:00",
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "_make_request", new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                result = await client.get_pipeline("pl_abc123")
                assert result.pipeline_id == "pl_abc123"
                mock_req.assert_awaited_once_with("GET", "/api/v1/pipelines/pl_abc123")


class TestPipelineExecution:
    """Test pipeline execution methods"""

    @pytest.mark.asyncio
    async def test_run_pipeline(self, temp_dir):
        pdf_file = temp_dir / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")

        mock_response = {
            "execution_id": "pex_abc123", "pipeline_id": "pl_abc123",
            "pipeline_version": 1, "status": "pending",
            "steps": [
                {"step_index": 0, "step_type": "convert", "status": "pending"},
            ],
            "created": "2026-01-01T00:00:00",
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "_submit_with_retry", new_callable=AsyncMock) as mock_submit:
                mock_submit.return_value = mock_response
                result = await client.run_pipeline("pl_abc123", file_path=pdf_file)
                assert isinstance(result, PipelineExecution)
                assert result.execution_id == "pex_abc123"
                assert len(result.steps) == 1
                assert isinstance(result.steps[0], PipelineExecutionStepResult)

    @pytest.mark.asyncio
    async def test_get_pipeline_execution(self):
        mock_response = {
            "execution_id": "pex_abc123", "pipeline_id": "pl_abc123",
            "pipeline_version": 1, "status": "completed",
            "steps": [
                {"step_index": 0, "step_type": "convert", "status": "completed",
                 "result_url": "/api/v1/pipelines/executions/pex_abc123/steps/0/result"},
            ],
            "created": "2026-01-01T00:00:00",
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "_make_request", new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                result = await client.get_pipeline_execution("pex_abc123")
                assert result.status == "completed"
                assert result.steps[0].result_url is not None


class TestCustomProcessorManagement:
    """Test custom processor management methods"""

    @pytest.mark.asyncio
    async def test_list_custom_processors(self):
        mock_response = {
            "pipelines": [{
                "processor_id": "cp_abc12", "name": "My Processor",
                "status": "completed", "success": True,
                "active_version": 1, "max_version": 2,
                "iteration_in_progress": False,
                "created_at": "2026-01-01T00:00:00",
            }],
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "_make_request", new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                result = await client.list_custom_processors()
                assert len(result["processors"]) == 1
                assert isinstance(result["processors"][0], CustomProcessor)
                assert result["processors"][0].processor_id == "cp_abc12"

    @pytest.mark.asyncio
    async def test_list_custom_processor_versions(self):
        mock_response = {
            "versions": [
                {"version": 2, "request_description": "Add totals", "created_at": "2026-01-02T00:00:00",
                 "runtime": 45.2, "is_active": True},
                {"version": 1, "request_description": "Initial", "created_at": "2026-01-01T00:00:00",
                 "runtime": 30.0, "is_active": False},
            ],
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "_make_request", new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                result = await client.list_custom_processor_versions("cp_abc12")
                assert len(result["versions"]) == 2
                assert isinstance(result["versions"][0], CustomProcessorVersion)
                assert result["versions"][0].is_active is True


class TestPipelineStepModel:
    """Test PipelineStep model"""

    def test_to_dict_minimal(self):
        step = PipelineStep(type="convert")
        d = step.to_dict()
        assert d == {"type": "convert", "settings": {}}

    def test_to_dict_with_custom_processor(self):
        step = PipelineStep(
            type="custom", settings={"mode": "fast"},
            custom_processor_id="cp_abc12", eval_rubric_id=5,
        )
        d = step.to_dict()
        assert d["custom_processor_id"] == "cp_abc12"
        assert d["eval_rubric_id"] == 5
