"""
Integration tests for live API functionality

These tests require a valid API key and will make actual API calls.
Run with: pytest integration/ -v

Set environment variables:
- DATALAB_API_KEY: Your API key
- DATALAB_BASE_URL: Optional, defaults to https://www.datalab.to
"""

import json
import pytest
import os
from pathlib import Path
from datalab_sdk import DatalabClient, AsyncDatalabClient, FileResult
from datalab_sdk.models import ConversionResult, OCRResult, ConvertOptions, OCROptions
from datalab_sdk.exceptions import DatalabError

# Test data files
DATA_DIR = Path(__file__).parent.parent / "data"


class TestMarkerIntegration:
    """Integration tests for marker/convert functionality"""

    def test_convert_pdf_basic(self):
        """Test basic PDF conversion to markdown"""
        client = DatalabClient()

        # Use a small PDF file
        pdf_file = DATA_DIR / "adversarial.pdf"

        # Convert with limited pages to keep test fast
        options = ConvertOptions(max_pages=2)
        result = client.convert(pdf_file, options=options)

        # Verify result
        assert isinstance(result, ConversionResult)
        assert result.success is True
        assert result.markdown is not None
        assert len(result.markdown) > 0
        assert result.page_count is not None
        assert result.page_count > 0
        assert result.output_format == "markdown"

    def test_convert_office_document(self):
        """Test conversion of Office document"""
        client = DatalabClient()

        # Test with a Word document
        doc_file = DATA_DIR / "bid_evaluation.docx"

        # Convert to HTML format
        options = ConvertOptions(output_format="html", max_pages=1)
        result = client.convert(doc_file, options=options)

        # Verify result
        assert isinstance(result, ConversionResult)
        assert result.success is True
        assert result.html is not None
        assert len(result.html) > 0
        assert result.output_format == "html"

    def test_convert_pdf_high_accuracy(self):
        client = DatalabClient()
        pdf_file = DATA_DIR / "adversarial.pdf"
        options = ConvertOptions(mode="accurate", max_pages=1)
        result = client.convert(pdf_file, options=options)

        assert "subspace" in result.markdown.lower()

    @pytest.mark.asyncio
    async def test_convert_async_with_json(self):
        """Test async conversion with JSON output"""
        async with AsyncDatalabClient() as client:
            # Test with PowerPoint file
            ppt_file = DATA_DIR / "08-Lambda-Calculus.pptx"

            # Convert to JSON format
            options = ConvertOptions(output_format="json", max_pages=1)
            result = await client.convert(ppt_file, options=options)

            # Verify result
            assert isinstance(result, ConversionResult)
            assert result.success is True
            assert result.json is not None
            assert result.output_format == "json"

            # Try to parse as JSON
            assert isinstance(result.json, (dict, list))


class TestOCRIntegration:
    """Integration tests for OCR functionality"""

    def test_ocr_pdf_basic(self):
        """Test basic OCR on PDF"""
        client = DatalabClient()

        # Use a PDF file
        pdf_file = DATA_DIR / "thinkpython.pdf"

        # OCR with limited pages
        options = OCROptions(max_pages=1)
        result = client.ocr(pdf_file, options)

        # Verify result
        assert isinstance(result, OCRResult)
        assert result.success is True
        assert result.pages is not None
        assert len(result.pages) > 0
        assert result.page_count is not None
        assert result.page_count > 0

        # Check page structure
        page = result.pages[0]
        assert "text_lines" in page
        assert "page" in page
        assert isinstance(page["text_lines"], list)

        # Check text extraction
        text = result.get_text()
        assert isinstance(text, str)
        assert len(text) > 0

    def test_ocr_image_file(self):
        """Test OCR on image file"""
        client = DatalabClient()

        # Use an image file
        image_file = DATA_DIR / "chi_hind.png"

        # OCR the image
        result = client.ocr(image_file)

        # Verify result
        assert isinstance(result, OCRResult)
        assert result.success is True
        assert result.pages is not None
        assert len(result.pages) > 0

        # Check that we got text lines
        page = result.pages[0]
        assert "text_lines" in page
        assert isinstance(page["text_lines"], list)

        # Check text extraction works
        text = result.get_text()
        assert isinstance(text, str)

    @pytest.mark.asyncio
    async def test_ocr_async_multiple_pages(self):
        """Test async OCR with multiple pages"""
        async with AsyncDatalabClient() as client:
            # Use a PDF with multiple pages
            pdf_file = DATA_DIR / "adversarial.pdf"

            # OCR with limited pages
            options = OCROptions(max_pages=2)
            result = await client.ocr(pdf_file, options)

            # Verify result
            assert isinstance(result, OCRResult)
            assert result.success is True
            assert result.pages is not None
            assert len(result.pages) <= 2  # Should respect max_pages
            assert result.page_count is not None

            # Check that each page has the expected structure
            for page in result.pages:
                assert "text_lines" in page
                assert "page" in page
                assert "image_bbox" in page
                assert isinstance(page["text_lines"], list)

                # Check text line structure
                for line in page["text_lines"]:
                    assert "text" in line
                    assert "confidence" in line
                    assert "bbox" in line
                    assert isinstance(line["text"], str)
                    assert isinstance(line["confidence"], (int, float))
                    assert isinstance(line["bbox"], list)


class TestErrorHandling:
    """Test error handling with live API"""

    def test_invalid_api_key(self):
        """Test behavior with invalid API key"""
        with pytest.raises(DatalabError):
            client = DatalabClient(api_key="invalid-key")
            pdf_file = DATA_DIR / "adversarial.pdf"
            client.convert(pdf_file)

    def test_nonexistent_file(self):
        """Test behavior with nonexistent file"""
        client = DatalabClient()

        with pytest.raises(Exception):  # Should raise DatalabFileError
            client.convert("nonexistent_file.pdf")

    def test_unsupported_file_type(self):
        """Test behavior with unsupported file type"""
        client = DatalabClient()

        # Create a temporary file with unsupported extension
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=".xyz", delete=False) as tmp:
            tmp.write(b"test content")
            tmp_path = tmp.name

        try:
            with pytest.raises(Exception):  # Should raise some kind of error
                client.convert(tmp_path)
        finally:
            os.unlink(tmp_path)


class TestSaveOutput:
    """Test save_output functionality with live API"""

    def test_convert_with_save_output(self, tmp_path):
        """Test convert with automatic saving"""
        client = DatalabClient()

        # Use a small file
        pdf_file = DATA_DIR / "adversarial.pdf"
        output_path = tmp_path / "test_output"

        # Convert with save_output
        options = ConvertOptions(max_pages=1)
        result = client.convert(pdf_file, options=options, save_output=output_path)

        # Verify result
        assert result.success is True

        # Check that file was saved
        assert (output_path.with_suffix(".md")).exists()

        # Check content
        saved_content = (output_path.with_suffix(".md")).read_text()
        assert len(saved_content) > 0
        assert saved_content == result.markdown

    def test_ocr_with_save_output(self, tmp_path):
        """Test OCR with automatic saving"""
        client = DatalabClient()

        # Use an image file
        image_file = DATA_DIR / "chi_hind.png"
        output_path = tmp_path / "ocr_output"

        # OCR with save_output
        result = client.ocr(image_file, save_output=output_path)

        # Verify result
        assert result.success is True

        # Check that files were saved
        assert (output_path.with_suffix(".txt")).exists()
        assert (output_path.with_suffix(".ocr.json")).exists()

        # Check text content
        saved_text = (output_path.with_suffix(".txt")).read_text()
        assert len(saved_text) > 0
        assert saved_text == json.dumps(result.pages, indent=2)

        saved_json = json.loads((output_path.with_suffix(".ocr.json")).read_text())
        assert saved_json["success"] is True
        assert len(saved_json["pages"]) == len(result.pages)


class TestStreamResponseTo:
    """Test stream_response_to functionality with live API"""

    def test_convert_stream_to_disk(self, tmp_path):
        """Test that convert streams JSON response to disk and returns FileResult"""
        client = DatalabClient()
        pdf_file = DATA_DIR / "adversarial.pdf"
        output_file = tmp_path / "result.json"

        options = ConvertOptions(max_pages=1)
        result = client.convert(
            pdf_file, options=options, stream_response_to=output_file
        )

        assert isinstance(result, FileResult)
        assert result.success is True
        assert result.status == "complete"
        assert result.output_path == output_file
        assert not result.error

        # Verify the file exists and contains valid JSON
        assert output_file.exists()
        data = json.loads(output_file.read_text())
        assert data["status"] == "complete"
        assert data["success"] is True
        assert "markdown" in data

    def test_convert_normal_path_unchanged(self):
        """Test that convert without stream_response_to still returns ConversionResult"""
        client = DatalabClient()
        pdf_file = DATA_DIR / "adversarial.pdf"

        options = ConvertOptions(max_pages=1)
        result = client.convert(pdf_file, options=options)

        assert isinstance(result, ConversionResult)
        assert result.success is True
        assert result.markdown is not None

    def test_save_output_and_stream_response_to_mutually_exclusive(self):
        """Test that using both save_output and stream_response_to raises ValueError"""
        client = DatalabClient()
        pdf_file = DATA_DIR / "adversarial.pdf"

        with pytest.raises(ValueError, match="Cannot use both"):
            client.convert(
                pdf_file,
                save_output="/tmp/out",
                stream_response_to="/tmp/stream.json",
            )

    def test_stream_response_to_invalid_directory(self):
        """Test that a non-existent parent directory raises ValueError"""
        client = DatalabClient()
        pdf_file = DATA_DIR / "adversarial.pdf"

        with pytest.raises(ValueError, match="Directory does not exist"):
            client.convert(
                pdf_file,
                stream_response_to="/nonexistent/dir/result.json",
            )

    @pytest.mark.asyncio
    async def test_convert_stream_async(self, tmp_path):
        """Test async convert with stream_response_to"""
        async with AsyncDatalabClient() as client:
            pdf_file = DATA_DIR / "adversarial.pdf"
            output_file = tmp_path / "async_result.json"

            options = ConvertOptions(max_pages=1)
            result = await client.convert(
                pdf_file, options=options, stream_response_to=output_file
            )

            assert isinstance(result, FileResult)
            assert result.success is True
            assert result.status == "complete"
            assert output_file.exists()

            data = json.loads(output_file.read_text())
            assert data["status"] == "complete"

    def test_convert_stream_html_format(self, tmp_path):
        """Test streaming with HTML output format"""
        client = DatalabClient()
        pdf_file = DATA_DIR / "adversarial.pdf"
        output_file = tmp_path / "result.json"

        options = ConvertOptions(output_format="html", max_pages=1)
        result = client.convert(
            pdf_file, options=options, stream_response_to=output_file
        )

        assert isinstance(result, FileResult)
        assert result.success is True
        assert output_file.exists()

        data = json.loads(output_file.read_text())
        assert "html" in data
