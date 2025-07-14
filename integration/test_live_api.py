"""
Integration tests for live API functionality

These tests require a valid API key and will make actual API calls.
Run with: pytest integration/ -v

Set environment variables:
- DATALAB_API_KEY: Your API key
- DATALAB_BASE_URL: Optional, defaults to https://www.datalab.to
"""

import pytest
import os
from pathlib import Path
from datalab_sdk import DatalabClient, AsyncDatalabClient
from datalab_sdk.models import ProcessingOptions, ConversionResult, OCRResult
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
        options = ProcessingOptions(max_pages=2)
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
        options = ProcessingOptions(output_format="html", max_pages=1)
        result = client.convert(doc_file, options=options)

        # Verify result
        assert isinstance(result, ConversionResult)
        assert result.success is True
        assert result.html is not None
        assert len(result.html) > 0
        assert result.output_format == "html"

    @pytest.mark.asyncio
    async def test_convert_async_with_json(self):
        """Test async conversion with JSON output"""
        async with AsyncDatalabClient() as client:
            # Test with PowerPoint file
            ppt_file = DATA_DIR / "08-Lambda-Calculus.pptx"

            # Convert to JSON format
            options = ProcessingOptions(output_format="json", max_pages=1)
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
        result = client.ocr(pdf_file, max_pages=1)

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
            result = await client.ocr(pdf_file, max_pages=2)

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
        options = ProcessingOptions(max_pages=1)
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
        assert saved_text == result.get_text()

        # Check JSON content
        import json

        saved_json = json.loads((output_path.with_suffix(".ocr.json")).read_text())
        assert saved_json["success"] is True
        assert len(saved_json["pages"]) == len(result.pages)
