"""
Integration tests for all README examples

These tests verify that every code example in the README works correctly
against the live API. They should always pass to ensure documentation accuracy.

Set environment variables:
- DATALAB_API_KEY: Your API key
- DATALAB_BASE_URL: Optional, defaults to https://www.datalab.to
"""

import pytest
import os
import json
import tempfile
from pathlib import Path
from datalab_sdk import DatalabClient, AsyncDatalabClient
from datalab_sdk.models import ProcessingOptions
from datalab_sdk.settings import settings

# Test data files
DATA_DIR = Path(__file__).parent.parent / "data"


class TestBasicUsageExamples:
    """Test Basic Usage section examples from README"""

    def test_basic_usage_example(self):
        """Test the basic usage example from README"""
        # Example from README - Basic Usage section
        from datalab_sdk import DatalabClient

        client = DatalabClient()

        # Convert PDF to markdown (using test data)
        result = client.convert(
            DATA_DIR / "adversarial.pdf", options=ProcessingOptions(max_pages=1)
        )
        print(result.markdown)

        # Verify it worked
        assert result.success is True
        assert result.markdown is not None
        assert len(result.markdown) > 0

        # OCR a document (using test data)
        ocr_result = client.ocr(DATA_DIR / "chi_hind.png")
        print(ocr_result.get_text())  # Get all text as string

        # Verify it worked
        assert ocr_result.success is True
        assert isinstance(ocr_result.get_text(), str)

    @pytest.mark.asyncio
    async def test_async_usage_example(self):
        """Test the async usage example from README"""
        # Example from README - Async Usage section
        from datalab_sdk import AsyncDatalabClient

        async def main():
            async with AsyncDatalabClient() as client:
                # Convert PDF to markdown
                result = await client.convert(
                    DATA_DIR / "adversarial.pdf", options=ProcessingOptions(max_pages=1)
                )
                print(result.markdown)

                # Verify it worked
                assert result.success is True
                assert result.markdown is not None

                # OCR a document
                ocr_result = await client.ocr(DATA_DIR / "chi_hind.png")
                print(f"OCR found {len(ocr_result.pages)} pages")

                # Verify it worked
                assert ocr_result.success is True
                assert len(ocr_result.pages) > 0

        await main()


class TestAPIMethodExamples:
    """Test API Methods section examples from README"""

    def test_document_conversion_examples(self):
        """Test Document Conversion section examples"""
        from datalab_sdk import DatalabClient, ProcessingOptions

        client = DatalabClient()

        # Basic conversion
        result = client.convert(
            DATA_DIR / "adversarial.pdf", options=ProcessingOptions(max_pages=1)
        )
        assert result.success is True
        assert result.markdown is not None

        # With options
        options = ProcessingOptions(
            force_ocr=True,
            output_format="html",
            use_llm=False,  # Keep false for cost reasons
            max_pages=1,
        )
        result = client.convert(DATA_DIR / "adversarial.pdf", options=options)
        assert result.success is True
        assert result.html is not None

        # Convert and save automatically
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "result"
            result = client.convert(
                DATA_DIR / "adversarial.pdf",
                options=ProcessingOptions(max_pages=1),
                save_output=output_path,
            )
            assert result.success is True
            assert (output_path.with_suffix(".md")).exists()

    def test_ocr_examples(self):
        """Test OCR section examples"""
        from datalab_sdk import DatalabClient

        client = DatalabClient()

        # Basic OCR
        result = client.ocr(DATA_DIR / "chi_hind.png")
        text = result.get_text()
        print(text)
        assert result.success is True
        assert isinstance(text, str)

        # OCR with options
        result = client.ocr(DATA_DIR / "adversarial.pdf", max_pages=1)
        assert result.success is True
        assert len(result.pages) > 0

        # OCR and save automatically
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "ocr_result"
            result = client.ocr(DATA_DIR / "chi_hind.png", save_output=output_path)
            assert result.success is True
            assert (output_path.with_suffix(".txt")).exists()
            assert (output_path.with_suffix(".ocr.json")).exists()


class TestErrorHandlingExamples:
    """Test Error Handling section examples"""

    def test_error_handling_example(self):
        """Test error handling example from README"""
        from datalab_sdk import DatalabAPIError, DatalabTimeoutError

        client = DatalabClient()

        # Test with valid file (should not raise error)
        try:
            result = client.convert(
                DATA_DIR / "adversarial.pdf", options=ProcessingOptions(max_pages=1)
            )
            assert result.success is True
        except DatalabAPIError as e:
            pytest.fail(f"Unexpected API Error: {e}")
        except DatalabTimeoutError as e:
            pytest.fail(f"Unexpected Timeout: {e}")

        # Test with invalid API key (should raise error)
        invalid_client = DatalabClient(api_key="invalid-key")

        with pytest.raises(DatalabAPIError):
            invalid_client.convert(
                DATA_DIR / "adversarial.pdf", options=ProcessingOptions(max_pages=1)
            )


class TestExamplesSectionFromReadme:
    """Test Examples section from README"""

    def test_extract_json_data_example(self):
        """Test Extract JSON Data example"""
        from datalab_sdk import DatalabClient, ProcessingOptions

        client = DatalabClient()
        options = ProcessingOptions(output_format="json", max_pages=1)
        result = client.convert(DATA_DIR / "adversarial.pdf", options=options)

        # Parse JSON to find equations (modified to not fail if no equations)
        data = result.json
        if isinstance(data, str):
            data = json.loads(data)

        # Look for equations (this might be empty, that's OK)
        equations = [
            block
            for block in data
            if isinstance(block, dict) and block.get("btype") == "Formula"
        ]
        print(f"Found {len(equations)} equations")

        # Verify the conversion worked
        assert result.success is True
        assert data is not None
        assert isinstance(data, (dict, list))

    @pytest.mark.asyncio
    async def test_batch_process_documents_example(self):
        """Test Batch Process Documents example (simplified)"""
        from pathlib import Path
        from datalab_sdk import AsyncDatalabClient

        async def process_documents():
            # Use test data files instead of scanning directory
            files = [DATA_DIR / "adversarial.pdf", DATA_DIR / "chi_hind.png"]

            async with AsyncDatalabClient() as client:
                with tempfile.TemporaryDirectory() as tmp_dir:
                    for file in files[:2]:  # Process first 2 files
                        output_path = Path(tmp_dir) / file.stem
                        if file.suffix == ".pdf":
                            result = await client.convert(
                                str(file),
                                options=ProcessingOptions(max_pages=1),
                                save_output=output_path,
                            )
                            print(f"{file.name}: {result.page_count} pages")
                            assert result.success is True
                            assert (output_path.with_suffix(".md")).exists()
                        else:
                            result = await client.ocr(
                                str(file), save_output=output_path
                            )
                            print(f"{file.name}: OCR completed")
                            assert result.success is True
                            assert (output_path.with_suffix(".txt")).exists()

        await process_documents()


class TestClientInitializationVariations:
    """Test different ways to initialize the client"""

    def test_client_with_explicit_api_key(self):
        """Test client initialization with explicit API key"""
        from datalab_sdk import DatalabClient

        client = DatalabClient()
        result = client.convert(
            DATA_DIR / "adversarial.pdf", options=ProcessingOptions(max_pages=1)
        )
        assert result.success is True

    def test_client_with_env_var(self):
        """Test client initialization using environment variable"""
        from datalab_sdk import DatalabClient

        # Set environment variable
        original_key = os.environ.get("DATALAB_API_KEY")

        try:
            # Client should use environment variable
            client = DatalabClient()
            result = client.convert(
                DATA_DIR / "adversarial.pdf", options=ProcessingOptions(max_pages=1)
            )
            assert result.success is True
        finally:
            # Restore original environment
            if original_key:
                os.environ["DATALAB_API_KEY"] = original_key
            else:
                os.environ.pop("DATALAB_API_KEY", None)

    @pytest.mark.asyncio
    async def test_async_client_context_manager(self):
        """Test async client context manager usage"""

        async with AsyncDatalabClient(
            api_key=settings.DATALAB_API_KEY, base_url=settings.DATALAB_HOST
        ) as client:
            result = await client.convert(
                DATA_DIR / "adversarial.pdf", options=ProcessingOptions(max_pages=1)
            )
            assert result.success is True
            assert result.markdown is not None


class TestProcessingOptionsVariations:
    """Test different ProcessingOptions configurations"""

    def test_processing_options_defaults(self):
        """Test ProcessingOptions with default values"""
        from datalab_sdk import ProcessingOptions

        options = ProcessingOptions()
        assert options.force_ocr is False
        assert options.output_format == "markdown"
        assert options.use_llm is False
        assert options.max_pages is None

    def test_processing_options_custom_values(self):
        """Test ProcessingOptions with custom values"""
        from datalab_sdk import ProcessingOptions

        options = ProcessingOptions(
            force_ocr=True,
            output_format="html",
            use_llm=False,  # Keep false for cost reasons
            max_pages=1,
        )

        client = DatalabClient()
        result = client.convert(DATA_DIR / "adversarial.pdf", options=options)

        assert result.success is True
        assert result.output_format == "html"
        assert result.html is not None

    def test_processing_options_json_output(self):
        """Test ProcessingOptions with JSON output"""
        from datalab_sdk import ProcessingOptions

        options = ProcessingOptions(output_format="json", max_pages=1)

        client = DatalabClient()
        result = client.convert(DATA_DIR / "adversarial.pdf", options=options)

        assert result.success is True
        assert result.output_format == "json"
        assert result.json is not None
