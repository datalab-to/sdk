"""
Tests for Collection functionality
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, AsyncMock
from datalab_sdk import Collection, DatalabClient, ConvertOptions, AsyncDatalabClient
from datalab_sdk.collections import CollectionResult
from datalab_sdk.models import ConversionResult, OCRResult
from datalab_sdk.exceptions import DatalabError


@pytest.fixture
def collection_test_dir():
    """Create a temporary directory with test files for collection testing"""
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_dir = Path(tmpdir)

        # Create some dummy files with different extensions
        (temp_dir / "doc1.pdf").write_text("dummy pdf content 1")
        (temp_dir / "doc2.pdf").write_text("dummy pdf content 2")
        (temp_dir / "doc3.docx").write_text("dummy docx content")
        (temp_dir / "image.jpg").write_text("dummy jpg content")
        (temp_dir / "spreadsheet.xlsx").write_text("dummy xlsx content")

        yield temp_dir


class TestCollectionCreation:
    """Test different methods of creating collections"""

    def test_from_local_directory_all_extensions(self, collection_test_dir):
        """Test creating collection from local directory with all supported extensions"""
        collection = Collection.from_local_directory("test", collection_test_dir)

        assert collection.name == "test"
        assert len(collection.sources) > 0
        # Should find files with various extensions
        assert any(str(source).endswith('.pdf') for source in collection.sources)
        assert any(str(source).endswith('.docx') for source in collection.sources)

    def test_from_local_directory_filtered_extensions(self, collection_test_dir):
        """Test creating collection with specific file extensions"""
        collection = Collection.from_local_directory("test-pdf", collection_test_dir, [".pdf"])

        assert collection.name == "test-pdf"
        assert len(collection.sources) == 2  # Only 2 PDF files
        assert all(str(source).endswith('.pdf') for source in collection.sources)

    def test_from_local_directory_nonexistent(self):
        """Test creating collection from nonexistent directory"""
        with pytest.raises(ValueError, match="Directory not found"):
            Collection.from_local_directory("test", "/nonexistent/directory")

    def test_from_urls(self):
        """Test creating collection from list of URLs"""
        urls = [
            "https://example.com/doc1.pdf",
            "https://example.com/doc2.pdf",
            "s3://bucket/doc3.pdf"
        ]

        collection = Collection.from_urls("test-urls", urls)

        assert collection.name == "test-urls"
        assert len(collection.sources) == 3
        assert collection.sources == urls

    def test_from_mixed_sources(self, collection_test_dir):
        """Test creating collection from mixed sources"""
        mixed_sources = [
            str(collection_test_dir / "doc1.pdf"),
            "https://example.com/doc2.pdf",
            "s3://bucket/doc3.pdf"
        ]

        collection = Collection.from_mixed_sources("mixed", mixed_sources)

        assert collection.name == "mixed"
        assert len(collection.sources) == 3
        assert collection.sources == mixed_sources

    def test_from_s3_prefix_invalid_uri(self):
        """Test S3 collection with invalid URI"""
        with pytest.raises(ValueError, match="Invalid S3 URI"):
            Collection.from_s3_prefix("test", "http://invalid-uri")

    def test_from_s3_prefix_success(self):
        """Test successful S3 collection creation"""
        mock_s3_objects = [
            {'Key': 'docs/file1.pdf'},
            {'Key': 'docs/file2.pdf'},
            {'Key': 'docs/other.txt'}
        ]

        with patch('boto3.Session') as mock_session_class:
            # Mock session and client
            mock_session = mock_session_class.return_value
            mock_client = mock_session.client.return_value

            # Mock paginator
            mock_paginator = mock_client.get_paginator.return_value
            mock_paginator.paginate.return_value = [{'Contents': mock_s3_objects}]

            collection = Collection.from_s3_prefix(
                "test-s3",
                "s3://bucket/docs/",
                [".pdf"]
            )

            assert collection.name == "test-s3"
            assert len(collection.sources) == 2  # Only PDF files
            assert "s3://bucket/docs/file1.pdf" in collection.sources
            assert "s3://bucket/docs/file2.pdf" in collection.sources
            assert "s3://bucket/docs/other.txt" not in collection.sources


class TestCollectionProcessing:
    """Test collection processing methods"""

    @pytest.mark.asyncio
    async def test_convert_all_success(self, collection_test_dir):
        """Test successful conversion of all documents in collection"""
        collection = Collection.from_local_directory("test", collection_test_dir, [".pdf"])

        # Mock successful conversion result
        mock_result = ConversionResult(
            success=True,
            output_format="markdown",
            markdown="# Test Document",
            page_count=1,
            status="complete"
        )

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "convert", new_callable=AsyncMock) as mock_convert:
                mock_convert.return_value = mock_result

                result = await collection.convert_all(
                    client=client,
                    options=ConvertOptions(),
                    max_concurrent=2
                )

                # Verify results
                assert isinstance(result, CollectionResult)
                assert result.collection_name == "test"
                assert result.total_files == 2
                assert result.successful == 2
                assert result.failed == 0
                assert len(result.results) == 2
                assert len(result.errors) == 0

                # Verify convert was called for each file
                assert mock_convert.await_count == 2

    @pytest.mark.asyncio
    async def test_ocr_all_success(self, collection_test_dir):
        """Test successful OCR of all documents in collection"""
        collection = Collection.from_local_directory("test", collection_test_dir, [".pdf"])

        # Mock successful OCR result
        mock_result = OCRResult(
            success=True,
            pages=[{"text_lines": [{"text": "Test content"}], "page": 1}],
            page_count=1,
            status="complete"
        )

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "ocr", new_callable=AsyncMock) as mock_ocr:
                mock_ocr.return_value = mock_result

                result = await collection.ocr_all(
                    client=client,
                    max_concurrent=1
                )

                # Verify results
                assert isinstance(result, CollectionResult)
                assert result.collection_name == "test"
                assert result.total_files == 2
                assert result.successful == 2
                assert result.failed == 0

                # Verify OCR was called for each file
                assert mock_ocr.await_count == 2

    @pytest.mark.asyncio
    async def test_convert_all_partial_failure(self, collection_test_dir):
        """Test collection processing with some failures"""
        collection = Collection.from_local_directory("test", collection_test_dir, [".pdf"])

        # Mock results: first succeeds, second fails
        success_result = ConversionResult(
            success=True,
            output_format="markdown",
            markdown="# Success",
            page_count=1,
            status="complete"
        )

        failure_result = ConversionResult(
            success=False,
            output_format="markdown",
            error="Processing failed",
            status="failed"
        )

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "convert", new_callable=AsyncMock) as mock_convert:
                mock_convert.side_effect = [success_result, failure_result]

                result = await collection.convert_all(client=client)

                # Verify mixed results
                assert result.total_files == 2
                assert result.successful == 1
                assert result.failed == 1
                assert len(result.errors) == 1
                assert "Processing failed" in result.errors[0]["error"]

    @pytest.mark.asyncio
    async def test_ocr_all_with_urls_fails(self):
        """Test that OCR fails with URL sources"""
        collection = Collection.from_urls("test", ["https://example.com/doc.pdf"])

        async with AsyncDatalabClient(api_key="test-key") as client:
            result = await collection.ocr_all(client=client)

            # Should fail because OCR doesn't support URLs
            assert result.total_files == 1
            assert result.successful == 0
            assert result.failed == 1
            assert "OCR method only supports local files" in result.errors[0]["error"]

    @pytest.mark.asyncio
    async def test_convert_all_with_output_dir(self, collection_test_dir, temp_dir):
        """Test collection processing with output directory"""
        collection = Collection.from_local_directory("test", collection_test_dir, [".pdf"])

        mock_result = ConversionResult(
            success=True,
            output_format="markdown",
            markdown="# Test",
            page_count=1,
            status="complete"
        )

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "convert", new_callable=AsyncMock) as mock_convert:
                mock_convert.return_value = mock_result

                result = await collection.convert_all(
                    client=client,
                    output_dir=temp_dir / "output"
                )

                # Verify output directory was created
                assert (temp_dir / "output").exists()
                assert result.successful == 2


class TestSyncCollectionProcessing:
    """Test synchronous collection processing through DatalabClient"""

    def test_process_collection_convert(self, collection_test_dir):
        """Test sync collection processing for convert method"""
        collection = Collection.from_local_directory("test", collection_test_dir, [".pdf"])

        mock_result = CollectionResult(
            collection_name="test",
            total_files=2,
            successful=2,
            failed=0,
            results=[],
            errors=[]
        )

        client = DatalabClient(api_key="test-key")

        with patch.object(collection, "convert_all", new_callable=AsyncMock) as mock_convert:
            mock_convert.return_value = mock_result

            result = client.process_collection(collection, method="convert")

            assert isinstance(result, CollectionResult)
            assert result.collection_name == "test"
            assert result.successful == 2

    def test_process_collection_ocr(self, collection_test_dir):
        """Test sync collection processing for OCR method"""
        collection = Collection.from_local_directory("test", collection_test_dir, [".pdf"])

        mock_result = CollectionResult(
            collection_name="test",
            total_files=2,
            successful=2,
            failed=0,
            results=[],
            errors=[]
        )

        client = DatalabClient(api_key="test-key")

        with patch.object(collection, "ocr_all", new_callable=AsyncMock) as mock_ocr:
            mock_ocr.return_value = mock_result

            result = client.process_collection(collection, method="ocr")

            assert isinstance(result, CollectionResult)
            assert result.collection_name == "test"

    def test_process_collection_invalid_method(self, collection_test_dir):
        """Test collection processing with invalid method"""
        collection = Collection.from_local_directory("test", collection_test_dir, [".pdf"])
        client = DatalabClient(api_key="test-key")

        with pytest.raises(ValueError, match="Unsupported method"):
            client.process_collection(collection, method="invalid")