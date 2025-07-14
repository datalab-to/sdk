"""
Test configuration and fixtures for datalab_sdk tests
"""

import pytest
import asyncio
from pathlib import Path
from aiohttp import web
from aiohttp.test_utils import TestServer
import json
import tempfile

from datalab_sdk import DatalabClient, AsyncDatalabClient
from datalab_sdk.models import ProcessingOptions


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_pdf_file(temp_dir):
    """Create a sample PDF file for testing"""
    pdf_path = temp_dir / "test.pdf"
    # Create a minimal PDF-like content
    pdf_path.write_bytes(b"%PDF-1.4\n%Test PDF content\n%%EOF\n")
    return pdf_path


@pytest.fixture
def sample_image_file(temp_dir):
    """Create a sample image file for testing"""
    image_path = temp_dir / "test.png"
    # Create a minimal PNG-like content
    image_path.write_bytes(
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xdb\x00\x00\x00\x00IEND\xaeB`\x82"
    )
    return image_path


@pytest.fixture
def mock_api_responses():
    """Mock API responses for different endpoints"""
    return {
        "marker": {
            "initial": {
                "success": True,
                "error": None,
                "request_id": "test-request-id",
                "request_check_url": "https://api.datalab.to/api/v1/marker/test-request-id",
            },
            "result": {
                "success": True,
                "status": "complete",
                "output_format": "markdown",
                "markdown": "# Test Document\n\nThis is a test document.",
                "html": "<h1>Test Document</h1><p>This is a test document.</p>",
                "json": json.dumps({"content": "test"}),
                "images": {"image1.png": "base64encodedimage"},
                "metadata": {"pages": 1, "language": "en"},
                "error": "",
                "page_count": 1,
            },
        },
        "ocr": {
            "initial": {
                "success": True,
                "error": None,
                "request_id": "test-ocr-request-id",
                "request_check_url": "https://api.datalab.to/api/v1/ocr/test-ocr-request-id",
            },
            "result": {
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
            },
        },
    }


@pytest.fixture
async def mock_server(mock_api_responses):
    """Create a mock API server for testing"""

    async def handle_marker_post(request):
        return web.json_response(mock_api_responses["marker"]["initial"])

    async def handle_ocr_post(request):
        return web.json_response(mock_api_responses["ocr"]["initial"])

    async def handle_marker_get(request):
        return web.json_response(mock_api_responses["marker"]["result"])

    async def handle_ocr_get(request):
        return web.json_response(mock_api_responses["ocr"]["result"])

    app = web.Application()
    app.router.add_post("/api/v1/marker", handle_marker_post)
    app.router.add_post("/api/v1/ocr", handle_ocr_post)
    app.router.add_get("/api/v1/marker/{request_id}", handle_marker_get)
    app.router.add_get("/api/v1/ocr/{request_id}", handle_ocr_get)

    server = TestServer(app)
    await server.start_server()

    yield server

    await server.close()


@pytest.fixture
def mock_client(mock_server):
    """Create a client pointed at the mock server"""
    base_url = f"http://{mock_server.host}:{mock_server.port}"
    return DatalabClient(api_key="test-api-key", base_url=base_url)


@pytest.fixture
async def mock_async_client(mock_server):
    """Create an async client pointed at the mock server"""
    base_url = f"http://{mock_server.host}:{mock_server.port}"
    async with AsyncDatalabClient(api_key="test-api-key", base_url=base_url) as client:
        yield client


@pytest.fixture
def processing_options():
    """Create sample processing options"""
    return ProcessingOptions(
        force_ocr=True, output_format="markdown", use_llm=False, max_pages=10
    )


@pytest.fixture
def event_loop():
    """Create an event loop for async tests"""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
