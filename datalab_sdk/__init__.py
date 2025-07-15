"""
Datalab SDK - Python client for Datalab API

This SDK provides both synchronous and asynchronous interfaces to the Datalab API,
supporting document conversion, OCR, layout analysis, and table recognition.
"""

from .client import DatalabClient, AsyncDatalabClient
from .exceptions import DatalabError, DatalabAPIError, DatalabTimeoutError
from .models import ConversionResult, OCRResult, ConvertOptions, OCROptions

__version__ = "1.0.0"
__all__ = [
    "DatalabClient",
    "AsyncDatalabClient",
    "DatalabError",
    "DatalabAPIError",
    "DatalabTimeoutError",
    "ConversionResult",
    "OCRResult",
    "ConvertOptions",
    "OCROptions",
]
