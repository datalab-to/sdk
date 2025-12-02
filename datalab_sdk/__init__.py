"""
Datalab SDK - Python client for Datalab API

This SDK provides both synchronous and asynchronous interfaces to the Datalab API,
supporting document conversion, OCR, and layout analysis.
"""

from .client import DatalabClient, AsyncDatalabClient
from .exceptions import DatalabError, DatalabAPIError, DatalabTimeoutError
from .models import (
    ConversionResult,
    OCRResult,
    ConvertOptions,
    OCROptions,
    Workflow,
    WorkflowStep,
    WorkflowExecution,
    InputConfig,
    UploadedFileMetadata,
)
from .settings import settings

__version__ = settings.VERSION
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
    "Workflow",
    "WorkflowStep",
    "WorkflowExecution",
    "InputConfig",
    "UploadedFileMetadata",
]
