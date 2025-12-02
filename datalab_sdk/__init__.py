"""
Datalab SDK - Python client for Datalab API

This SDK provides both synchronous and asynchronous interfaces to the Datalab API,
supporting document conversion, OCR, layout analysis, and table recognition.
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
    ExtractionSchemaGenerationResult,
    ThumbnailResult,
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
    "ExtractionSchemaGenerationResult",
    "ThumbnailResult",
]
