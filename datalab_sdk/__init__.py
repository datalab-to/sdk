"""
Datalab SDK - Python client for Datalab API

This SDK provides both synchronous and asynchronous interfaces to the Datalab API,
supporting document conversion, extraction, segmentation, and more.
"""

from .client import DatalabClient, AsyncDatalabClient
from .exceptions import DatalabError, DatalabAPIError, DatalabTimeoutError
from .models import (
    ConversionResult,
    CreateDocumentResult,
    OCRResult,
    ConvertOptions,
    ExtractOptions,
    SegmentOptions,
    CustomPipelineOptions,
    TrackChangesOptions,
    OCROptions,
    FormFillingOptions,
    FormFillingResult,
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
    "CreateDocumentResult",
    "OCRResult",
    "ConvertOptions",
    "ExtractOptions",
    "SegmentOptions",
    "CustomPipelineOptions",
    "TrackChangesOptions",
    "OCROptions",
    "FormFillingOptions",
    "FormFillingResult",
    "Workflow",
    "WorkflowStep",
    "WorkflowExecution",
    "InputConfig",
    "UploadedFileMetadata",
]
