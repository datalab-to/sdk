"""
Datalab SDK data models
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Union, Literal
from pathlib import Path
import json
import base64
import warnings


@dataclass
class ProcessingOptions:
    """Options that most processing endpoints accept.

    Every option maps to the API form field with the same name. The SDK
    sends a field only when its value is not None.
    """

    # Common options
    max_pages: Optional[int] = None
    skip_cache: bool = False
    page_range: Optional[str] = None

    def to_form_data(self) -> Dict[str, Any]:
        """Convert to form data format for API requests"""
        form_data = {}

        # Add non-None values
        for key, value in self.__dict__.items():
            if value is not None:
                if isinstance(value, bool):
                    form_data[key] = (None, value)
                elif isinstance(value, (dict, list)):
                    form_data[key] = (None, json.dumps(value, indent=2))
                else:
                    form_data[key] = (None, value)

        return form_data


# Keys of the convert `additional_config` JSON that ConvertOptions exposes as
# typed boolean fields. The API accepts these keys inside additional_config.
_ADDITIONAL_CONFIG_FLAGS = (
    "keep_spreadsheet_formatting",
    "keep_pageheader_in_output",
    "keep_pagefooter_in_output",
)


@dataclass
class ConvertOptions(ProcessingOptions):
    """Options for document conversion via the /convert endpoint.

    Note: the SDK default for `mode` is "fast". The API default is
    "balanced". Set `mode` explicitly to choose the mode.
    """

    paginate: bool = False
    disable_image_extraction: bool = False
    disable_image_captions: bool = False
    fence_synthetic_captions: bool = False
    additional_config: Optional[Dict[str, Any]] = None
    save_checkpoint: bool = False
    output_format: str = (
        "markdown"  # markdown, json, html, chunks (comma-separated for multiple)
    )
    mode: str = "fast"  # lite, fast, balanced, accurate
    keep_spreadsheet_formatting: bool = False  # sent inside additional_config
    webhook_url: Optional[str] = None
    # Comma-separated list of extra features: 'track_changes',
    # 'chart_understanding', 'table_cell_bboxes', 'list_item_bboxes',
    # 'extract_links', 'extract_bookmarks', 'infographic', 'new_block_types'
    extras: Optional[str] = None
    add_block_ids: bool = False  # add block IDs to HTML output
    include_markdown_in_chunks: bool = (
        False  # include markdown field in chunks/JSON output
    )
    token_efficient_markdown: bool = False  # optimize markdown for LLM token usage
    # Merge tables, paragraphs and lists that continue across pages.
    # The API bills a compute surcharge for this option.
    merge_cross_page: bool = False
    # Predict per-word bounding boxes with confidence scores (HTML output).
    word_bboxes: bool = False
    # Return compact HTML instead of indented HTML.
    disable_html_prettify: bool = False
    # Transcribe text character for character, with no normalization.
    exact_transcription: bool = False
    keep_pageheader_in_output: bool = False  # sent inside additional_config
    keep_pagefooter_in_output: bool = False  # sent inside additional_config
    # Residency region override, for example "us" or "eu". When you set it,
    # send the document as file_url (or a datalab:// file reference).
    processing_location: Optional[str] = None

    def to_form_data(self) -> Dict[str, Any]:
        """Convert to form data format for API requests"""
        form_data = super().to_form_data()

        # The typed additional_config flags go inside additional_config,
        # not at the top level.
        for flag in _ADDITIONAL_CONFIG_FLAGS:
            form_data.pop(flag, None)

        additional_config_dict = {}
        if self.additional_config:
            additional_config_dict.update(self.additional_config)
        for flag in _ADDITIONAL_CONFIG_FLAGS:
            if getattr(self, flag):
                additional_config_dict[flag] = True

        if additional_config_dict:
            form_data["additional_config"] = (None, json.dumps(additional_config_dict))

        return form_data


@dataclass
class ExtractOptions(ProcessingOptions):
    """Options for structured data extraction via the /extract endpoint.

    When `mode` is None, the SDK does not send it. The API then selects the
    parse mode that matches `extraction_mode`: "accurate" for balanced and
    accurate extraction, and "fast" for the other modes.
    """

    page_schema: str = (
        ""  # JSON schema with 'properties' key. Mutually exclusive with schema_id.
    )
    schema_id: Optional[str] = (
        None  # ID of a saved extraction schema (e.g. sch_k8Hx9mP2nQ4v). Mutually exclusive with page_schema.
    )
    schema_version: Optional[int] = (
        None  # Version of the schema. Only valid with schema_id.
    )
    checkpoint_id: Optional[str] = (
        None  # From previous /convert with save_checkpoint=true
    )
    # Parse mode: fast, balanced, accurate. None lets the API choose.
    mode: Optional[str] = None
    # Extraction mode: turbo, fast, balanced, accurate. None lets the API
    # choose (the default is balanced). Turbo needs a file, not a checkpoint.
    extraction_mode: Optional[str] = None
    output_format: str = "markdown"  # markdown, json, html, chunks
    save_checkpoint: bool = False
    webhook_url: Optional[str] = None
    processing_location: Optional[str] = None  # residency region, e.g. "eu"

    def to_form_data(self) -> Dict[str, Any]:
        """Convert to form data format for API requests"""
        form_data = super().to_form_data()
        # When using schema_id, suppress the empty default page_schema
        if self.schema_id and not self.page_schema:
            form_data.pop("page_schema", None)
        return form_data


@dataclass
class SegmentOptions(ProcessingOptions):
    """Options for document segmentation via the /segment endpoint"""

    # Required. JSON object with 'segments', and optional
    # 'segmentation_strategy', 'granularity' and 'prompt' keys.
    segmentation_schema: str = ""
    checkpoint_id: Optional[str] = (
        None  # From previous /convert with save_checkpoint=true
    )
    mode: str = "fast"  # fast, balanced, accurate
    save_checkpoint: bool = False
    webhook_url: Optional[str] = None
    processing_location: Optional[str] = None  # residency region, e.g. "eu"


@dataclass
class CustomProcessorOptions(ProcessingOptions):
    """Options for running a custom processor via /custom-processor.

    Deprecated: use AgentOptions with processor="cp_XXXXX" and run_agent().
    """

    pipeline_id: str = ""  # Required - custom processor ID (cp_XXXXX format)
    version: Optional[int] = (
        None  # Specify processor version to run (default: active version)
    )
    mode: str = "fast"  # fast, balanced, accurate
    output_format: str = "markdown"  # markdown, json, html, chunks
    paginate: bool = (
        False  # Separate pages with horizontal rules containing page numbers
    )
    add_block_ids: bool = False  # Add data-block-id attributes to HTML elements
    include_markdown_in_chunks: bool = (
        False  # Include markdown field in chunks/JSON output
    )
    disable_image_extraction: bool = False  # Disable image extraction from the document
    disable_image_captions: bool = (
        False  # Disable synthetic image captions/descriptions
    )
    webhook_url: Optional[str] = None
    disable_html_prettify: bool = False  # Return compact HTML
    exact_transcription: bool = False  # Transcribe text character for character
    processing_location: Optional[str] = None  # residency region, e.g. "eu"

    @property
    def processor_id(self) -> str:
        """Alias for pipeline_id (the API wire format uses pipeline_id)"""
        return self.pipeline_id

    @processor_id.setter
    def processor_id(self, value: str):
        self.pipeline_id = value


# Backward-compatible alias
CustomPipelineOptions = CustomProcessorOptions


@dataclass
class TrackChangesOptions(ProcessingOptions):
    """Options for the /track-changes endpoint (DOCX or PDF input)"""

    output_format: str = "markdown,html,chunks"  # comma-separated
    paginate: bool = False
    webhook_url: Optional[str] = None
    processing_location: Optional[str] = None  # residency region, e.g. "eu"


@dataclass
class OCROptions(ProcessingOptions):
    """Options for the deprecated /ocr endpoint"""

    processing_location: Optional[str] = None  # residency region, e.g. "eu"


@dataclass
class FormFillingOptions(ProcessingOptions):
    """Options for form filling via the /fill endpoint.

    The /fill endpoint does not accept max_pages. Use page_range instead.
    """

    field_data: Dict[str, Dict[str, str]] = field(default_factory=dict)
    context: Optional[str] = None  # Optional context to guide form filling
    confidence_threshold: float = 0.5  # Minimum confidence for field matching (0.0-1.0)
    processing_location: Optional[str] = None  # residency region, e.g. "eu"

    def to_form_data(self) -> Dict[str, Any]:
        """Convert to form data format for API requests"""
        if self.max_pages is not None:
            warnings.warn(
                "The /fill endpoint ignores max_pages. Use page_range instead.",
                UserWarning,
                stacklevel=3,
            )

        # Start with parent's form data
        form_data = super().to_form_data()
        form_data.pop("max_pages", None)

        # field_data must be JSON string
        form_data["field_data"] = (None, json.dumps(self.field_data))

        # Add context if provided
        if self.context is not None:
            form_data["context"] = (None, self.context)

        # Add confidence_threshold
        form_data["confidence_threshold"] = (None, str(self.confidence_threshold))

        return form_data


@dataclass
class FormFieldsOptions(ProcessingOptions):
    """Options for finding fillable fields via the /form-fields endpoint"""

    webhook_url: Optional[str] = None
    processing_location: Optional[str] = None  # residency region, e.g. "eu"


@dataclass
class AgentOptions:
    """Open-beta options for running a published agent processor via /agent.

    `processor` is required. It is a processor ID such as "cp_abc123", or an
    ID pinned to a version such as "cp_abc123@2.0". The SDK sends an option
    only when its value is not None, so the API defaults apply otherwise.
    """

    processor: str = ""
    # Verification effort before the run may finish: fast, balanced, accurate.
    # The API default is "fast".
    verification_effort: Optional[str] = None
    # Return the images of the run inline as base64.
    return_images: bool = False
    # "balanced" runs to the turn limit of the task. "fast" caps each agent
    # pass for lower cost. The API default is "balanced".
    mode: Optional[str] = None
    skip_cache: bool = False
    webhook_url: Optional[str] = None
    processing_location: Optional[str] = None  # residency region, e.g. "eu"

    def to_form_data(self) -> Dict[str, Any]:
        """Convert to form data format for API requests"""
        return ProcessingOptions.to_form_data(self)


@dataclass
class ConversionResult:
    """Result from document conversion"""

    success: bool
    output_format: str
    markdown: Optional[str] = None
    html: Optional[str] = None
    json: Optional[Dict[str, Any]] = None
    chunks: Optional[Dict[str, Any]] = None
    extraction_schema_json: Optional[str] = None
    segmentation_results: Optional[Dict[str, Any]] = None
    images: Optional[Dict[str, str]] = None
    metadata: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    error_in: Optional[Literal["VALIDATION", "INFERENCE", "OTHER"]] = None
    page_count: Optional[int] = None
    status: str = "complete"
    checkpoint_id: Optional[str] = None
    versions: Optional[Union[Dict[str, Any], str]] = None
    parse_quality_score: Optional[float] = None
    runtime: Optional[float] = None
    cost_breakdown: Optional[Dict[str, Any]] = None
    request_id: Optional[str] = None
    # Signed URL of the full result. The SDK downloads it for you.
    result_url: Optional[str] = None
    expires_in: Optional[int] = None
    # Extraction confidence scoring (0-5 average) and its status:
    # "complete", "partial" or "failed".
    extraction_score_average: Optional[float] = None
    extraction_scoring_status: Optional[str] = None
    extraction_mode: Optional[str] = None
    segmentation_schema: Optional[str] = None
    # True when the run reused a checkpoint of the same file.
    checkpoint_reused: Optional[bool] = None

    def save_output(
        self, output_path: Union[str, Path], save_images: bool = True
    ) -> None:
        """Save the conversion output to files"""
        output_path = Path(output_path)

        # Save main content
        if self.markdown:
            with open(output_path.with_suffix(".md"), "w", encoding="utf-8") as f:
                f.write(self.markdown)

        if self.html:
            with open(output_path.with_suffix(".html"), "w", encoding="utf-8") as f:
                f.write(self.html)

        if self.json:
            with open(output_path.with_suffix(".json"), "w", encoding="utf-8") as f:
                json.dump(self.json, f, indent=2)

        if self.chunks:
            with open(
                output_path.with_suffix(".chunks.json"), "w", encoding="utf-8"
            ) as f:
                json.dump(self.chunks, f, indent=2)

        if self.extraction_schema_json:
            with open(
                output_path.with_name(f"{output_path.stem}_extraction_results.json"),
                "w",
                encoding="utf-8",
            ) as f:
                f.write(self.extraction_schema_json)

        # Save images if present
        if save_images and self.images:
            images_dir = output_path.parent
            images_dir.mkdir(exist_ok=True)

            for filename, base64_data in self.images.items():
                image_path = images_dir / filename
                with open(image_path, "wb") as f:
                    f.write(base64.b64decode(base64_data))

        # Save metadata if present
        if self.metadata:
            with open(
                output_path.with_suffix(".metadata.json"), "w", encoding="utf-8"
            ) as f:
                json.dump(self.metadata, f, indent=2)


@dataclass
class WorkflowStep:
    """Configuration for a single workflow step"""

    unique_name: str
    settings: Dict[str, Any]
    step_key: Optional[str] = ""
    depends_on: List[str] = field(default_factory=list)
    # Additional fields returned by API
    id: Optional[int] = None
    version: Optional[str] = None
    name: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API requests"""
        data = {
            "step_key": self.step_key,
            "unique_name": self.unique_name,
            "settings": self.settings,
            "depends_on": self.depends_on,
        }
        # Include optional fields if present
        if self.id is not None:
            data["id"] = self.id
        if self.version:
            data["version"] = self.version
        if self.name:
            data["name"] = self.name
        return data


@dataclass
class Workflow:
    """Represents a workflow configuration"""

    name: str
    team_id: int
    steps: List[WorkflowStep]
    id: Optional[int] = None
    created: Optional[str] = None
    updated: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API requests"""
        data = {
            "name": self.name,
            "team_id": self.team_id,
            "steps": [step.to_dict() for step in self.steps],
        }
        if self.id is not None:
            data["id"] = self.id
        return data


@dataclass
class InputConfig:
    """
    Configuration for workflow input

    Supports three formats:
    1. Direct file URLs: InputConfig(file_urls=["https://..."])
    2. Bucket enumeration: InputConfig(bucket="my-bucket", prefix="path/", pattern="*.pdf")
    3. Explicit storage type: InputConfig(bucket="my-bucket", storage_type="s3")
    """

    file_urls: Optional[list[str]] = None
    bucket: Optional[str] = None
    prefix: Optional[str] = None
    pattern: Optional[str] = None
    storage_type: Optional[str] = None  # "s3" or "r2"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API requests"""
        data = {}
        if self.file_urls:
            data["file_urls"] = self.file_urls
        if self.bucket:
            data["bucket"] = self.bucket
        if self.prefix:
            data["prefix"] = self.prefix
        if self.pattern:
            data["pattern"] = self.pattern
        if self.storage_type:
            data["storage_type"] = self.storage_type
        return data


@dataclass
class WorkflowExecution:
    """Result from workflow execution"""

    id: int
    workflow_id: int
    status: str  # "IN_PROGRESS", "COMPLETED", "FAILED"
    input_config: Dict[str, Any]
    success: bool = True
    steps: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created: Optional[str] = None
    updated: Optional[str] = None

    def save_output(self, output_path: Union[str, Path]) -> None:
        """Save the execution steps to a JSON file"""
        output_path = Path(output_path)

        output_data = {
            "id": self.id,
            "workflow_id": self.workflow_id,
            "status": self.status,
            "success": self.success,
            "input_config": self.input_config,
            "steps": self.steps,
            "error": self.error,
            "created": self.created,
            "updated": self.updated,
        }

        with open(output_path.with_suffix(".json"), "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2)


@dataclass
class UploadedFileMetadata:
    """Metadata for an uploaded file"""

    file_id: int
    original_filename: str
    content_type: str
    reference: str
    upload_status: str  # "pending", "completed", "failed"
    file_size: Optional[int] = None
    created: Optional[str] = None
    error: Optional[str] = None


@dataclass
class OCRResult:
    """Result from OCR processing"""

    success: bool
    pages: List[Dict[str, Any]]
    error: Optional[str] = None
    page_count: Optional[int] = None
    status: str = "complete"
    versions: Optional[Union[Dict[str, Any], str]] = None
    cost_breakdown: Optional[Dict[str, Any]] = None
    request_id: Optional[str] = None

    def get_text(self, page_num: Optional[int] = None) -> str:
        """Extract text from OCR results"""
        if page_num is not None:
            # Get text from specific page
            page = next((p for p in self.pages if p.get("page") == page_num), None)
            if page:
                return "\n".join([line["text"] for line in page.get("text_lines", [])])
            return ""
        else:
            # Get all text
            all_text = []
            for page in self.pages:
                page_text = "\n".join(
                    [line["text"] for line in page.get("text_lines", [])]
                )
                all_text.append(page_text)
            return "\n\n".join(all_text)

    def save_output(self, output_path: Union[str, Path]) -> None:
        """Save the OCR output to a text file"""
        output_path = Path(output_path)

        # Save as text file
        with open(output_path.with_suffix(".txt"), "w", encoding="utf-8") as f:
            json.dump(self.pages, f, indent=2)

        # Save detailed OCR data as JSON
        with open(output_path.with_suffix(".ocr.json"), "w", encoding="utf-8") as f:
            json.dump(
                {
                    "success": self.success,
                    "pages": self.pages,
                    "error": self.error,
                    "page_count": self.page_count,
                    "status": self.status,
                },
                f,
                indent=2,
            )


@dataclass
class FormFillingResult:
    """Result from form filling"""

    status: str
    success: Optional[bool] = None
    error: Optional[str] = None
    error_in: Optional[Literal["VALIDATION", "INFERENCE", "OTHER"]] = None
    output_format: Optional[str] = None  # "pdf" or "png"
    output_base64: Optional[str] = None  # Base64-encoded filled form
    fields_filled: Optional[List[str]] = (
        None  # List of field keys that were successfully filled
    )
    fields_not_found: Optional[List[str]] = (
        None  # List of field keys that couldn't be matched
    )
    runtime: Optional[float] = None
    page_count: Optional[int] = None
    cost_breakdown: Optional[Dict[str, Any]] = None
    versions: Optional[Union[Dict[str, Any], str]] = None
    request_id: Optional[str] = None
    result_url: Optional[str] = None
    expires_in: Optional[int] = None

    def save_output(self, output_path: Union[str, Path]) -> None:
        """Save the filled form to a file"""
        output_path = Path(output_path)

        if not self.output_base64:
            raise ValueError("No output data available to save")

        # Determine file extension based on output_format
        if self.output_format == "png":
            output_path = output_path.with_suffix(".png")
        elif self.output_format == "pdf":
            output_path = output_path.with_suffix(".pdf")
        else:
            # Default to PDF if format is unknown
            output_path = output_path.with_suffix(".pdf")

        # Decode and save base64 data
        with open(output_path, "wb") as f:
            f.write(base64.b64decode(self.output_base64))

        # Save metadata if available
        metadata = {
            "status": self.status,
            "success": self.success,
            "error": self.error,
            "output_format": self.output_format,
            "fields_filled": self.fields_filled,
            "fields_not_found": self.fields_not_found,
            "runtime": self.runtime,
            "page_count": self.page_count,
            "cost_breakdown": self.cost_breakdown,
            "versions": self.versions,
        }

        with open(
            output_path.with_suffix(".metadata.json"), "w", encoding="utf-8"
        ) as f:
            json.dump(metadata, f, indent=2)


@dataclass
class CreateDocumentResult:
    """Result from document creation (/create-document endpoint)"""

    status: str
    success: Optional[bool] = None
    error: Optional[str] = None
    output_format: Optional[str] = None  # "docx"
    output_base64: Optional[str] = None  # Base64-encoded document
    runtime: Optional[float] = None
    page_count: Optional[int] = None
    cost_breakdown: Optional[Dict[str, Any]] = None
    versions: Optional[Union[Dict[str, Any], str]] = None
    request_id: Optional[str] = None
    result_url: Optional[str] = None
    expires_in: Optional[int] = None

    def save_output(self, output_path: Union[str, Path]) -> None:
        """Save the created document to a file"""
        output_path = Path(output_path)

        if not self.output_base64:
            raise ValueError("No output data available to save")

        output_path = output_path.with_suffix(".docx")

        with open(output_path, "wb") as f:
            f.write(base64.b64decode(self.output_base64))


@dataclass
class AgentResult:
    """Result from an open-beta published agent processor run (/agent endpoint)"""

    status: str
    success: Optional[bool] = None
    error: Optional[str] = None
    output_shape: Optional[str] = None  # parse, json, wcag, jats, segment
    finished: Optional[bool] = None
    turns: Optional[int] = None
    markdown: Optional[str] = None
    html: Optional[str] = None
    json: Optional[Union[Dict[str, Any], str]] = None
    chunks: Optional[Union[Dict[str, Any], str]] = None
    extraction_schema_json: Optional[Union[Dict[str, Any], str]] = None
    segments: Optional[List[Dict[str, Any]]] = None
    metadata: Optional[Dict[str, Any]] = None
    verdict: Optional[str] = None
    jats_xml: Optional[str] = None
    accessible_pdf: Optional[str] = None  # base64 PDF for the 'wcag' shape
    images: Optional[Dict[str, str]] = None
    verifier_metrics: Optional[List[Dict[str, Any]]] = None
    page_count: Optional[int] = None
    cost_breakdown: Optional[Dict[str, Any]] = None
    parse_quality_score: Optional[float] = None
    runtime: Optional[float] = None
    versions: Optional[Union[Dict[str, Any], str]] = None
    request_id: Optional[str] = None
    result_url: Optional[str] = None
    expires_in: Optional[int] = None

    def save_output(
        self, output_path: Union[str, Path], save_images: bool = True
    ) -> None:
        """Save the agent outputs to files next to output_path"""
        output_path = Path(output_path)

        if self.markdown:
            output_path.with_suffix(".md").write_text(self.markdown, encoding="utf-8")
        if self.html:
            output_path.with_suffix(".html").write_text(self.html, encoding="utf-8")
        for value, suffix in (
            (self.json, ".json"),
            (self.chunks, ".chunks.json"),
            (self.extraction_schema_json, ".extraction.json"),
            (self.segments, ".segments.json"),
        ):
            if value:
                text = value if isinstance(value, str) else json.dumps(value, indent=2)
                output_path.with_suffix(suffix).write_text(text, encoding="utf-8")
        if self.jats_xml:
            output_path.with_suffix(".xml").write_text(self.jats_xml, encoding="utf-8")
        if self.accessible_pdf:
            output_path.with_suffix(".pdf").write_bytes(
                base64.b64decode(self.accessible_pdf)
            )
        if save_images and self.images:
            for filename, base64_data in self.images.items():
                (output_path.parent / filename).write_bytes(
                    base64.b64decode(base64_data)
                )
        if self.metadata:
            output_path.with_suffix(".metadata.json").write_text(
                json.dumps(self.metadata, indent=2), encoding="utf-8"
            )


@dataclass
class FormFieldsResult:
    """Result from fillable field detection (/form-fields endpoint)

    Each page is a dict with 'page', 'fields' and 'error' keys. Each field
    has 'field_type', 'bbox' ([x0, y0, x1, y1] as page fractions) and
    'label'.
    """

    status: str
    success: Optional[bool] = None
    error: Optional[str] = None
    pages: Optional[List[Dict[str, Any]]] = None
    page_count: Optional[int] = None
    cost_breakdown: Optional[Dict[str, Any]] = None
    runtime: Optional[float] = None
    versions: Optional[Union[Dict[str, Any], str]] = None
    request_id: Optional[str] = None
    result_url: Optional[str] = None
    expires_in: Optional[int] = None

    def save_output(self, output_path: Union[str, Path]) -> None:
        """Save the detected fields to a JSON file"""
        output_path = Path(output_path)
        output_path.with_suffix(".form_fields.json").write_text(
            json.dumps(
                {
                    "status": self.status,
                    "success": self.success,
                    "error": self.error,
                    "pages": self.pages,
                    "page_count": self.page_count,
                },
                indent=2,
            ),
            encoding="utf-8",
        )


@dataclass
class ExtractionSchemaSuggestions:
    """Result from schema generation (/marker/extraction/gen_schemas).

    `suggestions` has 'simple_schema', 'moderate_schema' and
    'complex_schema' keys. Each value is a JSON schema string.
    """

    status: str
    success: Optional[bool] = None
    error: Optional[str] = None
    suggestions: Optional[Dict[str, Optional[str]]] = None
    page_count: Optional[int] = None
    request_id: Optional[str] = None


@dataclass
class FileResult:
    """Lightweight result returned when stream_response_to is used.
    The full JSON response is written directly to disk at output_path,
    never fully loaded into memory."""

    success: bool
    status: str
    output_path: Path
    error: Optional[str] = None


# --- Extraction Schema models ---


@dataclass
class ExtractionSchema:
    """Represents a saved extraction schema for structured extraction"""

    schema_id: str
    name: str
    schema_json: Dict[str, Any]
    id: Optional[int] = None
    description: Optional[str] = None
    version: int = 1
    version_history: Optional[List[Dict[str, Any]]] = None
    archived: bool = False
    created: Optional[str] = None
    updated: Optional[str] = None


# --- Pipeline models ---


@dataclass
class PipelineProcessor:
    """Configuration for a single processor within a pipeline"""

    type: str  # convert, extract, segment, custom, fill
    settings: Dict[str, Any] = field(default_factory=dict)
    custom_processor_id: Optional[str] = None  # For custom steps (cp_XXXXX)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API requests"""
        data: Dict[str, Any] = {
            "type": self.type,
            "settings": self.settings,
        }
        if self.custom_processor_id is not None:
            data["custom_processor_id"] = self.custom_processor_id
        return data


@dataclass
class PipelineConfig:
    """Represents a pipeline definition"""

    pipeline_id: str  # pl_XXXXX
    steps: List[Dict[str, Any]]
    name: Optional[str] = None
    is_saved: bool = False
    archived: bool = False
    active_version: int = 0
    id: Optional[int] = None
    created: Optional[str] = None
    updated: Optional[str] = None
    # True when create_pipeline(reuse_draft=True) reused an existing draft.
    reused: bool = False
    # True when the draft steps differ from the active published version.
    draft_differs: bool = False


@dataclass
class PipelineVersion:
    """Immutable version snapshot of a pipeline"""

    version: int
    steps: List[Dict[str, Any]]
    description: Optional[str] = None
    id: Optional[int] = None
    created: Optional[str] = None


@dataclass
class PipelineExecutionStepResult:
    """Status of a single step within a pipeline execution"""

    step_index: int
    step_type: str
    status: str  # pending, dispatched, running, completed, failed
    lookup_key: Optional[str] = None
    result_url: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    error_message: Optional[str] = None
    checkpoint_id: Optional[str] = None
    source_step_type: Optional[str] = None  # for eval steps: the step it scores
    checkpoint_reused: Optional[bool] = None


@dataclass
class PipelineExecution:
    """Result from pipeline execution"""

    execution_id: str  # pex_XXXXX
    pipeline_id: str  # pl_XXXXX
    pipeline_version: int
    status: str  # pending, running, completed, completed_with_errors, failed
    steps: List[PipelineExecutionStepResult] = field(default_factory=list)
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    created: Optional[str] = None
    config_snapshot: Optional[Dict[str, Any]] = None
    input_config: Optional[Dict[str, Any]] = None
    rate_breakdown: Optional[Dict[str, Any]] = None


# --- Custom Processor models ---


@dataclass
class CustomProcessor:
    """Represents a custom processor (formerly custom pipeline)"""

    processor_id: str  # cp_XXXXX
    status: str  # processing, completed, failed
    name: Optional[str] = None
    success: Optional[bool] = None
    active_version: int = 0
    max_version: int = 0
    iteration_in_progress: bool = False
    pipeline_id: Optional[str] = None  # auto-created workspace pipeline (pl_XXXXX)
    created_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None
    request_description: Optional[str] = None
    is_template: bool = False


@dataclass
class CustomProcessorVersion:
    """Version information for a custom processor"""

    version: int
    request_description: str = ""
    created_at: Optional[str] = None
    runtime: Optional[float] = None
    is_active: bool = False
