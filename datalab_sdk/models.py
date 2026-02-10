"""
Datalab SDK data models
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Union, Literal
from pathlib import Path
import json
import base64


@dataclass
class ProcessingOptions:
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


@dataclass
class ConvertOptions(ProcessingOptions):
    """Options for marker conversion"""

    # Marker specific options
    paginate: bool = False
    disable_image_extraction: bool = False
    disable_image_captions: bool = False
    fence_synthetic_captions: bool = False
    additional_config: Optional[Dict[str, Any]] = None
    page_schema: Optional[Dict[str, Any]] = None
    segmentation_schema: Optional[str] = None  # JSON string for document segmentation
    save_checkpoint: bool = False
    output_format: str = "markdown"  # markdown, json, html, chunks
    mode: str = "balanced"  # fast, balanced, accurate
    keep_spreadsheet_formatting: bool = False
    webhook_url: Optional[str] = None
    # Comma-separated list of extra features: 'track_changes', 'chart_understanding',
    # 'table_row_bboxes', 'extract_links', 'infographic', 'new_block_types', 'training_mode'
    extras: Optional[str] = None
    add_block_ids: bool = False  # add block IDs to HTML output
    include_markdown_in_chunks: bool = False  # include markdown field in chunks/JSON output
    token_efficient_markdown: bool = False  # generate token-efficient markdown optimized for LLM consumption

    def to_form_data(self) -> Dict[str, Any]:
        """Convert to form data format for API requests"""
        # Start with parent's form data
        form_data = super().to_form_data()

        # Remove keep_spreadsheet_formatting from top-level (it goes in additional_config)
        form_data.pop("keep_spreadsheet_formatting", None)

        additional_config_dict = {}
        if self.additional_config:
            additional_config_dict.update(self.additional_config)
        if self.keep_spreadsheet_formatting:
            additional_config_dict["keep_spreadsheet_formatting"] = True

        if additional_config_dict:
            form_data["additional_config"] = (None, json.dumps(additional_config_dict))

        return form_data


@dataclass
class OCROptions(ProcessingOptions):
    pass


@dataclass
class FormFillingOptions(ProcessingOptions):
    """Options for form filling"""

    field_data: Dict[str, Dict[str, str]] = field(default_factory=dict)
    context: Optional[str] = None  # Optional context to guide form filling
    confidence_threshold: float = 0.5  # Minimum confidence for field matching (0.0-1.0)

    def to_form_data(self) -> Dict[str, Any]:
        """Convert to form data format for API requests"""
        # Start with parent's form data
        form_data = super().to_form_data()

        # field_data must be JSON string
        form_data["field_data"] = (None, json.dumps(self.field_data))

        # Add context if provided
        if self.context is not None:
            form_data["context"] = (None, self.context)

        # Add confidence_threshold
        form_data["confidence_threshold"] = (None, str(self.confidence_threshold))

        return form_data


@dataclass
class ConversionResult:
    """Result from document conversion (marker endpoint)"""

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
                output_path.with_suffix("_extraction_results.json"),
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
