"""
Datalab SDK data models
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Union
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
    force_ocr: bool = False
    format_lines: bool = False
    paginate: bool = False
    use_llm: bool = False
    strip_existing_ocr: bool = False
    disable_image_extraction: bool = False
    block_correction_prompt: Optional[str] = None
    additional_config: Optional[Dict[str, Any]] = None
    page_schema: Optional[Dict[str, Any]] = None
    output_format: str = "markdown"  # markdown, json, html, chunks
    mode: str = "fast"  # fast, balanced, accurate


@dataclass
class OCROptions(ProcessingOptions):
    pass


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
    images: Optional[Dict[str, str]] = None
    metadata: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    page_count: Optional[int] = None
    status: str = "complete"

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
            with open(output_path.with_suffix(".chunks.json"), "w", encoding="utf-8") as f:
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
    step_key: Optional[str] = '' # TODO: fix API
    depends_on: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API requests"""
        return {
            "step_key": self.step_key,
            "unique_name": self.unique_name,
            "settings": self.settings,
            "depends_on": self.depends_on,
        }


@dataclass
class Workflow:
    """Represents a workflow configuration"""

    name: str
    team_id: int
    steps: List[WorkflowStep]
    id: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

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
    """Configuration for workflow input"""

    type: str  # "single_file" or other types
    file_url: Optional[str] = None
    file_path: Optional[str] = None
    additional_config: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API requests"""
        data = {"type": self.type}
        if self.file_url:
            data["file_url"] = self.file_url
        if self.additional_config:
            data.update(self.additional_config)
        return data


@dataclass
class WorkflowExecution:
    """Result from workflow execution"""

    id: int
    workflow_id: int
    status: str  # "processing", "complete", "failed"
    input_config: Dict[str, Any]
    success: bool = True
    results: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: Optional[str] = None
    completed_at: Optional[str] = None

    def save_output(self, output_path: Union[str, Path]) -> None:
        """Save the execution results to a JSON file"""
        output_path = Path(output_path)

        output_data = {
            "id": self.id,
            "workflow_id": self.workflow_id,
            "status": self.status,
            "success": self.success,
            "input_config": self.input_config,
            "results": self.results,
            "error": self.error,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
        }

        with open(output_path.with_suffix(".json"), "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2)


@dataclass
class OCRResult:
    """Result from OCR processing"""

    success: bool
    pages: List[Dict[str, Any]]
    error: Optional[str] = None
    page_count: Optional[int] = None
    status: str = "complete"

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
