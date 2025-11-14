"""
Datalab SDK data models
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
import json
import base64
import webbrowser

from datalab_sdk.utils import generate_spreadsheet_html_viewer


@dataclass
class ProcessingOptions:
    # Common options
    max_pages: Optional[int] = None
    skip_cache: bool = False
    page_range: Optional[str] = None
    extras: Optional[str] = None

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

        # If this is a spreadsheet result, save HTML viewer instead
        if self._is_spreadsheet_result():
            self.save_html_viewer(output_path)
            return

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

    def _is_spreadsheet_result(self) -> bool:
        """Check if this result contains spreadsheet table data."""
        if not isinstance(self.json, dict):
            return False
        # Check for nested structure: json.children[].children[]
        if "children" in self.json:
            for page in self.json.get("children", []):
                if isinstance(page, dict) and "children" in page:
                    for block in page.get("children", []):
                        if isinstance(block, dict) and block.get("block_type") == "Table":
                            return True
        return False

    def _extract_blocks(self) -> List[Dict[str, Any]]:
        """Extract all blocks from the nested JSON structure."""
        blocks = []
        if isinstance(self.json, dict) and "children" in self.json:
            for page in self.json.get("children", []):
                if isinstance(page, dict) and "children" in page:
                    for block in page.get("children", []):
                        blocks.append(block)
        return blocks

    def get_tables_by_sheet(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract tables grouped by sheet name.

        Returns:
            Dictionary mapping sheet names to lists of table blocks.
            Each table block contains: id, block_type, html, and other metadata.
        """
        blocks = self._extract_blocks()
        sheets: Dict[str, List[Dict[str, Any]]] = {}

        for block in blocks:
            block_type = block.get("block_type", "")
            if block_type != "Table":
                continue

            block_id = block.get("id", "")
            sheet_name = "unknown"

            # Extract sheet name from block ID (format: /page/SheetName/Table/0)
            if "/page/" in block_id and "/Table/" in block_id:
                parts = block_id.split("/")
                if len(parts) >= 3:
                    sheet_name = parts[2]
            elif "/page/" in block_id:
                parts = block_id.split("/")
                if len(parts) >= 3:
                    sheet_name = parts[2]

            if sheet_name not in sheets:
                sheets[sheet_name] = []
            sheets[sheet_name].append(block)

        # Sort tables within each sheet by their index in the block ID
        for sheet_name in sheets:
            sheets[sheet_name].sort(
                key=lambda b: self._extract_table_index(b.get("id", ""))
            )

        return sheets

    def _extract_table_index(self, block_id: str) -> int:
        """Extract table index from block ID."""
        if "/Table/" in block_id:
            parts = block_id.split("/")
            if len(parts) >= 5:
                try:
                    return int(parts[4])
                except ValueError:
                    return 0
        return 0

    def get_sheet_names(self) -> List[str]:
        """Get list of sheet names that contain tables."""
        return sorted(self.get_tables_by_sheet().keys())

    def get_table_count(self) -> int:
        """Get total number of tables across all sheets."""
        tables_by_sheet = self.get_tables_by_sheet()
        return sum(len(tables) for tables in tables_by_sheet.values())

    def generate_html_viewer(self, title: str = "XLSX Tables") -> str:
        """
        Generate HTML viewer with tabs for each sheet.

        Args:
            title: Title to display in the HTML viewer

        Returns:
            HTML string with tabs per sheet
        """
        sheets = self.get_tables_by_sheet()
        return generate_spreadsheet_html_viewer(sheets, title)

    def save_html_viewer(
        self, output_path: Union[str, Path], title: Optional[str] = None
    ) -> Path:
        """
        Save HTML viewer to file.

        Args:
            output_path: Path where to save the HTML file
            title: Optional title for the HTML viewer (defaults to filename)

        Returns:
            Path to the saved HTML file
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if title is None:
            title = output_path.stem

        html_content = self.generate_html_viewer(title=title)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        return output_path

    def open_in_browser(self, html_path: Optional[Union[str, Path]] = None) -> None:
        """
        Open HTML viewer in default browser.

        Args:
            html_path: Optional path to HTML file. If not provided, creates a temporary file.
        """
        if html_path is None:
            import tempfile
            import os

            # Create temporary file
            fd, temp_path = tempfile.mkstemp(suffix=".html", prefix="datalab_tables_")
            os.close(fd)
            html_path = Path(temp_path)
            self.save_html_viewer(html_path)
            # Note: We don't delete the temp file, as browser may need it

        html_path = Path(html_path)
        if not html_path.exists():
            raise FileNotFoundError(f"HTML file not found: {html_path}")

        webbrowser.open(f"file://{html_path.absolute()}")

    def save_json(self, output_path: Union[str, Path]) -> Path:
        """
        Save raw JSON response to file.

        Args:
            output_path: Path where to save the JSON file

        Returns:
            Path to the saved JSON file
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Ensure .json extension
        if output_path.suffix != ".json":
            output_path = output_path.with_suffix(".json")

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "success": self.success,
                    "output_format": self.output_format,
                    "json": self.json,
                    "error": self.error,
                    "page_count": self.page_count,
                    "status": self.status,
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

        return output_path


@dataclass
class WorkflowStep:
    """Configuration for a single workflow step"""

    unique_name: str
    settings: Dict[str, Any]
    step_key: Optional[str] = ''
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
