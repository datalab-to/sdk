"""
Datalab API client - async core with sync wrapper
"""

import asyncio
import mimetypes
import os
import shutil
import tempfile
import warnings

import aiohttp
import ijson
from tenacity import (
    retry,
    retry_if_exception,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)
from pathlib import Path
from typing import Union, Optional, Dict, Any

from datalab_sdk.exceptions import (
    DatalabAPIError,
    DatalabTimeoutError,
    DatalabFileError,
)
from datalab_sdk.mimetypes import MIMETYPE_MAP
from datalab_sdk.models import (
    ConversionResult,
    CreateDocumentResult,
    FileResult,
    OCRResult,
    ProcessingOptions,
    ConvertOptions,
    ExtractOptions,
    SegmentOptions,
    CustomProcessorOptions,
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
    ExtractionSchema,
    PipelineProcessor,
    PipelineConfig,
    PipelineVersion,
    PipelineExecution,
    PipelineExecutionStepResult,
    CustomProcessor,
    CustomProcessorVersion,
)
from datalab_sdk.settings import settings


class _AsyncIterableReader:
    """Adapts an async iterable of bytes into an async file-like object for ijson."""

    def __init__(self, async_iterable):
        self._iter = async_iterable.__aiter__()

    async def read(self, n=-1):
        if n == 0:
            return b""
        try:
            return await self._iter.__anext__()
        except StopAsyncIteration:
            return b""


class AsyncDatalabClient:
    """Asynchronous client for Datalab API"""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = settings.DATALAB_HOST,
        timeout: int = 300,
    ):
        """
        Initialize the async Datalab client

        Args:
            api_key: Your Datalab API key
            base_url: Base URL for the API (default: https://www.datalab.to)
            timeout: Default timeout for requests in seconds
        """
        if api_key is None:
            api_key = settings.DATALAB_API_KEY
        if api_key is None:
            raise DatalabAPIError("You must pass in an api_key or set DATALAB_API_KEY.")

        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = None

    async def __aenter__(self):
        """Async context manager entry"""
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()

    async def _ensure_session(self):
        """Ensure aiohttp session is created"""
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers={
                    "X-Api-Key": self.api_key,
                    "User-Agent": f"datalab-python-sdk/{settings.VERSION}",
                },
            )

    async def close(self):
        """Close the aiohttp session"""
        if self._session:
            await self._session.close()
            self._session = None

    async def _make_request(
        self, method: str, endpoint: str, **kwargs
    ) -> Dict[str, Any]:
        """Make an async request to the API"""
        await self._ensure_session()

        url = endpoint
        if not endpoint.startswith("http"):
            url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:
            async with self._session.request(method, url, **kwargs) as response:
                response.raise_for_status()
                return await response.json()
        except asyncio.TimeoutError:
            raise DatalabTimeoutError(f"Request timed out after {self.timeout} seconds")
        except aiohttp.ClientResponseError as e:
            try:
                error_data = await response.json()
                # FastAPI returns errors in "detail" field, but some APIs use "error"
                error_message = (
                    error_data.get("detail") or error_data.get("error") or str(e)
                )
            except Exception:
                error_message = str(e)
            raise DatalabAPIError(
                error_message,
                e.status,
                error_data if "error_data" in locals() else None,
            )
        except aiohttp.ClientError as e:
            raise DatalabAPIError(f"Request failed: {str(e)}")

    @retry(
        retry=retry_if_exception(
            lambda e: isinstance(e, DatalabAPIError)
            and getattr(e, "status_code", None) == 429
        ),
        stop=stop_after_attempt(10),
        wait=wait_exponential_jitter(initial=5, max=120),
        reraise=True,
    )
    async def _submit_with_retry(self, endpoint: str, data=None, json=None) -> Dict[str, Any]:
        """POST submission with retry for rate limits (429)"""
        kwargs = {}
        if data is not None:
            kwargs["data"] = data
        if json is not None:
            kwargs["json"] = json
        return await self._make_request("POST", endpoint, **kwargs)

    async def _poll_result(
        self,
        check_url: str,
        max_polls: int = 300,
        poll_interval: int = 1,
        stream_response_to: Optional[Path] = None,
    ) -> Union[Dict[str, Any], FileResult]:
        """Poll for result completion"""
        full_url = (
            check_url
            if check_url.startswith("http")
            else f"{self.base_url}/{check_url.lstrip('/')}"
        )

        for i in range(max_polls):
            if stream_response_to:
                result = await self._poll_get_streaming(full_url, stream_response_to)
                status, success, error = result.status, result.success, result.error
            else:
                data = await self._poll_get_with_retry(full_url)
                status = data.get("status")
                success = data.get("success", True)
                error = data.get("error")

            if status == "complete":
                return result if stream_response_to else data

            if not success and status != "processing":
                raise DatalabAPIError(
                    f"Processing failed: {error or 'Unknown error'}"
                )

            await asyncio.sleep(poll_interval)

        raise DatalabTimeoutError(
            f"Polling timed out after {max_polls * poll_interval} seconds"
        )

    @retry(
        retry=(
            retry_if_exception_type(DatalabTimeoutError)
            | retry_if_exception(
                lambda e: isinstance(e, DatalabAPIError)
                and (
                    # retry request timeout or too many requests
                    getattr(e, "status_code", None) in (408, 429)
                    or (
                        # or if there's a server error
                        getattr(e, "status_code", None) is not None
                        and getattr(e, "status_code") >= 500
                    )
                    # or datalab api error without status code (e.g., connection errors)
                    or getattr(e, "status_code", None) is None
                )
            )
        ),
        stop=stop_after_attempt(10),
        wait=wait_exponential_jitter(initial=5, max=120),
        reraise=True,
    )
    async def _poll_get_with_retry(self, url: str) -> Dict[str, Any]:
        """GET wrapper for polling with scoped retries for transient failures"""
        return await self._make_request("GET", url)

    @staticmethod
    async def _tee_stream_to_file(content, fd):
        """Async generator that writes each chunk to fd and yields it for ijson."""
        async for chunk in content.iter_any():
            os.write(fd, chunk)
            yield chunk

    @retry(
        retry=(
            retry_if_exception_type(DatalabTimeoutError)
            | retry_if_exception(
                lambda e: isinstance(e, DatalabAPIError)
                and (
                    getattr(e, "status_code", None) in (408, 429)
                    or (
                        getattr(e, "status_code", None) is not None
                        and getattr(e, "status_code") >= 500
                    )
                    or getattr(e, "status_code", None) is None
                )
            )
        ),
        stop=stop_after_attempt(10),
        wait=wait_exponential_jitter(initial=5, max=120),
        reraise=True,
    )
    async def _poll_get_streaming(self, url: str, stream_response_to: Path) -> FileResult:
        """GET with streaming to disk. Extracts status/success/error via ijson."""
        await self._ensure_session()

        fd = None
        tmp_path = None
        try:
            fd, tmp_path = tempfile.mkstemp()

            try:
                resp = await self._session.get(url)
            except asyncio.TimeoutError:
                raise DatalabTimeoutError(f"Request timed out after {self.timeout} seconds")
            except aiohttp.ClientError as e:
                raise DatalabAPIError(f"Request failed: {str(e)}")

            try:
                if resp.status >= 400:
                    body = await resp.read()
                    try:
                        import json as _json
                        error_data = _json.loads(body)
                        error_message = (
                            error_data.get("detail") or error_data.get("error") or str(resp.status)
                        )
                    except Exception:
                        error_message = f"HTTP {resp.status}"
                    raise DatalabAPIError(error_message, resp.status)

                status = None
                success = None
                error = None

                tee_reader = _AsyncIterableReader(
                    self._tee_stream_to_file(resp.content, fd)
                )
                async for prefix, event, value in ijson.parse_async(tee_reader):
                    if prefix == "status" and event == "string":
                        status = value
                    elif prefix == "success" and event == "boolean":
                        success = value
                    elif prefix == "error" and event in ("string", "null"):
                        error = value
            finally:
                resp.release()

            if status is None:
                raise DatalabAPIError("Response missing 'status' field")

            if success is None:
                success = True

            # Close fd before moving
            os.close(fd)
            fd = None

            if status == "complete":
                shutil.move(tmp_path, stream_response_to)
                tmp_path = None
                return FileResult(
                    success=success,
                    status=status,
                    output_path=stream_response_to,
                    error=error,
                )
            else:
                return FileResult(
                    success=success,
                    status=status,
                    output_path=stream_response_to,
                    error=error,
                )
        finally:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
            if tmp_path is not None:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

    def _prepare_file_data(self, file_path: Union[str, Path]) -> tuple:
        """Prepare file data for upload"""
        file_path = Path(file_path)

        if not file_path.exists():
            raise DatalabFileError(f"File not found: {file_path}")

        # Read file content
        file_data = file_path.read_bytes()

        # Check if file is empty
        if not file_data:
            raise DatalabFileError(
                f"File is empty: {file_path}. Please provide a file with content."
            )

        # Determine MIME type
        mime_type, _ = mimetypes.guess_type(str(file_path))
        if not mime_type:
            # Try to detect from extension
            extension = file_path.suffix.lower()
            mime_type = MIMETYPE_MAP.get(extension, "application/octet-stream")

        return file_path.name, file_data, mime_type

    def get_form_params(self, file_path=None, file_url=None, options=None, require_file=True):
        form_data = aiohttp.FormData()

        if file_url and file_path:
            raise ValueError("Either file_path or file_url must be provided, not both.")

        # Use either file_url or file upload, not both
        if file_url:
            form_data.add_field("file_url", file_url)
        elif file_path:
            filename, file_data, mime_type = self._prepare_file_data(file_path)
            form_data.add_field(
                "file", file_data, filename=filename, content_type=mime_type
            )
        elif require_file:
            raise ValueError("Either file_path or file_url must be provided")

        if options:
            for key, value in options.to_form_data().items():
                if isinstance(value, tuple):
                    form_data.add_field(key, str(value[1]))
                else:
                    form_data.add_field(key, str(value))

        return form_data

    def _build_conversion_result(self, result_data: Dict[str, Any], default_format: str = "markdown") -> ConversionResult:
        """Build a ConversionResult from API response data"""
        return ConversionResult(
            success=result_data.get("success", False),
            output_format=result_data.get("output_format", default_format),
            markdown=result_data.get("markdown"),
            html=result_data.get("html"),
            json=result_data.get("json"),
            chunks=result_data.get("chunks"),
            extraction_schema_json=result_data.get("extraction_schema_json"),
            segmentation_results=result_data.get("segmentation_results"),
            images=result_data.get("images"),
            metadata=result_data.get("metadata"),
            error=result_data.get("error"),
            error_in=result_data.get("error_in"),
            page_count=result_data.get("page_count"),
            status=result_data.get("status", "complete"),
            checkpoint_id=result_data.get("checkpoint_id"),
            versions=result_data.get("versions"),
            parse_quality_score=result_data.get("parse_quality_score"),
            runtime=result_data.get("runtime"),
            cost_breakdown=result_data.get("cost_breakdown"),
            evaluation=result_data.get("evaluation"),
        )

    async def _submit_and_poll(
        self,
        endpoint: str,
        data: aiohttp.FormData,
        max_polls: int = 300,
        poll_interval: int = 1,
        stream_response_to: Optional[Path] = None,
    ) -> Union[Dict[str, Any], FileResult]:
        """Submit a request and poll for the result"""
        initial_data = await self._submit_with_retry(endpoint, data=data)

        if not initial_data.get("success"):
            raise DatalabAPIError(
                f"Request failed: {initial_data.get('error', 'Unknown error')}"
            )

        return await self._poll_result(
            initial_data["request_check_url"],
            max_polls=max_polls,
            poll_interval=poll_interval,
            stream_response_to=stream_response_to,
        )

    # Convenient endpoint-specific methods
    async def convert(
        self,
        file_path: Optional[Union[str, Path]] = None,
        file_url: Optional[str] = None,
        options: Optional[ConvertOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        stream_response_to: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> Union[ConversionResult, FileResult]:
        """
        Convert a document to markdown, HTML, JSON, or chunks

        Args:
            file_path: Path to the file to convert
            file_url: URL of the file to convert
            options: Processing options for conversion
            save_output: Optional path to save output files
            stream_response_to: Optional path to stream raw JSON response to disk
            max_polls: Maximum number of polling attempts
            poll_interval: Seconds between polling attempts
        """
        if save_output and stream_response_to:
            raise ValueError("Cannot use both 'save_output' and 'stream_response_to'.")

        resolved_stream_response_to = None
        if stream_response_to:
            resolved_stream_response_to = Path(stream_response_to)
            if not resolved_stream_response_to.parent.is_dir():
                raise ValueError(f"Directory does not exist: {resolved_stream_response_to.parent}")

        if options is None:
            options = ConvertOptions()

        result_data = await self._submit_and_poll(
            "/api/v1/convert",
            data=self.get_form_params(
                file_path=file_path, file_url=file_url, options=options
            ),
            max_polls=max_polls,
            poll_interval=poll_interval,
            stream_response_to=resolved_stream_response_to,
        )

        if isinstance(result_data, FileResult):
            return result_data

        result = self._build_conversion_result(result_data, options.output_format)

        # Save output if requested
        if save_output and result.success:
            output_path = Path(save_output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            result.save_output(output_path)

        return result

    async def extract(
        self,
        file_path: Optional[Union[str, Path]] = None,
        file_url: Optional[str] = None,
        options: Optional[ExtractOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        stream_response_to: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> Union[ConversionResult, FileResult]:
        """
        Extract structured data from a document using a JSON schema or saved extraction schema

        Provide a page_schema for inline extraction, or a schema_id to use a saved
        extraction schema. These are mutually exclusive.

        Args:
            file_path: Path to the file to extract from
            file_url: URL of the file to extract from
            options: Extraction options (must include page_schema or schema_id)
            save_output: Optional path to save output files
            stream_response_to: Optional path to stream raw JSON response to disk
            max_polls: Maximum number of polling attempts
            poll_interval: Seconds between polling attempts
        """
        if save_output and stream_response_to:
            raise ValueError("Cannot use both 'save_output' and 'stream_response_to'.")

        resolved_stream_response_to = None
        if stream_response_to:
            resolved_stream_response_to = Path(stream_response_to)
            if not resolved_stream_response_to.parent.is_dir():
                raise ValueError(f"Directory does not exist: {resolved_stream_response_to.parent}")

        if options is None:
            raise ValueError("options must be provided with page_schema or schema_id")

        has_page_schema = bool(options.page_schema)
        has_schema_id = bool(options.schema_id)

        if has_page_schema and has_schema_id:
            raise ValueError("page_schema and schema_id are mutually exclusive. Provide one or the other.")
        if not has_page_schema and not has_schema_id:
            raise ValueError("Either page_schema or schema_id must be provided in options.")
        if options.schema_version is not None and not has_schema_id:
            raise ValueError("schema_version can only be used with schema_id.")

        has_file = file_path is not None or file_url is not None
        has_checkpoint = options.checkpoint_id is not None

        if not has_file and not has_checkpoint:
            raise ValueError("Either file_path/file_url or options.checkpoint_id must be provided")
        if has_file and has_checkpoint:
            raise ValueError("Provide either file_path/file_url or checkpoint_id, not both")

        result_data = await self._submit_and_poll(
            "/api/v1/extract",
            data=self.get_form_params(
                file_path=file_path, file_url=file_url, options=options, require_file=False
            ),
            max_polls=max_polls,
            poll_interval=poll_interval,
            stream_response_to=resolved_stream_response_to,
        )

        if isinstance(result_data, FileResult):
            return result_data

        result = self._build_conversion_result(result_data, options.output_format)

        if save_output and result.success:
            output_path = Path(save_output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            result.save_output(output_path)

        return result

    async def segment(
        self,
        file_path: Optional[Union[str, Path]] = None,
        file_url: Optional[str] = None,
        options: Optional[SegmentOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        stream_response_to: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> Union[ConversionResult, FileResult]:
        """
        Segment a document into sections using a schema

        Returns page ranges for each identified segment. Provide a file for
        end-to-end processing, or set checkpoint_id in options to skip re-parsing.

        Args:
            file_path: Path to the file to segment
            file_url: URL of the file to segment
            options: Segmentation options (must include segmentation_schema)
            save_output: Optional path to save output files
            stream_response_to: Optional path to stream raw JSON response to disk
            max_polls: Maximum number of polling attempts
            poll_interval: Seconds between polling attempts
        """
        if save_output and stream_response_to:
            raise ValueError("Cannot use both 'save_output' and 'stream_response_to'.")

        resolved_stream_response_to = None
        if stream_response_to:
            resolved_stream_response_to = Path(stream_response_to)
            if not resolved_stream_response_to.parent.is_dir():
                raise ValueError(f"Directory does not exist: {resolved_stream_response_to.parent}")

        if options is None:
            raise ValueError("options must be provided with segmentation_schema")

        has_file = file_path is not None or file_url is not None
        has_checkpoint = options.checkpoint_id is not None

        if not has_file and not has_checkpoint:
            raise ValueError("Either file_path/file_url or options.checkpoint_id must be provided")
        if has_file and has_checkpoint:
            raise ValueError("Provide either file_path/file_url or checkpoint_id, not both")

        result_data = await self._submit_and_poll(
            "/api/v1/segment",
            data=self.get_form_params(
                file_path=file_path, file_url=file_url, options=options, require_file=False
            ),
            max_polls=max_polls,
            poll_interval=poll_interval,
            stream_response_to=resolved_stream_response_to,
        )

        if isinstance(result_data, FileResult):
            return result_data

        result = self._build_conversion_result(result_data, "markdown")

        if save_output and result.success:
            output_path = Path(save_output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            result.save_output(output_path)

        return result

    async def run_custom_processor(
        self,
        file_path: Optional[Union[str, Path]] = None,
        file_url: Optional[str] = None,
        options: Optional[CustomProcessorOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        stream_response_to: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> Union[ConversionResult, FileResult]:
        """
        Execute a custom processor on a document

        Args:
            file_path: Path to the file to process
            file_url: URL of the file to process
            options: Custom processor options (must include pipeline_id)
            save_output: Optional path to save output files
            stream_response_to: Optional path to stream raw JSON response to disk
            max_polls: Maximum number of polling attempts
            poll_interval: Seconds between polling attempts
        """
        if save_output and stream_response_to:
            raise ValueError("Cannot use both 'save_output' and 'stream_response_to'.")

        resolved_stream_response_to = None
        if stream_response_to:
            resolved_stream_response_to = Path(stream_response_to)
            if not resolved_stream_response_to.parent.is_dir():
                raise ValueError(f"Directory does not exist: {resolved_stream_response_to.parent}")

        if options is None:
            raise ValueError("options must be provided with pipeline_id")

        result_data = await self._submit_and_poll(
            "/api/v1/custom-processor",
            data=self.get_form_params(
                file_path=file_path, file_url=file_url, options=options
            ),
            max_polls=max_polls,
            poll_interval=poll_interval,
            stream_response_to=resolved_stream_response_to,
        )

        if isinstance(result_data, FileResult):
            return result_data

        result = self._build_conversion_result(result_data, options.output_format)

        if save_output and result.success:
            output_path = Path(save_output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            result.save_output(output_path)

        return result

    async def run_custom_pipeline(
        self,
        file_path: Optional[Union[str, Path]] = None,
        file_url: Optional[str] = None,
        options: Optional[CustomProcessorOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        stream_response_to: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> Union[ConversionResult, FileResult]:
        """Execute a custom processor on a document

        .. deprecated::
            Use run_custom_processor() instead.
        """
        warnings.warn(
            "run_custom_pipeline() is deprecated. Use run_custom_processor() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return await self.run_custom_processor(
            file_path=file_path,
            file_url=file_url,
            options=options,
            save_output=save_output,
            stream_response_to=stream_response_to,
            max_polls=max_polls,
            poll_interval=poll_interval,
        )

    async def track_changes(
        self,
        file_path: Optional[Union[str, Path]] = None,
        file_url: Optional[str] = None,
        options: Optional[TrackChangesOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        stream_response_to: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> Union[ConversionResult, FileResult]:
        """
        Extract and display tracked changes from DOCX documents

        Args:
            file_path: Path to the DOCX file
            file_url: URL of the DOCX file
            options: Track changes options
            save_output: Optional path to save output files
            stream_response_to: Optional path to stream raw JSON response to disk
            max_polls: Maximum number of polling attempts
            poll_interval: Seconds between polling attempts
        """
        if save_output and stream_response_to:
            raise ValueError("Cannot use both 'save_output' and 'stream_response_to'.")

        resolved_stream_response_to = None
        if stream_response_to:
            resolved_stream_response_to = Path(stream_response_to)
            if not resolved_stream_response_to.parent.is_dir():
                raise ValueError(f"Directory does not exist: {resolved_stream_response_to.parent}")

        if options is None:
            options = TrackChangesOptions()

        result_data = await self._submit_and_poll(
            "/api/v1/track-changes",
            data=self.get_form_params(
                file_path=file_path, file_url=file_url, options=options
            ),
            max_polls=max_polls,
            poll_interval=poll_interval,
            stream_response_to=resolved_stream_response_to,
        )

        if isinstance(result_data, FileResult):
            return result_data

        result = self._build_conversion_result(result_data, options.output_format)

        if save_output and result.success:
            output_path = Path(save_output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            result.save_output(output_path)

        return result

    async def create_document(
        self,
        markdown: str,
        output_format: str = "docx",
        webhook_url: Optional[str] = None,
        save_output: Optional[Union[str, Path]] = None,
        stream_response_to: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> Union[CreateDocumentResult, FileResult]:
        """
        Create a DOCX document from markdown with track changes support

        The input markdown can contain track changes markup:
        - <ins> tags for insertions
        - <del> tags for deletions
        - <comment> tags for comments

        Args:
            markdown: The markdown content to convert to a document
            output_format: Output format (currently only 'docx')
            webhook_url: Optional webhook URL for completion notification
            save_output: Optional path to save the output file
            stream_response_to: Optional path to stream raw JSON response to disk
            max_polls: Maximum number of polling attempts
            poll_interval: Seconds between polling attempts
        """
        if save_output and stream_response_to:
            raise ValueError("Cannot use both 'save_output' and 'stream_response_to'.")

        resolved_stream_response_to = None
        if stream_response_to:
            resolved_stream_response_to = Path(stream_response_to)
            if not resolved_stream_response_to.parent.is_dir():
                raise ValueError(f"Directory does not exist: {resolved_stream_response_to.parent}")

        payload = {
            "markdown": markdown,
            "output_format": output_format,
        }
        if webhook_url:
            payload["webhook_url"] = webhook_url

        initial_data = await self._submit_with_retry(
            "/api/v1/create-document",
            json=payload,
        )

        if not initial_data.get("success"):
            raise DatalabAPIError(
                f"Request failed: {initial_data.get('error', 'Unknown error')}"
            )

        result_data = await self._poll_result(
            initial_data["request_check_url"],
            max_polls=max_polls,
            poll_interval=poll_interval,
            stream_response_to=resolved_stream_response_to,
        )

        if isinstance(result_data, FileResult):
            return result_data

        result = CreateDocumentResult(
            status=result_data.get("status", "complete"),
            success=result_data.get("success"),
            error=result_data.get("error"),
            output_format=result_data.get("output_format"),
            output_base64=result_data.get("output_base64"),
            runtime=result_data.get("runtime"),
            page_count=result_data.get("page_count"),
            cost_breakdown=result_data.get("cost_breakdown"),
            versions=result_data.get("versions"),
        )

        if save_output and result.success and result.output_base64:
            output_path = Path(save_output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            result.save_output(output_path)

        return result

    async def ocr(
        self,
        file_path: Union[str, Path],
        options: Optional[ProcessingOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        stream_response_to: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> Union[OCRResult, FileResult]:
        """Perform OCR on a document

        .. deprecated::
            The /ocr endpoint is deprecated. Use convert() instead.
        """
        if save_output and stream_response_to:
            raise ValueError("Cannot use both 'save_output' and 'stream_response_to'.")

        resolved_stream_response_to = None
        if stream_response_to:
            resolved_stream_response_to = Path(stream_response_to)
            if not resolved_stream_response_to.parent.is_dir():
                raise ValueError(f"Directory does not exist: {resolved_stream_response_to.parent}")

        warnings.warn(
            "The ocr() method is deprecated and will be removed in a future version. "
            "Use convert() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if options is None:
            options = OCROptions()

        initial_data = await self._submit_with_retry(
            "/api/v1/ocr",
            data=self.get_form_params(file_path=file_path, options=options),
        )

        if not initial_data.get("success"):
            raise DatalabAPIError(
                f"Request failed: {initial_data.get('error', 'Unknown error')}"
            )

        result_data = await self._poll_result(
            initial_data["request_check_url"],
            max_polls=max_polls,
            poll_interval=poll_interval,
            stream_response_to=resolved_stream_response_to,
        )

        if isinstance(result_data, FileResult):
            return result_data

        result = OCRResult(
            success=result_data.get("success", False),
            pages=result_data.get("pages", []),
            error=result_data.get("error"),
            page_count=result_data.get("page_count"),
            status=result_data.get("status", "complete"),
            versions=result_data.get("versions"),
            cost_breakdown=result_data.get("cost_breakdown"),
        )

        # Save output if requested
        if save_output and result.success:
            output_path = Path(save_output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            result.save_output(output_path)

        return result

    async def fill(
        self,
        file_path: Optional[Union[str, Path]] = None,
        file_url: Optional[str] = None,
        options: Optional[FormFillingOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        stream_response_to: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> Union[FormFillingResult, FileResult]:
        """
        Fill PDF or image forms with provided field data

        Args:
            file_path: Path to the file to fill
            file_url: URL of the file to fill
            options: Form filling options (must include field_data)
            save_output: Optional path to save output files
            stream_response_to: Optional path to stream raw JSON response to disk
            max_polls: Maximum number of polling attempts
            poll_interval: Seconds between polling attempts
        """
        if save_output and stream_response_to:
            raise ValueError("Cannot use both 'save_output' and 'stream_response_to'.")

        resolved_stream_response_to = None
        if stream_response_to:
            resolved_stream_response_to = Path(stream_response_to)
            if not resolved_stream_response_to.parent.is_dir():
                raise ValueError(f"Directory does not exist: {resolved_stream_response_to.parent}")

        if options is None:
            raise ValueError("options must be provided with field_data")

        initial_data = await self._submit_with_retry(
            "/api/v1/fill",
            data=self.get_form_params(
                file_path=file_path, file_url=file_url, options=options
            ),
        )

        if not initial_data.get("success"):
            raise DatalabAPIError(
                f"Request failed: {initial_data.get('error', 'Unknown error')}"
            )

        result_data = await self._poll_result(
            initial_data["request_check_url"],
            max_polls=max_polls,
            poll_interval=poll_interval,
            stream_response_to=resolved_stream_response_to,
        )

        if isinstance(result_data, FileResult):
            return result_data

        result = FormFillingResult(
            status=result_data.get("status", "complete"),
            success=result_data.get("success"),
            error=result_data.get("error"),
            error_in=result_data.get("error_in"),
            output_format=result_data.get("output_format"),
            output_base64=result_data.get("output_base64"),
            fields_filled=result_data.get("fields_filled"),
            fields_not_found=result_data.get("fields_not_found"),
            runtime=result_data.get("runtime"),
            page_count=result_data.get("page_count"),
            cost_breakdown=result_data.get("cost_breakdown"),
            versions=result_data.get("versions"),
        )

        # Save output if requested
        if save_output and result.success and result.output_base64:
            output_path = Path(save_output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            result.save_output(output_path)

        return result

    # Workflow methods
    async def create_workflow(
        self,
        name: str,
        steps: list[WorkflowStep],
    ) -> Workflow:
        """
        Create a new workflow

        Args:
            name: Name of the workflow
            steps: List of workflow steps

        Returns:
            Workflow object with ID and metadata
        """
        workflow_data = {
            "name": name,
            "steps": [step.to_dict() for step in steps],
        }

        response = await self._make_request(
            "POST",
            "/api/v1/workflows/workflows",
            json=workflow_data,
        )

        # Parse response into Workflow object
        workflow_steps = [
            WorkflowStep(
                unique_name=step["unique_name"],
                settings=step["settings"],
                depends_on=step.get("depends_on", []),
                id=step.get("id"),
            )
            for step in response.get("steps", [])
        ]

        return Workflow(
            id=response.get("id"),
            name=response["name"],
            team_id=response["team_id"],
            steps=workflow_steps,
            created=response.get("created"),
            updated=response.get("updated"),
        )

    async def get_workflow(self, workflow_id: int) -> Workflow:
        """
        Get a workflow by ID

        Args:
            workflow_id: ID of the workflow to retrieve

        Returns:
            Workflow object
        """
        response = await self._make_request(
            "GET",
            f"/api/v1/workflows/workflows/{workflow_id}",
        )

        workflow_steps = [
            WorkflowStep(
                step_key=step["step_key"],
                unique_name=step["unique_name"],
                settings=step["settings"],
                depends_on=step.get("depends_on", []),
                id=step.get("id"),
                version=step.get("version"),
                name=step.get("name"),
            )
            for step in response.get("steps", [])
        ]

        return Workflow(
            id=response.get("id"),
            name=response["name"],
            team_id=response["team_id"],
            steps=workflow_steps,
            created=response.get("created"),
            updated=response.get("updated"),
        )

    async def get_step_types(self) -> dict:
        """
        Get all available workflow step types

        Returns:
            Dictionary containing step_types list with their schemas
        """
        response = await self._make_request(
            "GET",
            "/api/v1/workflows/step-types",
        )
        return response

    async def list_workflows(self) -> list[Workflow]:
        """
        List all workflows for the authenticated user's team

        Returns:
            List of Workflow objects
        """
        response = await self._make_request(
            "GET",
            "/api/v1/workflows/workflows",
        )

        workflows = []
        for workflow_data in response.get("workflows", []):
            workflow_steps = [
                WorkflowStep(
                    step_key=step["step_key"],
                    unique_name=step["unique_name"],
                    settings=step["settings"],
                    depends_on=step.get("depends_on", []),
                    id=step.get("id"),
                    version=step.get("version"),
                    name=step.get("name"),
                )
                for step in workflow_data.get("steps", [])
            ]

            workflows.append(
                Workflow(
                    id=workflow_data.get("id"),
                    name=workflow_data["name"],
                    team_id=workflow_data["team_id"],
                    steps=workflow_steps,
                    created=workflow_data.get("created"),
                    updated=workflow_data.get("updated"),
                )
            )

        return workflows

    async def delete_workflow(self, workflow_id: int) -> Dict[str, Any]:
        """
        Delete a workflow definition

        Args:
            workflow_id: ID of the workflow to delete

        Returns:
            Dictionary containing:
                - success: Whether the deletion was successful
                - message: Confirmation message

        Raises:
            DatalabAPIError: If workflow has executions or cannot be deleted
        """
        response = await self._make_request(
            "DELETE",
            f"/api/v1/workflows/workflows/{workflow_id}",
        )

        return {
            "success": response.get("success", True),
            "message": response.get(
                "message", f"Workflow {workflow_id} deleted successfully"
            ),
        }

    async def execute_workflow(
        self,
        workflow_id: int,
        input_config: InputConfig,
    ) -> WorkflowExecution:
        """
        Trigger a workflow execution

        Args:
            workflow_id: ID of the workflow to execute
            input_config: Input configuration for the workflow

        Returns:
            WorkflowExecution object with initial status (typically "processing")
            Use get_execution_status() to check completion status
        """
        execution_data = {
            "input_config": input_config.to_dict(),
        }

        response = await self._make_request(
            "POST",
            f"/api/v1/workflows/workflows/{workflow_id}/execute",
            json=execution_data,
        )

        execution_id = response.get("execution_id") or response.get("id")

        if not execution_id:
            raise DatalabAPIError("No execution ID returned from API")

        # Return initial execution status without polling
        return WorkflowExecution(
            id=execution_id,
            workflow_id=workflow_id,
            status=response.get("status", "processing"),
            input_config=input_config.to_dict(),
            success=response.get("success", True),
            steps=response.get("results"),
            error=response.get("error"),
            created=response.get("created"),
            updated=response.get("updated"),
        )

    async def get_execution_status(
        self,
        execution_id: int,
        max_polls: int = 1,
        poll_interval: int = 1,
        download_results: bool = False,
    ) -> WorkflowExecution:
        """
        Get the status of a workflow execution, optionally polling until completion

        Args:
            execution_id: ID of the execution to check
            max_polls: Maximum number of polling attempts (default: 1 for single check)
            poll_interval: Seconds between polling attempts (default: 1)
            download_results: If True, download results from presigned URLs (default: False)

        Returns:
            WorkflowExecution object with current status and results.
            Results will contain presigned URLs or downloaded data depending on download_results flag.
        """
        for i in range(max_polls):
            response = await self._make_request(
                "GET",
                f"/api/v1/workflows/executions/{execution_id}",
            )

            status = response.get("status", "unknown").upper()

            # API returns step results with presigned URLs
            steps_data = response.get("steps", {})

            # Optionally download results from presigned URLs
            if download_results and steps_data and status == "COMPLETED":
                steps = await self._download_step_results(steps_data)
            else:
                # Keep the raw step data with URLs
                steps = steps_data

            # Determine success based on status
            success = status == "COMPLETED"
            error = response.get("error")

            # If any step failed, extract error from nested structure
            if status == "FAILED" or not success:
                failed_steps = []
                for step_name, step_info in steps_data.items():
                    for file_key, file_step_data in step_info.items():
                        if (
                            isinstance(file_step_data, dict)
                            and file_step_data.get("status") == "FAILED"
                        ):
                            failed_steps.append(f"{step_name}[{file_key}]")
                if failed_steps and not error:
                    error = f"Step(s) failed: {', '.join(failed_steps)}"

            execution = WorkflowExecution(
                id=response.get("execution_id") or response.get("id") or execution_id,
                workflow_id=response["workflow_id"],
                status=status,
                input_config=response.get("input_config", {}),
                success=success,
                steps=steps,
                error=error,
                created=response.get("created"),
                updated=response.get("updated"),
            )

            # If complete or failed, return immediately
            if status in ("COMPLETED", "FAILED"):
                return execution

            # Continue polling if in progress or pending
            if i < max_polls - 1:
                await asyncio.sleep(poll_interval)

        # Return the last status even if not complete (after max_polls)
        return execution

    async def _download_step_results(self, steps_data: dict) -> dict:
        """
        Download results from presigned URLs for each step

        Args:
            steps_data: Dictionary of step data with nested structure:
                       step_name -> file_id/aggregated -> step_data

        Returns:
            Dictionary with downloaded results for each step
        """
        results = {}

        for step_name, step_info in steps_data.items():
            results[step_name] = {}

            # Iterate through file_ids/aggregated keys
            for file_key, file_step_data in step_info.items():
                if isinstance(file_step_data, dict):
                    output_url = file_step_data.get("output_url")
                    if output_url:
                        try:
                            # Download from presigned URL
                            async with aiohttp.ClientSession() as session:
                                async with session.get(output_url) as resp:
                                    if resp.status == 200:
                                        content_type = resp.headers.get(
                                            "Content-Type", ""
                                        )
                                        if "json" in content_type:
                                            downloaded_data = await resp.json()
                                        else:
                                            downloaded_data = await resp.text()
                                        # Merge downloaded data with metadata
                                        results[step_name][file_key] = {
                                            **file_step_data,
                                            "downloaded_data": downloaded_data,
                                        }
                                    else:
                                        results[step_name][file_key] = {
                                            **file_step_data,
                                            "error": f"Failed to download: HTTP {resp.status}",
                                        }
                        except Exception as e:
                            results[step_name][file_key] = {
                                **file_step_data,
                                "error": f"Download failed: {str(e)}",
                            }
                    else:
                        # Keep the step info if no URL available
                        results[step_name][file_key] = file_step_data
                else:
                    results[step_name][file_key] = file_step_data

        return results

    async def _upload_single_file(
        self,
        file_path: Union[str, Path],
    ) -> UploadedFileMetadata:
        """
        Internal method to upload a single file to Datalab storage

        This method handles the complete upload flow:
        1. Request a presigned upload URL
        2. Upload the file to the presigned URL
        3. Confirm the upload with the API

        Args:
            file_path: Path to the local file to upload

        Returns:
            UploadedFileMetadata object with file information including file_id and reference
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise DatalabFileError(f"File not found: {file_path}")

        # Determine content type
        mime_type, _ = mimetypes.guess_type(str(file_path))
        if not mime_type:
            extension = file_path.suffix.lower()
            mime_type = MIMETYPE_MAP.get(extension, "application/octet-stream")

        # Step 1: Request presigned upload URL
        response = await self._make_request(
            "POST",
            "/api/v1/files/upload",
            json={
                "filename": file_path.name,
                "content_type": mime_type,
            },
        )

        file_id = response["file_id"]
        upload_url = response["upload_url"]
        reference = response["reference"]

        # Step 2: Upload file to presigned URL
        try:
            file_data = file_path.read_bytes()
            async with aiohttp.ClientSession() as session:
                async with session.put(
                    upload_url,
                    data=file_data,
                    headers={"Content-Type": mime_type},
                ) as upload_response:
                    upload_response.raise_for_status()
        except Exception as e:
            raise DatalabFileError(f"Failed to upload file to storage: {str(e)}")

        # Step 3: Confirm upload with API
        try:
            confirm_response = await self._make_request(
                "GET",
                f"/api/v1/files/{file_id}/confirm",
            )
        except Exception as e:
            raise DatalabAPIError(f"Failed to confirm file upload: {str(e)}")

        # Return file metadata
        return UploadedFileMetadata(
            file_id=file_id,
            original_filename=file_path.name,
            content_type=mime_type,
            reference=reference,
            upload_status="completed",
            file_size=file_path.stat().st_size,
            created=confirm_response.get("created"),
        )

    async def upload_files(
        self,
        file_paths: Union[str, Path, list[Union[str, Path]]],
    ) -> Union[UploadedFileMetadata, list[UploadedFileMetadata]]:
        """
        Upload one or more files to Datalab storage

        This method handles the complete upload flow for each file:
        1. Request a presigned upload URL
        2. Upload the file to the presigned URL
        3. Confirm the upload with the API

        Multiple files are uploaded concurrently for better performance.

        Args:
            file_paths: Single file path or list of file paths to upload

        Returns:
            If single file: UploadedFileMetadata object
            If multiple files: List of UploadedFileMetadata objects

        Example:
            # Upload single file
            metadata = client.upload_files("document.pdf")

            # Upload multiple files
            metadatas = client.upload_files(["doc1.pdf", "doc2.pdf"])
        """
        # Handle single file path
        if isinstance(file_paths, (str, Path)):
            return await self._upload_single_file(file_paths)

        # Handle list of file paths
        tasks = [self._upload_single_file(file_path) for file_path in file_paths]
        return await asyncio.gather(*tasks)

    async def list_files(
        self,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        List uploaded files for the authenticated user's team

        Args:
            limit: Maximum number of files to return (default: 50)
            offset: Offset for pagination (default: 0)

        Returns:
            Dictionary containing:
                - files: List of UploadedFileMetadata objects
                - total: Total number of files
                - limit: Limit used
                - offset: Offset used
        """
        response = await self._make_request(
            "GET",
            f"/api/v1/files?limit={limit}&offset={offset}",
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )

        # Parse file metadata
        files = [
            UploadedFileMetadata(
                file_id=file_data["file_id"],
                original_filename=file_data["original_filename"],
                content_type=file_data["content_type"],
                reference=file_data["reference"],
                upload_status=file_data["upload_status"],
                file_size=file_data.get("file_size"),
                created=file_data.get("created"),
            )
            for file_data in response.get("files", [])
        ]

        return {
            "files": files,
            "total": response.get("total", 0),
            "limit": response.get("limit", limit),
            "offset": response.get("offset", offset),
        }

    async def get_file_metadata(
        self,
        file_id: Union[int, str],
    ) -> UploadedFileMetadata:
        """
        Get metadata for an uploaded file

        Args:
            file_id: File ID (integer or hashid string)

        Returns:
            UploadedFileMetadata object with file information
        """
        response = await self._make_request(
            "GET",
            f"/api/v1/files/{file_id}",
        )

        return UploadedFileMetadata(
            file_id=response["file_id"],
            original_filename=response["original_filename"],
            content_type=response["content_type"],
            reference=response["reference"],
            upload_status=response["upload_status"],
            file_size=response.get("file_size"),
            created=response.get("created"),
        )

    async def get_file_download_url(
        self,
        file_id: Union[int, str],
        expires_in: int = 3600,
    ) -> Dict[str, Any]:
        """
        Generate presigned URL for downloading a file

        Args:
            file_id: File ID (integer or hashid string)
            expires_in: URL expiry time in seconds (default: 3600, max: 86400)

        Returns:
            Dictionary containing:
                - download_url: Presigned URL for downloading the file
                - expires_in: URL expiry time in seconds
                - file_id: File ID
                - original_filename: Original filename
        """
        if expires_in < 60 or expires_in > 86400:
            raise ValueError("expires_in must be between 60 and 86400 seconds")

        response = await self._make_request(
            "GET",
            f"/api/v1/files/{file_id}/download?expires_in={expires_in}",
        )

        return {
            "download_url": response["download_url"],
            "expires_in": response["expires_in"],
            "file_id": response["file_id"],
            "original_filename": response["original_filename"],
        }

    async def delete_file(
        self,
        file_id: Union[int, str],
    ) -> Dict[str, Any]:
        """
        Delete an uploaded file

        Removes the file from both storage and the database.

        Args:
            file_id: File ID (integer or hashid string)

        Returns:
            Dictionary containing:
                - success: Whether the deletion was successful
                - message: Confirmation message
        """
        response = await self._make_request(
            "DELETE",
            f"/api/v1/files/{file_id}",
        )

        return {
            "success": response.get("success", True),
            "message": response.get("message", f"File {file_id} deleted successfully"),
        }

    # --- Extraction Schema methods ---

    async def create_extraction_schema(
        self,
        name: str,
        schema_json: Dict[str, Any],
        description: Optional[str] = None,
    ) -> ExtractionSchema:
        """
        Create a new extraction schema

        Args:
            name: Name for the schema (max 200 characters)
            schema_json: JSON schema for extraction (must contain 'properties' key)
            description: Optional description
        """
        payload: Dict[str, Any] = {"name": name, "schema_json": schema_json}
        if description is not None:
            payload["description"] = description

        response = await self._make_request("POST", "/api/v1/extraction_schemas", json=payload)
        return self._build_extraction_schema(response)

    async def list_extraction_schemas(
        self,
        limit: int = 50,
        offset: int = 0,
        include_archived: bool = False,
    ) -> Dict[str, Any]:
        """
        List extraction schemas for the authenticated user's team

        Args:
            limit: Maximum number of schemas to return (default: 50, max: 200)
            offset: Offset for pagination (default: 0)
            include_archived: Include archived schemas (default: False)
        """
        params = f"limit={limit}&offset={offset}&include_archived={str(include_archived).lower()}"
        response = await self._make_request("GET", f"/api/v1/extraction_schemas?{params}")
        return {
            "schemas": [self._build_extraction_schema(s) for s in response.get("schemas", [])],
            "total": response.get("total", 0),
        }

    async def get_extraction_schema(self, schema_id: str) -> ExtractionSchema:
        """
        Get an extraction schema by its schema_id

        Args:
            schema_id: Schema ID string (e.g. sch_k8Hx9mP2nQ4v)
        """
        response = await self._make_request("GET", f"/api/v1/extraction_schemas/{schema_id}")
        return self._build_extraction_schema(response)

    async def update_extraction_schema(
        self,
        schema_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        schema_json: Optional[Dict[str, Any]] = None,
        archived: Optional[bool] = None,
        create_new_version: bool = False,
    ) -> ExtractionSchema:
        """
        Update an extraction schema

        Args:
            schema_id: Schema ID string (e.g. sch_k8Hx9mP2nQ4v)
            name: New name (max 200 characters)
            description: New description
            schema_json: New JSON schema (must contain 'properties' key)
            archived: Set archived status
            create_new_version: If True, bump version and save current state to history
        """
        payload: Dict[str, Any] = {}
        if name is not None:
            payload["name"] = name
        if description is not None:
            payload["description"] = description
        if schema_json is not None:
            payload["schema_json"] = schema_json
        if archived is not None:
            payload["archived"] = archived
        if create_new_version:
            payload["create_new_version"] = True

        response = await self._make_request("PUT", f"/api/v1/extraction_schemas/{schema_id}", json=payload)
        return self._build_extraction_schema(response)

    async def delete_extraction_schema(self, schema_id: str) -> ExtractionSchema:
        """
        Delete (archive) an extraction schema

        Args:
            schema_id: Schema ID string (e.g. sch_k8Hx9mP2nQ4v)
        """
        response = await self._make_request("DELETE", f"/api/v1/extraction_schemas/{schema_id}")
        return self._build_extraction_schema(response)

    @staticmethod
    def _build_extraction_schema(data: Dict[str, Any]) -> ExtractionSchema:
        return ExtractionSchema(
            id=data.get("id"),
            schema_id=data["schema_id"],
            name=data["name"],
            description=data.get("description"),
            schema_json=data["schema_json"],
            version=data.get("version", 1),
            version_history=data.get("version_history"),
            archived=data.get("archived", False),
            created=data.get("created"),
            updated=data.get("updated"),
        )

    # --- Pipeline CRUD methods ---

    async def create_pipeline(
        self,
        steps: list[PipelineProcessor],
    ) -> PipelineConfig:
        """
        Create a new pipeline

        Args:
            steps: Ordered list of PipelineProcessor objects
        """
        payload = {"steps": [s.to_dict() for s in steps]}
        response = await self._make_request("POST", "/api/v1/pipelines", json=payload)
        return self._build_pipeline_config(response)

    async def list_pipelines(
        self,
        saved_only: bool = True,
        include_archived: bool = False,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        List pipelines for the authenticated user's team

        Args:
            saved_only: Only return saved pipelines (default: True)
            include_archived: Include archived pipelines (default: False)
            limit: Maximum number to return (default: 50, max: 200)
            offset: Offset for pagination (default: 0)
        """
        params = (
            f"saved_only={str(saved_only).lower()}"
            f"&include_archived={str(include_archived).lower()}"
            f"&limit={limit}&offset={offset}"
        )
        response = await self._make_request("GET", f"/api/v1/pipelines?{params}")
        return {
            "pipelines": [self._build_pipeline_config(p) for p in response.get("pipelines", [])],
            "total": response.get("total", 0),
        }

    async def get_pipeline(self, pipeline_id: str) -> PipelineConfig:
        """
        Get a pipeline by its pipeline_id

        Args:
            pipeline_id: Pipeline ID string (e.g. pl_k8Hx9mP2nQ4v)
        """
        response = await self._make_request("GET", f"/api/v1/pipelines/{pipeline_id}")
        return self._build_pipeline_config(response)

    async def update_pipeline(
        self,
        pipeline_id: str,
        steps: list[PipelineProcessor],
    ) -> PipelineConfig:
        """
        Update pipeline steps (auto-save path for draft edits)

        Args:
            pipeline_id: Pipeline ID string
            steps: New ordered list of PipelineProcessor objects
        """
        payload = {"steps": [s.to_dict() for s in steps]}
        response = await self._make_request("PUT", f"/api/v1/pipelines/{pipeline_id}", json=payload)
        return self._build_pipeline_config(response)

    async def save_pipeline(
        self,
        pipeline_id: str,
        name: str = "",
    ) -> PipelineConfig:
        """
        Name and promote a pipeline to saved status

        Args:
            pipeline_id: Pipeline ID string
            name: Display name (auto-generated if empty)
        """
        response = await self._make_request(
            "PUT", f"/api/v1/pipelines/{pipeline_id}/save", json={"name": name}
        )
        return self._build_pipeline_config(response)

    async def archive_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """Archive a pipeline, hiding it from the default list"""
        return await self._make_request("POST", f"/api/v1/pipelines/{pipeline_id}/archive")

    async def unarchive_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """Unarchive a pipeline, restoring it to the default list"""
        return await self._make_request("POST", f"/api/v1/pipelines/{pipeline_id}/unarchive")

    # --- Pipeline Versioning methods ---

    async def create_pipeline_version(
        self,
        pipeline_id: str,
        description: Optional[str] = None,
    ) -> PipelineVersion:
        """
        Create a new version snapshot of the pipeline's current steps

        Args:
            pipeline_id: Pipeline ID string
            description: Optional description for this version
        """
        payload: Dict[str, Any] = {}
        if description is not None:
            payload["description"] = description
        response = await self._make_request(
            "POST", f"/api/v1/pipelines/{pipeline_id}/versions", json=payload
        )
        return PipelineVersion(
            id=response.get("id"),
            version=response["version"],
            steps=response.get("steps", []),
            description=response.get("description"),
            created=response.get("created"),
        )

    async def list_pipeline_versions(self, pipeline_id: str) -> Dict[str, Any]:
        """List all versions of a pipeline, newest first"""
        response = await self._make_request("GET", f"/api/v1/pipelines/{pipeline_id}/versions")
        return {
            "versions": [
                PipelineVersion(
                    id=v.get("id"),
                    version=v["version"],
                    steps=v.get("steps", []),
                    description=v.get("description"),
                    created=v.get("created"),
                )
                for v in response.get("versions", [])
            ],
            "total": response.get("total", 0),
        }

    async def discard_pipeline_draft(
        self,
        pipeline_id: str,
        version: Optional[int] = None,
    ) -> PipelineConfig:
        """
        Discard draft changes and revert to a published version

        Args:
            pipeline_id: Pipeline ID string
            version: Version to revert to (default: active published version)
        """
        payload: Dict[str, Any] = {}
        if version is not None:
            payload["version"] = version
        response = await self._make_request(
            "POST", f"/api/v1/pipelines/{pipeline_id}/discard", json=payload
        )
        return self._build_pipeline_config(response)

    async def get_pipeline_rate(self, pipeline_id: str) -> Dict[str, Any]:
        """
        Get the per-page rate for a pipeline

        Returns dict with rate_per_1000_pages_cents and rate_breakdown.
        """
        return await self._make_request("GET", f"/api/v1/pipelines/{pipeline_id}/rate")

    # --- Pipeline Execution methods ---

    async def run_pipeline(
        self,
        pipeline_id: str,
        file_path: Optional[Union[str, Path]] = None,
        file_url: Optional[str] = None,
        page_range: Optional[str] = None,
        output_format: Optional[str] = None,
        run_evals: bool = False,
        skip_cache: bool = False,
        webhook_url: Optional[str] = None,
        version: Optional[int] = None,
        max_polls: int = 1,
        poll_interval: int = 1,
    ) -> PipelineExecution:
        """
        Execute a pipeline on a file

        Args:
            pipeline_id: Pipeline ID (pl_XXXXX)
            file_path: Path to the file to process
            file_url: URL of the file to process
            page_range: Page range to process (e.g. '0,2-4')
            output_format: Output format (json, html, markdown, chunks)
            run_evals: Whether to run evaluation steps
            skip_cache: Skip executor cache
            webhook_url: URL to POST when complete
            version: Pipeline version to execute (0=draft, omit=active)
            max_polls: Maximum polling attempts after submission (default: 1)
            poll_interval: Seconds between polls
        """
        form_data = self.get_form_params(file_path=file_path, file_url=file_url)
        if page_range is not None:
            form_data.add_field("page_range", page_range)
        if output_format is not None:
            form_data.add_field("output_format", output_format)
        if run_evals:
            form_data.add_field("run_evals", str(run_evals))
        if skip_cache:
            form_data.add_field("skip_cache", str(skip_cache))
        if webhook_url is not None:
            form_data.add_field("webhook_url", webhook_url)
        if version is not None:
            form_data.add_field("version", str(version))

        response = await self._submit_with_retry(
            f"/api/v1/pipelines/{pipeline_id}/run", data=form_data
        )
        execution = self._build_pipeline_execution(response)

        # Poll if requested
        if max_polls > 1 and execution.status not in ("completed", "completed_with_errors", "failed"):
            return await self.get_pipeline_execution(
                execution.execution_id, max_polls=max_polls - 1, poll_interval=poll_interval
            )
        return execution

    async def get_pipeline_execution(
        self,
        execution_id: str,
        max_polls: int = 1,
        poll_interval: int = 1,
    ) -> PipelineExecution:
        """
        Get the status of a pipeline execution, optionally polling until completion

        Args:
            execution_id: Execution ID (pex_XXXXX)
            max_polls: Maximum polling attempts (default: 1 for single check)
            poll_interval: Seconds between polls
        """
        for i in range(max_polls):
            response = await self._make_request(
                "GET", f"/api/v1/pipelines/executions/{execution_id}"
            )
            execution = self._build_pipeline_execution(response)

            if execution.status in ("completed", "completed_with_errors", "failed"):
                return execution

            if i < max_polls - 1:
                await asyncio.sleep(poll_interval)

        return execution

    async def list_pipeline_executions(
        self,
        pipeline_id: str,
        limit: int = 20,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """List recent executions for a pipeline"""
        response = await self._make_request(
            "GET", f"/api/v1/pipelines/{pipeline_id}/executions?limit={limit}&offset={offset}"
        )
        return {
            "executions": [
                self._build_pipeline_execution(e) for e in response.get("executions", [])
            ],
            "total": response.get("total", 0),
        }

    async def get_step_result(
        self,
        execution_id: str,
        step_index: int,
    ) -> Dict[str, Any]:
        """
        Fetch the result of a specific pipeline execution step

        Args:
            execution_id: Execution ID (pex_XXXXX)
            step_index: Zero-based step index
        """
        return await self._make_request(
            "GET", f"/api/v1/pipelines/executions/{execution_id}/steps/{step_index}/result"
        )

    @staticmethod
    def _build_pipeline_config(data: Dict[str, Any]) -> PipelineConfig:
        return PipelineConfig(
            id=data.get("id"),
            pipeline_id=data["pipeline_id"],
            name=data.get("name"),
            steps=data.get("steps", []),
            is_saved=data.get("is_saved", False),
            archived=data.get("archived", False),
            active_version=data.get("active_version", 0),
            created=data.get("created"),
            updated=data.get("updated"),
        )

    @staticmethod
    def _build_pipeline_execution(data: Dict[str, Any]) -> PipelineExecution:
        steps = [
            PipelineExecutionStepResult(
                step_index=s["step_index"],
                step_type=s["step_type"],
                status=s["status"],
                lookup_key=s.get("lookup_key"),
                result_url=s.get("result_url"),
                started_at=s.get("started_at"),
                finished_at=s.get("finished_at"),
                error_message=s.get("error_message"),
                source_step_type=s.get("source_step_type"),
                checkpoint_id=s.get("checkpoint_id"),
            )
            for s in data.get("steps", [])
        ]
        return PipelineExecution(
            execution_id=data["execution_id"],
            pipeline_id=data.get("pipeline_id", ""),
            pipeline_version=data.get("pipeline_version", 0),
            status=data.get("status", "pending"),
            steps=steps,
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            created=data.get("created"),
            config_snapshot=data.get("config_snapshot"),
            input_config=data.get("input_config"),
            rate_breakdown=data.get("rate_breakdown"),
        )

    # --- Custom Processor Management methods ---

    async def list_custom_processors(
        self,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        List custom processors for the authenticated user's team

        Args:
            limit: Maximum number to return (default: 50)
            offset: Offset for pagination (default: 0)
        """
        response = await self._make_request(
            "GET", f"/api/v1/custom_processors?limit={limit}&offset={offset}"
        )
        processors = [
            CustomProcessor(
                processor_id=p["processor_id"],
                name=p.get("name"),
                status=p.get("status", ""),
                success=p.get("success"),
                active_version=p.get("active_version", 0),
                max_version=p.get("max_version", 0),
                iteration_in_progress=p.get("iteration_in_progress", False),
                pipeline_id=p.get("pipeline_id"),
                created_at=p.get("created_at"),
                completed_at=p.get("completed_at"),
                error_message=p.get("error_message"),
                eval_rubric_id=p.get("eval_rubric_id"),
            )
            for p in response.get("pipelines", [])
        ]
        return {"processors": processors}

    async def get_custom_processor_status(self, lookup_key: str) -> Dict[str, Any]:
        """
        Check the status of a custom processor request

        Args:
            lookup_key: The lookup key returned when the processor was submitted
        """
        return await self._make_request("GET", f"/api/v1/custom_processors/{lookup_key}")

    async def list_custom_processor_versions(self, processor_id: str) -> Dict[str, Any]:
        """
        List versions of a custom processor

        Args:
            processor_id: Processor ID (cp_XXXXX)
        """
        response = await self._make_request(
            "GET", f"/api/v1/custom_processors/{processor_id}/versions"
        )
        versions = [
            CustomProcessorVersion(
                version=v["version"],
                request_description=v.get("request_description", ""),
                created_at=v.get("created_at"),
                runtime=v.get("runtime"),
                is_active=v.get("is_active", False),
            )
            for v in response.get("versions", [])
        ]
        return {"versions": versions}

    async def set_active_processor_version(
        self,
        processor_id: str,
        version: int,
    ) -> Dict[str, Any]:
        """
        Set the active version of a custom processor

        Args:
            processor_id: Processor ID (cp_XXXXX)
            version: Version number to activate
        """
        form_data = aiohttp.FormData()
        form_data.add_field("version", str(version))
        return await self._submit_with_retry(
            f"/api/v1/custom_processors/{processor_id}/set_active", data=form_data
        )

    async def archive_custom_processor(self, processor_id: str) -> Dict[str, Any]:
        """
        Archive a custom processor

        Args:
            processor_id: Processor ID (cp_XXXXX)
        """
        return await self._make_request(
            "POST", f"/api/v1/custom_processors/{processor_id}/archive"
        )


class DatalabClient:
    """Synchronous wrapper around AsyncDatalabClient"""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = settings.DATALAB_HOST,
        timeout: int = 300,
    ):
        """
        Initialize the Datalab client

        Args:
            api_key: Your Datalab API key
            base_url: Base URL for the API (default: https://www.datalab.to)
            timeout: Default timeout for requests in seconds
        """
        self._async_client = AsyncDatalabClient(api_key, base_url, timeout)

    def _run_async(self, coro):
        """Run async coroutine in sync context"""
        try:
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(self._async_wrapper(coro))
        except RuntimeError:
            # No event loop exists, create and clean up
            return asyncio.run(self._async_wrapper(coro))

    async def _async_wrapper(self, coro):
        """Wrapper to ensure session management"""
        async with self._async_client:
            return await coro

    def convert(
        self,
        file_path: Optional[Union[str, Path]] = None,
        file_url: Optional[str] = None,
        options: Optional[ConvertOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        stream_response_to: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> Union[ConversionResult, FileResult]:
        """
        Convert a document to markdown, HTML, JSON, or chunks (sync version)

        Args:
            file_path: Path to the file to convert
            file_url: URL of the file to convert
            options: Processing options for conversion
            save_output: Optional path to save output files
            stream_response_to: Optional path to stream raw JSON response to disk
            max_polls: Maximum number of polling attempts
            poll_interval: Seconds between polling attempts
        """
        return self._run_async(
            self._async_client.convert(
                file_path=file_path,
                file_url=file_url,
                options=options,
                save_output=save_output,
                stream_response_to=stream_response_to,
                max_polls=max_polls,
                poll_interval=poll_interval,
            )
        )

    def extract(
        self,
        file_path: Optional[Union[str, Path]] = None,
        file_url: Optional[str] = None,
        options: Optional[ExtractOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        stream_response_to: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> Union[ConversionResult, FileResult]:
        """
        Extract structured data using a JSON schema or saved extraction schema (sync version)

        Args:
            file_path: Path to the file to extract from
            file_url: URL of the file to extract from
            options: Extraction options (must include page_schema or schema_id)
            save_output: Optional path to save output files
            stream_response_to: Optional path to stream raw JSON response to disk
            max_polls: Maximum number of polling attempts
            poll_interval: Seconds between polling attempts
        """
        return self._run_async(
            self._async_client.extract(
                file_path=file_path,
                file_url=file_url,
                options=options,
                save_output=save_output,
                stream_response_to=stream_response_to,
                max_polls=max_polls,
                poll_interval=poll_interval,
            )
        )

    def segment(
        self,
        file_path: Optional[Union[str, Path]] = None,
        file_url: Optional[str] = None,
        options: Optional[SegmentOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        stream_response_to: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> Union[ConversionResult, FileResult]:
        """
        Segment a document into sections using a schema (sync version)

        Args:
            file_path: Path to the file to segment
            file_url: URL of the file to segment
            options: Segmentation options (must include segmentation_schema)
            save_output: Optional path to save output files
            stream_response_to: Optional path to stream raw JSON response to disk
            max_polls: Maximum number of polling attempts
            poll_interval: Seconds between polling attempts
        """
        return self._run_async(
            self._async_client.segment(
                file_path=file_path,
                file_url=file_url,
                options=options,
                save_output=save_output,
                stream_response_to=stream_response_to,
                max_polls=max_polls,
                poll_interval=poll_interval,
            )
        )

    def run_custom_processor(
        self,
        file_path: Optional[Union[str, Path]] = None,
        file_url: Optional[str] = None,
        options: Optional[CustomProcessorOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        stream_response_to: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> Union[ConversionResult, FileResult]:
        """
        Execute a custom processor on a document (sync version)

        Args:
            file_path: Path to the file to process
            file_url: URL of the file to process
            options: Custom processor options (must include pipeline_id)
            save_output: Optional path to save output files
            stream_response_to: Optional path to stream raw JSON response to disk
            max_polls: Maximum number of polling attempts
            poll_interval: Seconds between polling attempts
        """
        return self._run_async(
            self._async_client.run_custom_processor(
                file_path=file_path,
                file_url=file_url,
                options=options,
                save_output=save_output,
                stream_response_to=stream_response_to,
                max_polls=max_polls,
                poll_interval=poll_interval,
            )
        )

    def run_custom_pipeline(
        self,
        file_path: Optional[Union[str, Path]] = None,
        file_url: Optional[str] = None,
        options: Optional[CustomProcessorOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        stream_response_to: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> Union[ConversionResult, FileResult]:
        """Execute a custom processor on a document (sync version)

        .. deprecated::
            Use run_custom_processor() instead.
        """
        warnings.warn(
            "run_custom_pipeline() is deprecated. Use run_custom_processor() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.run_custom_processor(
            file_path=file_path,
            file_url=file_url,
            options=options,
            save_output=save_output,
            stream_response_to=stream_response_to,
            max_polls=max_polls,
            poll_interval=poll_interval,
        )

    def track_changes(
        self,
        file_path: Optional[Union[str, Path]] = None,
        file_url: Optional[str] = None,
        options: Optional[TrackChangesOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        stream_response_to: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> Union[ConversionResult, FileResult]:
        """
        Extract and display tracked changes from DOCX documents (sync version)

        Args:
            file_path: Path to the DOCX file
            file_url: URL of the DOCX file
            options: Track changes options
            save_output: Optional path to save output files
            stream_response_to: Optional path to stream raw JSON response to disk
            max_polls: Maximum number of polling attempts
            poll_interval: Seconds between polling attempts
        """
        return self._run_async(
            self._async_client.track_changes(
                file_path=file_path,
                file_url=file_url,
                options=options,
                save_output=save_output,
                stream_response_to=stream_response_to,
                max_polls=max_polls,
                poll_interval=poll_interval,
            )
        )

    def create_document(
        self,
        markdown: str,
        output_format: str = "docx",
        webhook_url: Optional[str] = None,
        save_output: Optional[Union[str, Path]] = None,
        stream_response_to: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> Union[CreateDocumentResult, FileResult]:
        """
        Create a DOCX document from markdown (sync version)

        Args:
            markdown: The markdown content to convert to a document
            output_format: Output format (currently only 'docx')
            webhook_url: Optional webhook URL for completion notification
            save_output: Optional path to save the output file
            stream_response_to: Optional path to stream raw JSON response to disk
            max_polls: Maximum number of polling attempts
            poll_interval: Seconds between polling attempts
        """
        return self._run_async(
            self._async_client.create_document(
                markdown=markdown,
                output_format=output_format,
                webhook_url=webhook_url,
                save_output=save_output,
                stream_response_to=stream_response_to,
                max_polls=max_polls,
                poll_interval=poll_interval,
            )
        )

    def ocr(
        self,
        file_path: Union[str, Path],
        options: Optional[ProcessingOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        stream_response_to: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> Union[OCRResult, FileResult]:
        """Perform OCR on a document (sync version)

        .. deprecated::
            The /ocr endpoint is deprecated. Use convert() instead.
        """
        return self._run_async(
            self._async_client.ocr(
                file_path=file_path,
                options=options,
                save_output=save_output,
                stream_response_to=stream_response_to,
                max_polls=max_polls,
                poll_interval=poll_interval,
            )
        )

    def fill(
        self,
        file_path: Optional[Union[str, Path]] = None,
        file_url: Optional[str] = None,
        options: Optional[FormFillingOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        stream_response_to: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> Union[FormFillingResult, FileResult]:
        """
        Fill PDF or image forms with provided field data (sync version)

        Args:
            file_path: Path to the file to fill
            file_url: URL of the file to fill
            options: Form filling options (must include field_data)
            save_output: Optional path to save output files
            stream_response_to: Optional path to stream raw JSON response to disk
            max_polls: Maximum number of polling attempts
            poll_interval: Seconds between polling attempts
        """
        return self._run_async(
            self._async_client.fill(
                file_path=file_path,
                file_url=file_url,
                options=options,
                save_output=save_output,
                stream_response_to=stream_response_to,
                max_polls=max_polls,
                poll_interval=poll_interval,
            )
        )

    # Workflow methods (sync)
    def create_workflow(
        self,
        name: str,
        steps: list[WorkflowStep],
    ) -> Workflow:
        """Create a new workflow (sync version)"""
        return self._run_async(
            self._async_client.create_workflow(
                name=name,
                steps=steps,
            )
        )

    def get_workflow(self, workflow_id: int) -> Workflow:
        """Get a workflow by ID (sync version)"""
        return self._run_async(self._async_client.get_workflow(workflow_id))

    def get_step_types(self) -> dict:
        """Get all available workflow step types (sync version)"""
        return self._run_async(self._async_client.get_step_types())

    def list_workflows(self) -> list[Workflow]:
        """List all workflows (sync version)"""
        return self._run_async(self._async_client.list_workflows())

    def execute_workflow(
        self,
        workflow_id: int,
        input_config: InputConfig,
    ) -> WorkflowExecution:
        """Execute a workflow (sync version)"""
        return self._run_async(
            self._async_client.execute_workflow(
                workflow_id=workflow_id,
                input_config=input_config,
            )
        )

    def get_execution_status(
        self,
        execution_id: int,
        max_polls: int = 1,
        poll_interval: int = 1,
        download_results: bool = False,
    ) -> WorkflowExecution:
        """Get execution status (sync version)"""
        return self._run_async(
            self._async_client.get_execution_status(
                execution_id=execution_id,
                max_polls=max_polls,
                poll_interval=poll_interval,
                download_results=download_results,
            )
        )

    # File upload methods (sync)
    def upload_files(
        self,
        file_paths: Union[str, Path, list[Union[str, Path]]],
    ) -> Union[UploadedFileMetadata, list[UploadedFileMetadata]]:
        """
        Upload one or more files to Datalab storage (sync version)

        This method handles the complete upload flow for each file:
        1. Request a presigned upload URL
        2. Upload the file to the presigned URL
        3. Confirm the upload with the API

        Multiple files are uploaded concurrently for better performance.

        Args:
            file_paths: Single file path or list of file paths to upload

        Returns:
            If single file: UploadedFileMetadata object
            If multiple files: List of UploadedFileMetadata objects

        Example:
            # Upload single file
            metadata = client.upload_files("document.pdf")

            # Upload multiple files
            metadatas = client.upload_files(["doc1.pdf", "doc2.pdf"])
        """
        return self._run_async(self._async_client.upload_files(file_paths=file_paths))

    def list_files(
        self,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        List uploaded files for the authenticated user's team (sync version)

        Args:
            limit: Maximum number of files to return (default: 50)
            offset: Offset for pagination (default: 0)

        Returns:
            Dictionary containing:
                - files: List of UploadedFileMetadata objects
                - total: Total number of files
                - limit: Limit used
                - offset: Offset used
        """
        return self._run_async(
            self._async_client.list_files(limit=limit, offset=offset)
        )

    def get_file_metadata(
        self,
        file_id: Union[int, str],
    ) -> UploadedFileMetadata:
        """
        Get metadata for an uploaded file (sync version)

        Args:
            file_id: File ID (integer or hashid string)

        Returns:
            UploadedFileMetadata object with file information
        """
        return self._run_async(self._async_client.get_file_metadata(file_id=file_id))

    def get_file_download_url(
        self,
        file_id: Union[int, str],
        expires_in: int = 3600,
    ) -> Dict[str, Any]:
        """
        Generate presigned URL for downloading a file (sync version)

        Args:
            file_id: File ID (integer or hashid string)
            expires_in: URL expiry time in seconds (default: 3600, max: 86400)

        Returns:
            Dictionary containing:
                - download_url: Presigned URL for downloading the file
                - expires_in: URL expiry time in seconds
                - file_id: File ID
                - original_filename: Original filename
        """
        return self._run_async(
            self._async_client.get_file_download_url(
                file_id=file_id, expires_in=expires_in
            )
        )

    def delete_file(
        self,
        file_id: Union[int, str],
    ) -> Dict[str, Any]:
        """
        Delete an uploaded file (sync version)

        Removes the file from both storage and the database.

        Args:
            file_id: File ID (integer or hashid string)

        Returns:
            Dictionary containing:
                - success: Whether the deletion was successful
                - message: Confirmation message
        """
        return self._run_async(self._async_client.delete_file(file_id=file_id))

    def delete_workflow(self, workflow_id: int) -> Dict[str, Any]:
        """
        Delete a workflow definition (sync version)

        Args:
            workflow_id: ID of the workflow to delete

        Returns:
            Dictionary containing:
                - success: Whether the deletion was successful
                - message: Confirmation message

        Raises:
            DatalabAPIError: If workflow has executions or cannot be deleted
        """
        return self._run_async(
            self._async_client.delete_workflow(workflow_id=workflow_id)
        )

    # --- Extraction Schema methods (sync) ---

    def create_extraction_schema(
        self, name: str, schema_json: Dict[str, Any], description: Optional[str] = None,
    ) -> ExtractionSchema:
        """Create a new extraction schema (sync version)"""
        return self._run_async(
            self._async_client.create_extraction_schema(
                name=name, schema_json=schema_json, description=description,
            )
        )

    def list_extraction_schemas(
        self, limit: int = 50, offset: int = 0, include_archived: bool = False,
    ) -> Dict[str, Any]:
        """List extraction schemas (sync version)"""
        return self._run_async(
            self._async_client.list_extraction_schemas(
                limit=limit, offset=offset, include_archived=include_archived,
            )
        )

    def get_extraction_schema(self, schema_id: str) -> ExtractionSchema:
        """Get an extraction schema by ID (sync version)"""
        return self._run_async(self._async_client.get_extraction_schema(schema_id=schema_id))

    def update_extraction_schema(
        self,
        schema_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        schema_json: Optional[Dict[str, Any]] = None,
        archived: Optional[bool] = None,
        create_new_version: bool = False,
    ) -> ExtractionSchema:
        """Update an extraction schema (sync version)"""
        return self._run_async(
            self._async_client.update_extraction_schema(
                schema_id=schema_id, name=name, description=description,
                schema_json=schema_json, archived=archived,
                create_new_version=create_new_version,
            )
        )

    def delete_extraction_schema(self, schema_id: str) -> ExtractionSchema:
        """Delete (archive) an extraction schema (sync version)"""
        return self._run_async(self._async_client.delete_extraction_schema(schema_id=schema_id))

    # --- Pipeline methods (sync) ---

    def create_pipeline(self, steps: list[PipelineProcessor]) -> PipelineConfig:
        """Create a new pipeline (sync version)"""
        return self._run_async(self._async_client.create_pipeline(steps=steps))

    def list_pipelines(
        self, saved_only: bool = True, include_archived: bool = False,
        limit: int = 50, offset: int = 0,
    ) -> Dict[str, Any]:
        """List pipelines (sync version)"""
        return self._run_async(
            self._async_client.list_pipelines(
                saved_only=saved_only, include_archived=include_archived,
                limit=limit, offset=offset,
            )
        )

    def get_pipeline(self, pipeline_id: str) -> PipelineConfig:
        """Get a pipeline by ID (sync version)"""
        return self._run_async(self._async_client.get_pipeline(pipeline_id=pipeline_id))

    def update_pipeline(self, pipeline_id: str, steps: list[PipelineProcessor]) -> PipelineConfig:
        """Update pipeline steps (sync version)"""
        return self._run_async(
            self._async_client.update_pipeline(pipeline_id=pipeline_id, steps=steps)
        )

    def save_pipeline(self, pipeline_id: str, name: str = "") -> PipelineConfig:
        """Save and name a pipeline (sync version)"""
        return self._run_async(
            self._async_client.save_pipeline(pipeline_id=pipeline_id, name=name)
        )

    def archive_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """Archive a pipeline (sync version)"""
        return self._run_async(self._async_client.archive_pipeline(pipeline_id=pipeline_id))

    def unarchive_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """Unarchive a pipeline (sync version)"""
        return self._run_async(self._async_client.unarchive_pipeline(pipeline_id=pipeline_id))

    def create_pipeline_version(
        self, pipeline_id: str, description: Optional[str] = None,
    ) -> PipelineVersion:
        """Create a pipeline version snapshot (sync version)"""
        return self._run_async(
            self._async_client.create_pipeline_version(
                pipeline_id=pipeline_id, description=description,
            )
        )

    def list_pipeline_versions(self, pipeline_id: str) -> Dict[str, Any]:
        """List pipeline versions (sync version)"""
        return self._run_async(self._async_client.list_pipeline_versions(pipeline_id=pipeline_id))

    def discard_pipeline_draft(
        self, pipeline_id: str, version: Optional[int] = None,
    ) -> PipelineConfig:
        """Discard draft and revert to a published version (sync version)"""
        return self._run_async(
            self._async_client.discard_pipeline_draft(
                pipeline_id=pipeline_id, version=version,
            )
        )

    def get_pipeline_rate(self, pipeline_id: str) -> Dict[str, Any]:
        """Get pipeline per-page rate (sync version)"""
        return self._run_async(self._async_client.get_pipeline_rate(pipeline_id=pipeline_id))

    def run_pipeline(
        self,
        pipeline_id: str,
        file_path: Optional[Union[str, Path]] = None,
        file_url: Optional[str] = None,
        page_range: Optional[str] = None,
        output_format: Optional[str] = None,
        run_evals: bool = False,
        skip_cache: bool = False,
        webhook_url: Optional[str] = None,
        version: Optional[int] = None,
        max_polls: int = 1,
        poll_interval: int = 1,
    ) -> PipelineExecution:
        """Execute a pipeline on a file (sync version)"""
        return self._run_async(
            self._async_client.run_pipeline(
                pipeline_id=pipeline_id, file_path=file_path, file_url=file_url,
                page_range=page_range, output_format=output_format,
                run_evals=run_evals, skip_cache=skip_cache,
                webhook_url=webhook_url, version=version,
                max_polls=max_polls, poll_interval=poll_interval,
            )
        )

    def get_pipeline_execution(
        self, execution_id: str, max_polls: int = 1, poll_interval: int = 1,
    ) -> PipelineExecution:
        """Get pipeline execution status (sync version)"""
        return self._run_async(
            self._async_client.get_pipeline_execution(
                execution_id=execution_id, max_polls=max_polls, poll_interval=poll_interval,
            )
        )

    def list_pipeline_executions(
        self, pipeline_id: str, limit: int = 20, offset: int = 0,
    ) -> Dict[str, Any]:
        """List pipeline executions (sync version)"""
        return self._run_async(
            self._async_client.list_pipeline_executions(
                pipeline_id=pipeline_id, limit=limit, offset=offset,
            )
        )

    def get_step_result(self, execution_id: str, step_index: int) -> Dict[str, Any]:
        """Get a pipeline execution step result (sync version)"""
        return self._run_async(
            self._async_client.get_step_result(
                execution_id=execution_id, step_index=step_index,
            )
        )

    # --- Custom Processor Management methods (sync) ---

    def list_custom_processors(self, limit: int = 50, offset: int = 0) -> Dict[str, Any]:
        """List custom processors (sync version)"""
        return self._run_async(
            self._async_client.list_custom_processors(limit=limit, offset=offset)
        )

    def get_custom_processor_status(self, lookup_key: str) -> Dict[str, Any]:
        """Check custom processor request status (sync version)"""
        return self._run_async(
            self._async_client.get_custom_processor_status(lookup_key=lookup_key)
        )

    def list_custom_processor_versions(self, processor_id: str) -> Dict[str, Any]:
        """List custom processor versions (sync version)"""
        return self._run_async(
            self._async_client.list_custom_processor_versions(processor_id=processor_id)
        )

    def set_active_processor_version(self, processor_id: str, version: int) -> Dict[str, Any]:
        """Set active processor version (sync version)"""
        return self._run_async(
            self._async_client.set_active_processor_version(
                processor_id=processor_id, version=version,
            )
        )

    def archive_custom_processor(self, processor_id: str) -> Dict[str, Any]:
        """Archive a custom processor (sync version)"""
        return self._run_async(
            self._async_client.archive_custom_processor(processor_id=processor_id)
        )
