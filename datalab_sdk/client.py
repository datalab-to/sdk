"""
Datalab API client - async core with sync wrapper
"""

import asyncio
import mimetypes
import aiohttp
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
    OCRResult,
    ProcessingOptions,
    ConvertOptions,
    OCROptions,
    FormFillingOptions,
    FormFillingResult,
    Workflow,
    WorkflowStep,
    WorkflowExecution,
    InputConfig,
    UploadedFileMetadata,
    ProviderResponse,
    PageResult,
)
from datalab_sdk.settings import settings


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
                error_message = error_data.get("detail") or error_data.get("error") or str(e)
            except Exception:
                error_message = str(e)
            raise DatalabAPIError(
                error_message,
                e.status,
                error_data if "error_data" in locals() else None,
            )
        except aiohttp.ClientError as e:
            raise DatalabAPIError(f"Request failed: {str(e)}")

    async def _poll_result(
        self, check_url: str, max_polls: int = 300, poll_interval: int = 1
    ) -> Dict[str, Any]:
        """Poll for result completion"""
        full_url = (
            check_url
            if check_url.startswith("http")
            else f"{self.base_url}/{check_url.lstrip('/')}"
        )

        for i in range(max_polls):
            data = await self._poll_get_with_retry(full_url)

            if data.get("status") == "complete":
                return data

            if not data.get("success", True) and not data.get("status") == "processing":
                raise DatalabAPIError(
                    f"Processing failed: {data.get('error', 'Unknown error')}"
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
        stop=stop_after_attempt(2),
        wait=wait_exponential_jitter(max=0.5),
        reraise=True,
    )
    async def _poll_get_with_retry(self, url: str) -> Dict[str, Any]:
        """GET wrapper for polling with scoped retries for transient failures"""
        return await self._make_request("GET", url)

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

    def get_form_params(self, file_path=None, file_url=None, options=None):
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
        else:
            raise ValueError("Either file_path or file_url must be provided")

        if options:
            for key, value in options.to_form_data().items():
                if isinstance(value, tuple):
                    form_data.add_field(key, str(value[1]))
                else:
                    form_data.add_field(key, str(value))

        return form_data

    # Convenient endpoint-specific methods
    async def convert(
        self,
        file_path: Optional[Union[str, Path]] = None,
        file_url: Optional[str] = None,
        options: Optional[ConvertOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> ConversionResult:
        """
        Convert a document using the marker endpoint

        Args:
            file_path: Path to the file to convert
            file_url: URL of the file to convert
            options: Processing options for conversion (use ConvertOptions.webhook_url
                    to override the webhook URL stored in your account settings)
            save_output: Optional path to save output files
            max_polls: Maximum number of polling attempts
            poll_interval: Seconds between polling attempts
        """
        if options is None:
            options = ConvertOptions()

        initial_data = await self._make_request(
            "POST",
            "/api/v1/marker",
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
        )

        result = ConversionResult(
            success=result_data.get("success", False),
            output_format=result_data.get("output_format", options.output_format),
            markdown=result_data.get("markdown"),
            html=result_data.get("html"),
            json=result_data.get("json"),
            chunks=result_data.get("chunks"),
            extraction_schema_json=result_data.get("extraction_schema_json"),
            segmentation_results=result_data.get("segmentation_results"),
            images=result_data.get("images"),
            metadata=result_data.get("metadata"),
            error=result_data.get("error"),
            page_count=result_data.get("page_count"),
            status=result_data.get("status", "complete"),
            checkpoint_id=result_data.get("checkpoint_id"),
            versions=result_data.get("versions"),
            parse_quality_score=result_data.get("parse_quality_score"),
            runtime=result_data.get("runtime"),
            cost_breakdown=result_data.get("cost_breakdown"),
        )

        # Save output if requested
        if save_output and result.success:
            output_path = Path(save_output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            result.save_output(output_path)

        return result

    async def ocr(
        self,
        file_path: Union[str, Path],
        options: Optional[ProcessingOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> OCRResult:
        """Perform OCR on a document"""
        if options is None:
            options = OCROptions()

        initial_data = await self._make_request(
            "POST",
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
        )

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
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> FormFillingResult:
        """
        Fill PDF or image forms with provided field data

        Args:
            file_path: Path to the file to fill
            file_url: URL of the file to fill
            options: Form filling options (must include field_data)
            save_output: Optional path to save output files
            max_polls: Maximum number of polling attempts
            poll_interval: Seconds between polling attempts
        """
        if options is None:
            raise ValueError("options must be provided with field_data")

        initial_data = await self._make_request(
            "POST",
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
        )

        result = FormFillingResult(
            status=result_data.get("status", "complete"),
            success=result_data.get("success"),
            error=result_data.get("error"),
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
            "message": response.get("message", f"Workflow {workflow_id} deleted successfully"),
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

    # Provider methods
    async def _call_provider(
        self,
        provider_name: str,
        endpoint_path: str,
        file_path: Union[str, Path],
        page_range: Optional[str] = None,
        mathpix_app_id: Optional[str] = None,
        mathpix_app_key: Optional[str] = None,
    ) -> ProviderResponse:
        """
        Internal helper for calling provider endpoints.

        Args:
            provider_name: Display name of the provider (for error messages)
            endpoint_path: API endpoint path (e.g., "olmocr", "mathpix")
            file_path: Path to the file to process
            page_range: Optional page range (e.g., "1-3" or "1,3,5")
            mathpix_app_id: Mathpix App ID (required for mathpix provider)
            mathpix_app_key: Mathpix App Key (required for mathpix provider)

        Returns:
            ProviderResponse object
        """
        await self._ensure_session()

        # Prepare file data
        filename, file_data, mime_type = self._prepare_file_data(file_path)

        # Prepare form data
        form_data = aiohttp.FormData()
        form_data.add_field("file", file_data, filename=filename, content_type=mime_type)

        if page_range:
            form_data.add_field("page_range", page_range)

        # Prepare headers
        headers = {}
        if provider_name.lower() == "mathpix":
            if not mathpix_app_id or not mathpix_app_key:
                raise DatalabAPIError(
                    "Mathpix requires app_id and app_key credentials"
                )
            headers["X-Mathpix-App-Id"] = mathpix_app_id
            headers["X-Mathpix-App-Key"] = mathpix_app_key

        # Make request
        url = f"{self.base_url}/api/v1/providers/{endpoint_path}"
        try:
            async with self._session.post(url, data=form_data, headers=headers) as response:
                try:
                    response.raise_for_status()
                    data = await response.json()
                except aiohttp.ClientResponseError as e:
                    try:
                        error_data = await response.json()
                        error_message = error_data.get("detail") or error_data.get("error") or str(e)
                    except Exception:
                        error_message = str(e)
                    raise DatalabAPIError(
                        error_message,
                        e.status,
                        error_data if "error_data" in locals() else None,
                    )
        except asyncio.TimeoutError:
            raise DatalabTimeoutError(
                f"{provider_name} request timed out after {self.timeout} seconds"
            )
        except DatalabAPIError:
            raise
        except aiohttp.ClientError as e:
            raise DatalabAPIError(f"{provider_name} request failed: {str(e)}")

        # Parse response
        pages = [
            PageResult(
                page_num=page.get("page_num", i),
                success=page.get("success", False),
                error=page.get("error"),
                markdown=page.get("markdown"),
                html=page.get("html"),
                blocks=page.get("blocks"),
                images=page.get("images"),
            )
            for i, page in enumerate(data.get("pages", []))
        ]

        return ProviderResponse(
            success=data.get("success", False),
            error=data.get("error"),
            pages=pages,
            runtime=data.get("runtime", 0.0),
        )

    async def olmocr(
        self,
        file_path: Union[str, Path],
        page_range: Optional[str] = None,
    ) -> ProviderResponse:
        """
        Process a document using OlmoOCR provider

        Args:
            file_path: Path to the file to process
            page_range: Optional page range (e.g., "1-3" or "1,3,5")

        Returns:
            ProviderResponse object
        """
        return await self._call_provider("OlmoOCR", "olmocr", file_path, page_range)

    async def rolmocr(
        self,
        file_path: Union[str, Path],
        page_range: Optional[str] = None,
    ) -> ProviderResponse:
        """
        Process a document using RolmoOCR provider

        Args:
            file_path: Path to the file to process
            page_range: Optional page range (e.g., "1-3" or "1,3,5")

        Returns:
            ProviderResponse object
        """
        return await self._call_provider("RolmoOCR", "rolmocr", file_path, page_range)

    async def dotsocr(
        self,
        file_path: Union[str, Path],
        page_range: Optional[str] = None,
    ) -> ProviderResponse:
        """
        Process a document using DotsOCR provider

        Args:
            file_path: Path to the file to process
            page_range: Optional page range (e.g., "1-3" or "1,3,5")

        Returns:
            ProviderResponse object
        """
        return await self._call_provider("DotsOCR", "dotsocr", file_path, page_range)

    async def deepseekocr(
        self,
        file_path: Union[str, Path],
        page_range: Optional[str] = None,
    ) -> ProviderResponse:
        """
        Process a document using DeepSeekOCR provider

        Args:
            file_path: Path to the file to process
            page_range: Optional page range (e.g., "1-3" or "1,3,5")

        Returns:
            ProviderResponse object
        """
        return await self._call_provider("DeepSeekOCR", "deepseekocr", file_path, page_range)

    async def mathpix(
        self,
        file_path: Union[str, Path],
        app_id: str,
        app_key: str,
        page_range: Optional[str] = None,
    ) -> ProviderResponse:
        """
        Process a document using Mathpix provider

        Args:
            file_path: Path to the file to process
            app_id: Mathpix App ID
            app_key: Mathpix App Key
            page_range: Optional page range (e.g., "1-3" or "1,3,5")

        Returns:
            ProviderResponse object
        """
        return await self._call_provider(
            "Mathpix",
            "mathpix",
            file_path,
            page_range,
            mathpix_app_id=app_id,
            mathpix_app_key=app_key,
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
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> ConversionResult:
        """
        Convert a document using the marker endpoint (sync version)

        Args:
            file_path: Path to the file to convert
            file_url: URL of the file to convert
            options: Processing options for conversion
            save_output: Optional path to save output files
            max_polls: Maximum number of polling attempts
            poll_interval: Seconds between polling attempts
        """
        return self._run_async(
            self._async_client.convert(
                file_path=file_path,
                file_url=file_url,
                options=options,
                save_output=save_output,
                max_polls=max_polls,
                poll_interval=poll_interval,
            )
        )

    def ocr(
        self,
        file_path: Union[str, Path],
        options: Optional[ProcessingOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> OCRResult:
        """Perform OCR on a document (sync version)"""
        return self._run_async(
            self._async_client.ocr(
                file_path=file_path,
                options=options,
                save_output=save_output,
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
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> FormFillingResult:
        """
        Fill PDF or image forms with provided field data (sync version)

        Args:
            file_path: Path to the file to fill
            file_url: URL of the file to fill
            options: Form filling options (must include field_data)
            save_output: Optional path to save output files
            max_polls: Maximum number of polling attempts
            poll_interval: Seconds between polling attempts
        """
        return self._run_async(
            self._async_client.fill(
                file_path=file_path,
                file_url=file_url,
                options=options,
                save_output=save_output,
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
        return self._run_async(
            self._async_client.get_file_metadata(file_id=file_id)
        )

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
            self._async_client.get_file_download_url(file_id=file_id, expires_in=expires_in)
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
        return self._run_async(
            self._async_client.delete_file(file_id=file_id)
        )

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

    # Provider methods (sync)
    def olmocr(
        self,
        file_path: Union[str, Path],
        page_range: Optional[str] = None,
    ) -> ProviderResponse:
        """Process a document using OlmoOCR provider (sync version)"""
        return self._run_async(
            self._async_client.olmocr(file_path=file_path, page_range=page_range)
        )

    def rolmocr(
        self,
        file_path: Union[str, Path],
        page_range: Optional[str] = None,
    ) -> ProviderResponse:
        """Process a document using RolmoOCR provider (sync version)"""
        return self._run_async(
            self._async_client.rolmocr(file_path=file_path, page_range=page_range)
        )

    def dotsocr(
        self,
        file_path: Union[str, Path],
        page_range: Optional[str] = None,
    ) -> ProviderResponse:
        """Process a document using DotsOCR provider (sync version)"""
        return self._run_async(
            self._async_client.dotsocr(file_path=file_path, page_range=page_range)
        )

    def deepseekocr(
        self,
        file_path: Union[str, Path],
        page_range: Optional[str] = None,
    ) -> ProviderResponse:
        """Process a document using DeepSeekOCR provider (sync version)"""
        return self._run_async(
            self._async_client.deepseekocr(file_path=file_path, page_range=page_range)
        )

    def mathpix(
        self,
        file_path: Union[str, Path],
        app_id: str,
        app_key: str,
        page_range: Optional[str] = None,
    ) -> ProviderResponse:
        """Process a document using Mathpix provider (sync version)"""
        return self._run_async(
            self._async_client.mathpix(
                file_path=file_path,
                app_id=app_id,
                app_key=app_key,
                page_range=page_range,
            )
        )
