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
    Workflow,
    WorkflowStep,
    WorkflowExecution,
    InputConfig,
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
                error_message = error_data.get("error", str(e))
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

        # Determine MIME type
        mime_type, _ = mimetypes.guess_type(str(file_path))
        if not mime_type:
            # Try to detect from extension
            extension = file_path.suffix.lower()
            mime_type = MIMETYPE_MAP.get(extension, "application/octet-stream")

        return file_path.name, file_path.read_bytes(), mime_type

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
        options: Optional[ProcessingOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> ConversionResult:
        """Convert a document using the marker endpoint"""
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
            images=result_data.get("images"),
            metadata=result_data.get("metadata"),
            error=result_data.get("error"),
            page_count=result_data.get("page_count"),
            status=result_data.get("status", "complete"),
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
        )

        # Save output if requested
        if save_output and result.success:
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
            results=response.get("results"),
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
                results = await self._download_step_results(steps_data)
            else:
                # Keep the raw step data with URLs
                results = steps_data

            # Determine success based on status
            success = status == "COMPLETED"
            error = response.get("error")

            # If any step failed, extract error
            if status == "FAILED" or not success:
                failed_steps = [
                    name for name, step in steps_data.items()
                    if step.get("status") == "FAILED"
                ]
                if failed_steps and not error:
                    error = f"Step(s) failed: {', '.join(failed_steps)}"

            execution = WorkflowExecution(
                id=response.get("execution_id") or response.get("id") or execution_id,
                workflow_id=response["workflow_id"],
                status=status,
                input_config=response.get("input_config", {}),
                success=success,
                results=results,
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
            steps_data: Dictionary of step data with output_url fields

        Returns:
            Dictionary with downloaded results for each step
        """
        results = {}

        for step_name, step_info in steps_data.items():
            output_url = step_info.get("output_url")
            if output_url:
                try:
                    # Download from presigned URL
                    import aiohttp
                    async with aiohttp.ClientSession() as session:
                        async with session.get(output_url) as resp:
                            if resp.status == 200:
                                content_type = resp.headers.get('Content-Type', '')
                                if 'json' in content_type:
                                    results[step_name] = await resp.json()
                                else:
                                    results[step_name] = await resp.text()
                            else:
                                results[step_name] = {
                                    "error": f"Failed to download: HTTP {resp.status}",
                                    "output_url": output_url
                                }
                except Exception as e:
                    results[step_name] = {
                        "error": f"Download failed: {str(e)}",
                        "output_url": output_url
                    }
            else:
                # Keep the step info if no URL available
                results[step_name] = step_info

        return results


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
        options: Optional[ProcessingOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> ConversionResult:
        """Convert a document using the marker endpoint (sync version)"""
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
