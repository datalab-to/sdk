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

    def process_collection(
        self,
        collection: "Collection",
        method: str = "convert",
        options: Optional[ProcessingOptions] = None,
        output_dir: Optional[Union[str, Path]] = None,
        max_concurrent: int = 5,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> "CollectionResult":
        """Process a collection of documents (sync version)"""
        return self._run_async(
            self._process_collection_async(
                collection=collection,
                method=method,
                options=options,
                output_dir=output_dir,
                max_concurrent=max_concurrent,
                max_polls=max_polls,
                poll_interval=poll_interval,
            )
        )

    async def _process_collection_async(
        self,
        collection: "Collection",
        method: str = "convert",
        options: Optional[ProcessingOptions] = None,
        output_dir: Optional[Union[str, Path]] = None,
        max_concurrent: int = 5,
        max_polls: int = 300,
        poll_interval: int = 1,
    ) -> "CollectionResult":
        """Internal async method for processing collections"""
        if method == "convert":
            return await collection.convert_all(
                client=self._async_client,
                options=options,
                output_dir=output_dir,
                max_concurrent=max_concurrent,
                max_polls=max_polls,
                poll_interval=poll_interval,
            )
        elif method == "ocr":
            return await collection.ocr_all(
                client=self._async_client,
                options=options,
                output_dir=output_dir,
                max_concurrent=max_concurrent,
                max_polls=max_polls,
                poll_interval=poll_interval,
            )
        else:
            raise ValueError(f"Unsupported method: {method}. Use 'convert' or 'ocr'")
