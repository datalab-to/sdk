"""
Datalab API client using httpx - async and sync clients.
"""

import asyncio
import mimetypes
import httpx
from pathlib import Path
from typing import Union, Optional, Dict, Any, Tuple
import time

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


class BaseClient:
    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = settings.DATALAB_HOST,
        timeout: int = 300,
    ):
        """
        Initialize the Datalab client.

        Args:
            api_key: Your Datalab API key.
            base_url: Base URL for the API (default: https://www.datalab.to).
            timeout: Default timeout for requests in seconds.
        """
        if api_key is None:
            api_key = settings.DATALAB_API_KEY
        if api_key is None:
            raise DatalabAPIError("You must pass in an api_key or set DATALAB_API_KEY.")

        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _prepare_file_data(self, file_path: Union[str, Path]) -> Tuple[str, bytes, str]:
        """Prepare file data for upload."""
        file_path = Path(file_path)
        if not file_path.exists():
            raise DatalabFileError(f"File not found: {file_path}")

        mime_type, _ = mimetypes.guess_type(str(file_path))
        if not mime_type:
            extension = file_path.suffix.lower()
            mime_type = MIMETYPE_MAP.get(extension, "application/octet-stream")

        return file_path.name, file_path.read_bytes(), mime_type

    def get_form_params(
        self, file_path: Union[str, Path], options: ProcessingOptions
    ) -> Tuple[Dict[str, Any], Dict[str, Tuple[str, bytes, str]]]:
        """Prepare form data for httpx, separating fields and file."""
        filename, file_data, mime_type = self._prepare_file_data(file_path)

        files = {"file": (filename, file_data, mime_type)}
        data = options.to_form_data()

        # The options.to_form_data() method returns a dictionary where values
        # might be tuples. httpx expects a simple dictionary for the `data`
        # parameter. We'll extract the string value.
        cleaned_data = {
            key: str(value[1]) if isinstance(value, tuple) else str(value)
            for key, value in data.items()
        }
        return cleaned_data, files


class AsyncDatalabClient(BaseClient):
    """Asynchronous client for Datalab API using httpx."""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = settings.DATALAB_HOST,
        timeout: int = 300,
    ):
        """
        Initialize the async Datalab client.

        Args:
            api_key: Your Datalab API key.
            base_url: Base URL for the API (default: https://www.datalab.to).
            timeout: Default timeout for requests in seconds.
        """
        super().__init__(api_key, base_url, timeout)
        
        self._client: httpx.AsyncClient | None = None

    async def _ensure_client(self):
        """Ensure httpx client is created."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                headers={
                    "X-Api-Key": self.api_key,
                    "User-Agent": f"datalab-python-sdk/{settings.VERSION}",
                },
                base_url=self.base_url,
                timeout=self.timeout,
            )

    async def close(self):
        """Close the httpx client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def _make_request(
        self, method: str, endpoint: str, **kwargs
    ) -> Dict[str, Any]:
        """Make an async request to the API."""
        await self._ensure_client()
        try:
            response = await self._client.request(method, endpoint, **kwargs)
            response.raise_for_status()
            return response.json()
        except httpx.TimeoutException as e:
            raise DatalabTimeoutError(
                f"Request timed out after {self.timeout} seconds"
            ) from e
        except httpx.HTTPStatusError as e:
            try:
                error_data = e.response.json()
                error_message = error_data.get("error", str(e))
            except Exception:
                error_message = str(e)
            raise DatalabAPIError(
                error_message,
                e.response.status_code,
                error_data if "error_data" in locals() else None,
            ) from e
        except httpx.RequestError as e:
            raise DatalabAPIError(f"Request failed: {str(e)}") from e

    async def _poll_result(
        self, check_url: str, max_polls: int = 300, poll_interval: int = 1
    ) -> Dict[str, Any]:
        """Poll for result completion."""
        for i in range(max_polls):
            data = await self._make_request("GET", check_url)

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

    
    async def convert(
        self,
        file_path: Union[str, Path],
        options: Optional[ConvertOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
    ) -> ConversionResult:
        """Convert a document using the marker endpoint."""
        if options is None:
            options = ConvertOptions()

        data, files = self.get_form_params(file_path, options)

        initial_data = await self._make_request(
            "POST", "/api/v1/marker", data=data, files=files
        )

        if not initial_data.get("success"):
            raise DatalabAPIError(
                f"Request failed: {initial_data.get('error', 'Unknown error')}"
            )

        result_data = await self._poll_result(initial_data["request_check_url"])

        result = ConversionResult(
            success=result_data.get("success", False),
            output_format=result_data.get("output_format", options.output_format),
            markdown=result_data.get("markdown"),
            html=result_data.get("html"),
            json=result_data.get("json"),
            images=result_data.get("images"),
            metadata=result_data.get("metadata"),
            error=result_data.get("error"),
            page_count=result_data.get("page_count"),
            status=result_data.get("status", "complete"),
        )

        if save_output and result.success:
            output_path = Path(save_output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            result.save_output(output_path)

        return result

    async def ocr(
        self,
        file_path: Union[str, Path],
        options: Optional[OCROptions] = None,
        save_output: Optional[Union[str, Path]] = None,
    ) -> OCRResult:
        """Perform OCR on a document."""
        if options is None:
            options = OCROptions()

        data, files = self.get_form_params(file_path, options)

        initial_data = await self._make_request(
            "POST", "/api/v1/ocr", data=data, files=files
        )

        if not initial_data.get("success"):
            raise DatalabAPIError(
                f"Request failed: {initial_data.get('error', 'Unknown error')}"
            )

        result_data = await self._poll_result(initial_data["request_check_url"])

        result = OCRResult(
            success=result_data.get("success", False),
            pages=result_data.get("pages", []),
            error=result_data.get("error"),
            page_count=result_data.get("page_count"),
            status=result_data.get("status", "complete"),
        )

        if save_output and result.success:
            output_path = Path(save_output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            result.save_output(output_path)

        return result


class DatalabClient(BaseClient):
    """Synchronous client for Datalab API using httpx."""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = settings.DATALAB_HOST,
        timeout: int = 300,
    ):
        """
        Initialize the Datalab client.

        Args:
            api_key: Your Datalab API key.
            base_url: Base URL for the API (default: https://www.datalab.to).
            timeout: Default timeout for requests in seconds.
        """
        super().__init__(api_key, base_url, timeout)
        self._client :httpx.Client | None = None 
        
    def _ensure_client(self):
        """Ensure httpx client is created."""
        if self._client is None:
            self._client = httpx.Client(
                headers={
                    "X-Api-Key": self.api_key,
                    "User-Agent": f"datalab-python-sdk/{settings.VERSION}",
                },
                base_url=self.base_url,
                timeout=self.timeout,
            )
        
    def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make a sync request to the API."""
        self._ensure_client()
        try:
            response = self._client.request(method, endpoint, **kwargs)
            response.raise_for_status()
            return response.json()
        except httpx.TimeoutException as e:
            raise DatalabTimeoutError(
                f"Request timed out after {self.timeout} seconds"
            ) from e
        except httpx.HTTPStatusError as e:
            try:
                error_data = e.response.json()
                error_message = error_data.get("error", str(e))
            except Exception:
                error_message = str(e)
            raise DatalabAPIError(
                error_message,
                e.response.status_code,
                error_data if "error_data" in locals() else None,
            ) from e
        except httpx.RequestError as e:
            raise DatalabAPIError(f"Request failed: {str(e)}") from e

    def _poll_result(
        self, check_url: str, max_polls: int = 300, poll_interval: int = 1
    ) -> Dict[str, Any]:
        """Poll for result completion."""
        for i in range(max_polls):
            data = self._make_request("GET", check_url)

            if data.get("status") == "complete":
                return data

            if not data.get("success", True) and not data.get("status") == "processing":
                raise DatalabAPIError(
                    f"Processing failed: {data.get('error', 'Unknown error')}"
                )

            time.sleep(poll_interval)

        raise DatalabTimeoutError(
            f"Polling timed out after {max_polls * poll_interval} seconds"
        )

    def convert(
        self,
        file_path: Union[str, Path],
        options: Optional[ConvertOptions] = None,
        save_output: Optional[Union[str, Path]] = None,
    ) -> ConversionResult:
        """Convert a document using the marker endpoint (sync version)."""
        if options is None:
            options = ConvertOptions()

        data, files = self.get_form_params(file_path, options)

        initial_data = self._make_request(
            "POST", "/api/v1/marker", data=data, files=files
        )

        if not initial_data.get("success"):
            raise DatalabAPIError(
                f"Request failed: {initial_data.get('error', 'Unknown error')}"
            )

        result_data = self._poll_result(initial_data["request_check_url"])

        result = ConversionResult(
            success=result_data.get("success", False),
            output_format=result_data.get("output_format", options.output_format),
            markdown=result_data.get("markdown"),
            html=result_data.get("html"),
            json=result_data.get("json"),
            images=result_data.get("images"),
            metadata=result_data.get("metadata"),
            error=result_data.get("error"),
            page_count=result_data.get("page_count"),
            status=result_data.get("status", "complete"),
        )

        if save_output and result.success:
            output_path = Path(save_output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            result.save_output(output_path)

        return result

    def ocr(
        self,
        file_path: Union[str, Path],
        options: Optional[OCROptions] = None,
        save_output: Optional[Union[str, Path]] = None,
    ) -> OCRResult:
        """Perform OCR on a document (sync version)."""
        if options is None:
            options = OCROptions()

        data, files = self.get_form_params(file_path, options)

        initial_data = self._make_request("POST", "/api/v1/ocr", data=data, files=files)

        if not initial_data.get("success"):
            raise DatalabAPIError(
                f"Request failed: {initial_data.get('error', 'Unknown error')}"
            )

        result_data = self._poll_result(initial_data["request_check_url"])

        result = OCRResult(
            success=result_data.get("success", False),
            pages=result_data.get("pages", []),
            error=result_data.get("error"),
            page_count=result_data.get("page_count"),
            status=result_data.get("status", "complete"),
        )

        if save_output and result.success:
            output_path = Path(save_output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            result.save_output(output_path)

        return result