"""
Datalab SDK exceptions
"""

from typing import Any, Optional


class DatalabError(Exception):
    """Base exception for Datalab SDK errors.

    Attributes:
        request_id: The request ID, when a document-processing method or
            get_result() fails after the API accepted the request. Pass
            check_url to get_result() to fetch the result without submitting
            the document again. For run_pipeline(), the execution ID.
        check_url: The URL that returns the request's status and result.
    """

    request_id: Optional[str] = None
    check_url: Optional[str] = None


class DatalabAPIError(DatalabError):
    """Exception raised when the API returns an error response.

    Attributes:
        status_code: The HTTP status code, or None for a connection error.
        response_data: The parsed JSON error body, when the API sent one.
    """

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_data: Optional[Any] = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data


class DatalabAuthenticationError(DatalabAPIError):
    """The API rejected the API key (HTTP 401 or 403)."""

    pass


class DatalabPaymentRequiredError(DatalabAPIError):
    """The request needs payment (HTTP 402).

    The API sends this when the account is out of credit, when an API key
    is over its spend limit, or when a plan limit is reached.
    """

    pass


class DatalabNotFoundError(DatalabAPIError):
    """The resource does not exist or has expired (HTTP 404)."""

    pass


class DatalabRateLimitError(DatalabAPIError):
    """The API rate limit was hit (HTTP 429).

    The SDK retries a 429 with backoff before it raises this error.
    """

    pass


class DatalabServerError(DatalabAPIError):
    """The API had a server error (HTTP 5xx)."""

    pass


class DatalabTimeoutError(DatalabError):
    """A request or the polling budget timed out.

    When request_id is None, the submission itself timed out, and the API
    may still have accepted the request. Otherwise the subclass says what
    happened to the request:

    - DatalabPollingTimeoutError: the request is still running.
    - DatalabTransferTimeoutError: the result timed out while it downloaded.
    """


class DatalabTransferTimeoutError(DatalabTimeoutError):
    """The result timed out while it downloaded. The request has finished.

    The SDK does not retry this error: the timeout limits each whole
    request, so a result too large to download within it would time out
    again. Raise the client's timeout and call get_result().
    """


class DatalabPollingTimeoutError(DatalabTimeoutError):
    """The polling budget (max_polls * poll_interval) ran out before the
    request finished. The request keeps running on the server and is
    charged; call get_result() to wait for it again.

    Attributes:
        status: The status of the last poll, for example "processing".
        progress_percent: The estimated progress of the last poll, if any.
    """

    def __init__(
        self,
        message: str,
        status: Optional[str] = None,
        progress_percent: Optional[int] = None,
    ):
        super().__init__(message)
        self.status = status
        self.progress_percent = progress_percent


class DatalabFileError(DatalabError):
    """Exception raised when there's an issue with file operations"""

    pass


class DatalabValidationError(DatalabError, ValueError):
    """Exception raised when input validation fails.

    This class is also a ValueError, so existing `except ValueError`
    handlers continue to catch it.
    """

    pass


def api_error_for_status(
    message: str,
    status_code: Optional[int] = None,
    response_data: Optional[Any] = None,
) -> DatalabAPIError:
    """Return the DatalabAPIError subclass that matches an HTTP status code."""
    if status_code in (401, 403):
        cls = DatalabAuthenticationError
    elif status_code == 402:
        cls = DatalabPaymentRequiredError
    elif status_code == 404:
        cls = DatalabNotFoundError
    elif status_code == 429:
        cls = DatalabRateLimitError
    elif status_code is not None and status_code >= 500:
        cls = DatalabServerError
    else:
        cls = DatalabAPIError
    return cls(message, status_code, response_data)
