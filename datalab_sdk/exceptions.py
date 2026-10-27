"""
Datalab SDK exceptions
"""

from typing import Any, Optional


class DatalabError(Exception):
    """Base exception for Datalab SDK errors"""

    pass


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
    """Exception raised when a request times out"""

    pass


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
