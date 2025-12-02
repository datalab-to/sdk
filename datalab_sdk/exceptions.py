"""
Datalab SDK exceptions
"""

from typing import Any


class DatalabError(Exception):
    """Base exception for Datalab SDK errors"""

    pass


class DatalabAPIError(DatalabError):
    """Exception raised when the API returns an error response"""

    def __init__(
        self, message: str, status_code: int = None, response_data: dict = None, details: Any = None
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data
        # Store error details (similar to frontend's MarkerAPIError.details)
        # This can be a string (raw error text) or the error field from JSON response
        self.details = details


class DatalabTimeoutError(DatalabError):
    """Exception raised when a request times out"""

    pass


class DatalabFileError(DatalabError):
    """Exception raised when there's an issue with file operations"""

    pass


class DatalabValidationError(DatalabError):
    """Exception raised when input validation fails"""

    pass
