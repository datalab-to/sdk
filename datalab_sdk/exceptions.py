"""
Datalab SDK exceptions
"""

from typing import Any, Optional


class DatalabError(Exception):
    """Base exception for Datalab SDK errors"""

    pass


class DatalabAPIError(DatalabError):
    """Exception raised when the API returns an error response"""

    def __init__(
        self, message: str, status_code: Optional[int] = None, response_data: Optional[dict] = None, details: Optional[Any] = None
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data
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
