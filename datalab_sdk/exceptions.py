"""
Datalab SDK exceptions
"""


class DatalabError(Exception):
    """Base exception for Datalab SDK errors"""

    pass


class DatalabAPIError(DatalabError):
    """Exception raised when the API returns an error response"""

    def __init__(
        self, message: str, status_code: int = None, response_data: dict = None
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data

    @property
    def details(self):
        """
        Get error details from the API response.
        
        Returns the 'error' field from response_data if available,
        otherwise returns response_data itself, or None if no response data exists.
        This matches the behavior of MarkerAPIError.details in the frontend.
        """
        if self.response_data is None:
            return None
        # Return the 'error' field if it exists, otherwise return the full response_data
        return self.response_data.get("error", self.response_data)


class DatalabTimeoutError(DatalabError):
    """Exception raised when a request times out"""

    pass


class DatalabFileError(DatalabError):
    """Exception raised when there's an issue with file operations"""

    pass


class DatalabValidationError(DatalabError):
    """Exception raised when input validation fails"""

    pass
