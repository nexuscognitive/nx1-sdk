"""Custom exceptions for the NX1 SDK."""

from typing import Any, Dict, Optional


class NX1Error(Exception):
    """Base exception for NX1 SDK errors."""
    pass


class NX1APIError(NX1Error):
    """API request error with status code and response details."""
    
    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response: Optional[Dict[str, Any]] = None
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response = response
    
    def __str__(self) -> str:
        base = super().__str__()
        if self.status_code:
            return f"[HTTP {self.status_code}] {base}"
        return base


class NX1ValidationError(NX1Error):
    """Validation error for invalid inputs or configuration."""
    pass


class NX1TimeoutError(NX1Error):
    """Timeout error when requests or jobs exceed time limits."""
    pass
