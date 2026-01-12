"""Base HTTP client for the NX1 SDK."""

import json
import logging
import warnings
from typing import Any, Dict, Optional
from urllib.parse import urljoin, urlparse

import requests
from urllib3.exceptions import InsecureRequestWarning

from nx1_sdk.exceptions import NX1APIError, NX1TimeoutError


class BaseClient:
    """Base HTTP client with common functionality for all API requests."""
    
    def __init__(
        self,
        api_key: str,
        host: str,
        verify_ssl: bool = True,
        timeout: int = 30,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the base HTTP client.
        
        Args:
            api_key: API key (PSK) for authentication
            host: API host URL
            verify_ssl: Whether to verify SSL certificates
            timeout: Request timeout in seconds
            logger: Optional custom logger instance
        """
        # Ensure host has https:// prefix
        if not urlparse(host).scheme:
            host = f"https://{host}"
        elif urlparse(host).scheme == 'http':
            host = host.replace('http://', 'https://', 1)
        
        self.base_url = host.rstrip('/') + '/'
        self.api_key = api_key
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.logger = logger or logging.getLogger(__name__)
        
        self.headers = {
            "Authorization-PSK": api_key,
            "Content-Type": "application/json"
        }
        
        if not self.verify_ssl:
            warnings.filterwarnings('ignore', category=InsecureRequestWarning)
    
    def _build_url(self, *path_components) -> str:
        """Build URL by properly joining path components."""
        path = '/'.join(str(c).strip('/') for c in path_components if c)
        return urljoin(self.base_url, path)
    
    def _request(
        self,
        method: str,
        *path_components,
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        data: Any = None,
        files: Optional[Dict] = None,
        headers: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Make HTTP request and handle response.
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE, PATCH)
            *path_components: URL path components to join
            params: Query parameters
            json_data: JSON body data
            data: Form data
            files: Files to upload
            headers: Additional headers
        
        Returns:
            Parsed JSON response or empty dict
        
        Raises:
            NX1APIError: On HTTP errors
            NX1TimeoutError: On request timeout
        """
        url = self._build_url(*path_components)
        request_headers = {**self.headers}
        
        if headers:
            request_headers.update(headers)
        
        # Remove Content-Type for file uploads (let requests set multipart boundary)
        if files:
            request_headers.pop("Content-Type", None)
        
        self.logger.debug(f"{method} {url}")
        
        try:
            response = requests.request(
                method=method,
                url=url,
                params=params,
                json=json_data,
                data=data,
                files=files,
                headers=request_headers,
                verify=self.verify_ssl,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            
            # Handle empty responses
            if not response.content:
                return {}
            
            # Try to parse JSON
            try:
                return response.json()
            except json.JSONDecodeError:
                return {"text": response.text}
                
        except requests.exceptions.HTTPError as e:
            error_detail = None
            try:
                error_detail = e.response.json()
            except Exception:
                error_detail = {"text": e.response.text if e.response else str(e)}
            
            raise NX1APIError(
                f"HTTP {e.response.status_code}: {error_detail}",
                status_code=e.response.status_code if e.response else None,
                response=error_detail
            )
        except requests.exceptions.Timeout:
            raise NX1TimeoutError(f"Request timed out after {self.timeout}s")
        except requests.exceptions.RequestException as e:
            raise NX1APIError(f"Request failed: {str(e)}")
    
    def get(self, *path, **kwargs) -> Dict:
        """Make GET request."""
        return self._request("GET", *path, **kwargs)
    
    def post(self, *path, **kwargs) -> Dict:
        """Make POST request."""
        return self._request("POST", *path, **kwargs)
    
    def put(self, *path, **kwargs) -> Dict:
        """Make PUT request."""
        return self._request("PUT", *path, **kwargs)
    
    def delete(self, *path, **kwargs) -> Dict:
        """Make DELETE request."""
        return self._request("DELETE", *path, **kwargs)
    
    def patch(self, *path, **kwargs) -> Dict:
        """Make PATCH request."""
        return self._request("PATCH", *path, **kwargs)
