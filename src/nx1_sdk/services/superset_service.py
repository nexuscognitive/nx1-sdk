"""Superset service client for Apache Superset API."""

import json
import logging
import warnings
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from urllib3.exceptions import InsecureRequestWarning

from nx1_sdk.exceptions import NX1APIError, NX1TimeoutError, NX1ValidationError


class SupersetClient:
    """
    Client for Apache Superset REST API.

    Authenticates via username/password to obtain a Bearer token,
    then fetches a CSRF token for state-changing requests.

    Usage:
        from nx1_sdk.services.superset_service import SupersetClient

        client = SupersetClient(
            host="https://superset-rapid.rapid.nx1cloud.com",
            username="trino-admin",
            password="secret",
        )

        dashboards = client.get_dashboards()
        datasets   = client.get_datasets()
        databases  = client.get_databases()
        result     = client.execute_sql(database_id=1, sql="show catalogs;")
    """

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        provider: str = "db",
        verify_ssl: bool = True,
        timeout: int = 30,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize and authenticate the Superset client.

        Args:
            host: Base URL of the Superset instance.
            username: Superset username.
            password: Superset password.
            provider: Auth provider, default ``db``.
            verify_ssl: Whether to verify SSL certificates.
            timeout: Request timeout in seconds.
            logger: Optional custom logger instance.

        Raises:
            NX1ValidationError: If host/username/password are missing.
            NX1APIError: If authentication fails.
        """
        if not host:
            raise NX1ValidationError("Superset host is required.")
        if not username:
            raise NX1ValidationError("Superset username is required.")
        if not password:
            raise NX1ValidationError("Superset password is required.")

        parsed = urlparse(host)
        if not parsed.scheme:
            host = f"https://{host}"
        elif parsed.scheme == "http":
            host = host.replace("http://", "https://", 1)

        self.base_url = host.rstrip("/") + "/"
        self.username = username
        self.password = password
        self.provider = provider
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.logger = logger or logging.getLogger(__name__)

        if not self.verify_ssl:
            warnings.filterwarnings("ignore", category=InsecureRequestWarning)

        self._access_token: Optional[str] = None
        self._csrf_token: Optional[str] = None

        # Authenticate on init
        self._login()

    # ------------------------------------------------------------------
    # Auth helpers
    # ------------------------------------------------------------------

    def _login(self) -> None:
        """POST /api/v1/security/login and store the access token."""
        url = self._url("api/v1/security/login")
        payload = {
            "username": self.username,
            "password": self.password,
            "provider": self.provider,
            "refresh": True,
        }
        self.logger.debug("Superset login: POST %s", url)
        try:
            response = requests.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                verify=self.verify_ssl,
                timeout=self.timeout,
            )
            response.raise_for_status()
            data = response.json()
            self._access_token = data["access_token"]
            self.logger.debug("Superset login successful.")
        except requests.exceptions.HTTPError as e:
            detail = _safe_json(e.response)
            raise NX1APIError(
                f"Superset login failed HTTP {e.response.status_code}: {detail}",
                status_code=e.response.status_code if e.response else None,
                response=detail,
            )
        except requests.exceptions.Timeout:
            raise NX1TimeoutError(f"Superset login timed out after {self.timeout}s")
        except requests.exceptions.RequestException as e:
            raise NX1APIError(f"Superset login request failed: {e}")

    def _fetch_csrf_token(self) -> str:
        """GET /api/v1/security/csrf_token/ and return the token."""
        url = self._url("api/v1/security/csrf_token/")
        self.logger.debug("Fetching CSRF token: GET %s", url)
        try:
            response = requests.get(
                url,
                headers=self._auth_headers(),
                verify=self.verify_ssl,
                timeout=self.timeout,
            )
            response.raise_for_status()
            data = response.json()
            token = data.get("result") or data.get("csrf_token") or data.get("token")
            if not token:
                raise NX1APIError(f"CSRF token not found in response: {data}")
            self._csrf_token = token
            self.logger.debug("CSRF token obtained.")
            return token
        except requests.exceptions.HTTPError as e:
            detail = _safe_json(e.response)
            raise NX1APIError(
                f"CSRF token fetch failed HTTP {e.response.status_code}: {detail}",
                status_code=e.response.status_code if e.response else None,
                response=detail,
            )
        except requests.exceptions.Timeout:
            raise NX1TimeoutError(f"CSRF token fetch timed out after {self.timeout}s")
        except requests.exceptions.RequestException as e:
            raise NX1APIError(f"CSRF token request failed: {e}")

    def get_csrf_token(self) -> str:
        """Return cached CSRF token, fetching it first if needed."""
        if not self._csrf_token:
            self._fetch_csrf_token()
        return self._csrf_token  # type: ignore[return-value]

    def _url(self, *path_parts: str) -> str:
        path = "/".join(p.strip("/") for p in path_parts if p)
        return urljoin(self.base_url, path)

    def _auth_headers(self, include_csrf: bool = False) -> Dict[str, str]:
        headers: Dict[str, str] = {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if include_csrf:
            headers["X-CSRFToken"] = self.get_csrf_token()
        return headers

    def _request(
        self,
        method: str,
        *path_parts: str,
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        include_csrf: bool = False,
    ) -> Any:
        url = self._url(*path_parts)
        headers = self._auth_headers(include_csrf=include_csrf)
        self.logger.debug("%s %s", method, url)
        try:
            response = requests.request(
                method=method,
                url=url,
                params=params,
                json=json_data,
                headers=headers,
                verify=self.verify_ssl,
                timeout=self.timeout,
            )
            response.raise_for_status()
            if not response.content:
                return {}
            try:
                return response.json()
            except json.JSONDecodeError:
                return {"text": response.text}
        except requests.exceptions.HTTPError as e:
            detail = _safe_json(e.response)
            raise NX1APIError(
                f"HTTP {e.response.status_code}: {detail}",
                status_code=e.response.status_code if e.response else None,
                response=detail,
            )
        except requests.exceptions.Timeout:
            raise NX1TimeoutError(f"Request timed out after {self.timeout}s")
        except requests.exceptions.RequestException as e:
            raise NX1APIError(f"Request failed: {e}")

    # ------------------------------------------------------------------
    # Dashboard endpoints
    # ------------------------------------------------------------------

    def get_dashboards(self) -> Dict[str, Any]:
        """GET /api/v1/dashboard/ — list all dashboards."""
        return self._request("GET", "api/v1/dashboard/")

    def get_dashboard(self, dashboard_id: int) -> Dict[str, Any]:
        """GET /api/v1/dashboard/{id} — get a specific dashboard."""
        return self._request("GET", "api/v1/dashboard", str(dashboard_id))

    # ------------------------------------------------------------------
    # Dataset endpoints
    # ------------------------------------------------------------------

    def get_datasets(self) -> Dict[str, Any]:
        """GET /api/v1/dataset/ — list all datasets."""
        return self._request("GET", "api/v1/dataset/")

    def get_dataset_info(self) -> Dict[str, Any]:
        """GET /api/v1/dataset/_info — metadata about the dataset resource."""
        return self._request("GET", "api/v1/dataset/_info")

    # ------------------------------------------------------------------
    # Database endpoints
    # ------------------------------------------------------------------

    def get_databases(self) -> Dict[str, Any]:
        """GET /api/v1/database/ — list all databases."""
        return self._request("GET", "api/v1/database/")

    # ------------------------------------------------------------------
    # SQL execution
    # ------------------------------------------------------------------

    def execute_sql(
        self,
        sql: str,
        database_id: int = 1,
        schema: Optional[str] = None,
        catalog: Optional[str] = None,
        query_limit: int = 1000,
        run_async: bool = False,
        sql_editor_id: str = "11",
        tab: str = "Untitled Query 1",
    ) -> Dict[str, Any]:
        """
        POST /api/v1/sqllab/execute/ — run a SQL statement.

        Requires both Bearer token (Authorization header) and CSRF token
        (X-CSRFToken header).

        Args:
            sql: SQL statement to execute.
            database_id: Superset database ID to run against.
            schema: Optional schema name.
            catalog: Optional catalog name.
            query_limit: Max rows to return.
            run_async: Whether to run asynchronously.
            sql_editor_id: SQL editor session ID.
            tab: Tab label shown in the UI.

        Returns:
            JSON response with query results.
        """
        payload: Dict[str, Any] = {
            "catalog": catalog,
            "database_id": database_id,
            "schema": schema,
            "sql": sql,
            "json": True,
            "runAsync": run_async,
            "select_as_cta": False,
            "expand_data": True,
            "queryLimit": query_limit,
            "ctas_method": "TABLE",
            "sql_editor_id": sql_editor_id,
            "tab": tab,
            "tmp_table_name": "",
        }
        return self._request(
            "POST",
            "api/v1/sqllab/execute/",
            json_data=payload,
            include_csrf=True,
        )


# ------------------------------------------------------------------
# Internal helper
# ------------------------------------------------------------------

def _safe_json(response: Optional[requests.Response]) -> Any:
    if response is None:
        return {}
    try:
        return response.json()
    except Exception:
        return {"text": response.text}
