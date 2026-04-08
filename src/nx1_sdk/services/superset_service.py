"""Superset service client that calls the NX1 Analytics API endpoints."""

import logging
from typing import Any, Dict, List, Optional

import requests

from nx1_sdk.exceptions import NX1APIError, NX1ValidationError


class SupersetClient:
    """
    Client for the NX1 Analytics API (backed by Apache Superset).

    Usage:
        from nx1_sdk.services.superset_service import SupersetClient

        client = SupersetClient(
            host="https://api.nx1cloud.com",
            token="<bearer-token>",
        )

        dashboards = client.get_dashboards()
        datasets   = client.get_datasets()
        databases  = client.get_databases()
        info       = client.get_dataset_info()
        result     = client.execute_sql(sql="show catalogs;", database_id=1)
    """

    def __init__(
        self,
        host: str,
        token: str,
        verify_ssl: bool = True,
        timeout: int = 30,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize the Superset client.

        Args:
            host: Base URL of the NX1 API (e.g. ``https://api.nx1cloud.com``).
            token: Bearer token for authentication.
            verify_ssl: Whether to verify SSL certificates.
            timeout: Request timeout in seconds.
            logger: Optional custom logger instance.

        Raises:
            NX1ValidationError: If host or token are missing.
        """
        if not host:
            raise NX1ValidationError("host is required.")
        if not token:
            raise NX1ValidationError("token is required.")

        self.logger = logger or logging.getLogger(__name__)
        self.timeout = timeout
        self._verify_ssl = verify_ssl
        self._base_url = host.rstrip("/") + "/api/analytics"

        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })

    # ------------------------------------------------------------------
    # Database endpoints
    # ------------------------------------------------------------------

    def get_databases(self) -> List[Dict[str, Any]]:
        """GET /api/analytics/databases — list all Superset databases."""
        return self._get("/databases")

    # ------------------------------------------------------------------
    # Dashboard endpoints
    # ------------------------------------------------------------------

    def get_dashboards(self) -> List[Dict[str, Any]]:
        """GET /api/analytics/dashboards — list all Superset dashboards."""
        return self._get("/dashboards")

    def get_dashboard(self, dashboard_id: int) -> Dict[str, Any]:
        """GET /api/analytics/dashboards/{dashboard_id} — get a dashboard by ID."""
        return self._get(f"/dashboards/{dashboard_id}")

    # ------------------------------------------------------------------
    # Dataset endpoints
    # ------------------------------------------------------------------

    def get_datasets(self) -> List[Dict[str, Any]]:
        """GET /api/analytics/datasets — list all Superset datasets."""
        return self._get("/datasets")

    def get_dataset_info(self) -> Dict[str, Any]:
        """GET /api/analytics/datasets/info — dataset resource metadata."""
        return self._get("/datasets/info")

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
    ) -> Dict[str, Any]:
        """
        POST /api/analytics/sql — execute a SQL statement via SQLLab.

        Args:
            sql: SQL statement to execute.
            database_id: Superset database ID to run against.
            schema: Optional schema name.
            catalog: Optional catalog name.
            query_limit: Max rows to return.

        Returns:
            Dict with ``query_id``, ``status``, ``columns``, and ``data`` keys.
        """
        payload: Dict[str, Any] = {
            "sql": sql,
            "database_id": database_id,
            "query_limit": query_limit,
        }
        if schema is not None:
            payload["schema"] = schema
        if catalog is not None:
            payload["catalog"] = catalog

        return self._post("/sql", payload)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, path: str) -> Any:
        url = self._base_url + path
        try:
            response = self._session.get(url, timeout=self.timeout, verify=self._verify_ssl)
            self._raise_for_status(response)
            return response.json()
        except NX1APIError:
            raise
        except Exception as e:
            raise NX1APIError(f"GET {path} failed: {e}")

    def _post(self, path: str, payload: Dict[str, Any]) -> Any:
        url = self._base_url + path
        try:
            response = self._session.post(url, json=payload, timeout=self.timeout, verify=self._verify_ssl)
            self._raise_for_status(response)
            return response.json()
        except NX1APIError:
            raise
        except Exception as e:
            raise NX1APIError(f"POST {path} failed: {e}")

    def _raise_for_status(self, response: requests.Response) -> None:
        if response.ok:
            return
        try:
            detail = response.json().get("detail", response.text)
        except Exception:
            detail = response.text
        raise NX1APIError(
            f"Analytics API error {response.status_code}: {detail}",
            status_code=response.status_code,
        )
