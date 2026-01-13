"""Main NX1 client that combines all service clients."""

import logging
import os
from typing import Any, Dict, Optional

from nx1_sdk.base import BaseClient
from nx1_sdk.exceptions import NX1ValidationError
from nx1_sdk.profiles import resolve_config
from nx1_sdk.services import (
    AppsClient,
    CrewsClient,
    DataConsumerClient,
    DataEngineeringClient,
    DataIngestionClient,
    DataMirroringClient,
    DataProductsClient,
    DataQualityClient,
    DataSharesClient,
    FilesClient,
    HealthClient,
    JobsClient,
    MetastoreClient,
    QueriesClient,
    S3Client,
    WorkerClient,
)


class NX1Client:
    """
    NX1 NLP Agent SDK Client
    
    A comprehensive client for interacting with all NX1 API endpoints.
    
    Configuration priority (highest to lowest):
    1. Explicit parameters (api_key, host)
    2. Environment variables (NX1_API_KEY, NX1_HOST)
    3. Profile configuration (~/.nx1/profiles)
    
    Usage:
        from nx1_sdk import NX1Client, IngestMode, ColumnTransformation, SparkDataType
        
        # Using explicit credentials
        client = NX1Client(
            api_key="your-psk-key",
            host="https://aiapi.example.nx1cloud.com"
        )
        
        # Using environment variables
        client = NX1Client()
        
        # Using a named profile
        client = NX1Client(profile="dev")
        
        # Health check
        health = client.health.ping()
        
        # Ingest local file (complete pipeline)
        job_id = client.ingestion.ingest_local_file(
            file_path="/path/to/data.csv",
            table="customers",
            schema_name="staging",
            mode=IngestMode.OVERWRITE,
            column_transformations=[
                ColumnTransformation.cast("date_col", SparkDataType.DATE),
                ColumnTransformation.rename("old_name", "new_name"),
                ColumnTransformation.encrypt("ssn")
            ]
        )
    
    Attributes:
        health: Health check endpoints
        metastore: Catalog, schema, table, column, tag, domain endpoints
        queries: Natural language query endpoints
        data_engineering: Data engineering pipeline endpoints
        ingestion: Data ingestion with file upload pipeline
        mirroring: Data mirroring job endpoints
        data_quality: Data quality rules and reports
        jobs: Job management endpoints
        files: File upload and management
        s3: S3 bucket management
        data_products: Data product and PSK management
        data: Data consumption from views
        apps: App management (versions, roles, components)
        crews: AI crew orchestration
        data_shares: Data sharing endpoints
        worker: Celery worker task endpoints
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        host: Optional[str] = None,
        profile: Optional[str] = None,
        verify_ssl: Optional[bool] = None,
        timeout: Optional[int] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the NX1 client.
        
        Configuration is resolved in this priority order:
        1. Explicit parameters (api_key, host, verify_ssl, timeout)
        2. Environment variables (NX1_API_KEY, NX1_HOST)
        3. Profile configuration (~/.nx1/profiles)
        
        Args:
            api_key: API key (PSK) for authentication.
            host: API host URL.
            profile: Profile name to load from ~/.nx1/profiles.
                     If not specified and no explicit credentials provided,
                     will try to use 'default' profile if it exists.
            verify_ssl: Whether to verify SSL certificates. Default True.
            timeout: Request timeout in seconds. Default 30.
            logger: Custom logger instance.
        
        Raises:
            NX1ValidationError: If api_key or host cannot be resolved.
        """
        # Resolve configuration from args, env vars, and profiles
        config = resolve_config(
            api_key=api_key,
            host=host,
            profile=profile,
            verify_ssl=verify_ssl,
            timeout=timeout
        )
        
        self.api_key = config["api_key"]
        self.host = config["host"]
        self.profile = profile
        
        if not self.api_key:
            raise NX1ValidationError(
                "API key required. Provide via: api_key parameter, NX1_API_KEY env var, "
                "or profile in ~/.nx1/profiles"
            )
        
        if not self.host:
            raise NX1ValidationError(
                "Host URL required. Provide via: host parameter, NX1_HOST env var, "
                "or profile in ~/.nx1/profiles"
            )
        
        # Initialize base HTTP client
        self._client = BaseClient(
            api_key=self.api_key,
            host=self.host,
            verify_ssl=config["verify_ssl"],
            timeout=config["timeout"],
            logger=logger
        )
        
        # Initialize service clients
        self.health = HealthClient(self._client)
        self.metastore = MetastoreClient(self._client)
        self.queries = QueriesClient(self._client)
        self.data_engineering = DataEngineeringClient(self._client)
        self.jobs = JobsClient(self._client)
        self.files = FilesClient(self._client)
        self.ingestion = DataIngestionClient(self._client)
        self.mirroring = DataMirroringClient(self._client)
        self.data_quality = DataQualityClient(self._client)
        self.s3 = S3Client(self._client)
        self.data_products = DataProductsClient(self._client)
        self.data = DataConsumerClient(self._client)
        self.apps = AppsClient(self._client)
        self.crews = CrewsClient(self._client)
        self.data_shares = DataSharesClient(self._client)
        self.worker = WorkerClient(self._client)
        
        # Set dependent clients for ingestion pipeline
        self.ingestion._set_clients(self.files, self.jobs)
    
    @property
    def base_url(self) -> str:
        """Get the base URL."""
        return self._client.base_url
    
    def ping(self) -> Dict[str, Any]:
        """Quick health check."""
        return self.health.ping()


def create_client(
    api_key: Optional[str] = None,
    host: Optional[str] = None,
    profile: Optional[str] = None,
    **kwargs
) -> NX1Client:
    """
    Create an NX1 client instance.
    
    This is a convenience function that creates an NX1Client instance.
    
    Args:
        api_key: API key (PSK) for authentication.
        host: API host URL.
        profile: Profile name to load from ~/.nx1/profiles.
        **kwargs: Additional arguments passed to NX1Client.
    
    Returns:
        NX1Client instance.
    """
    return NX1Client(api_key=api_key, host=host, profile=profile, **kwargs)
