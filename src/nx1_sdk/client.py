"""Main NX1 client that combines all service clients."""

import logging
import os
from typing import Any, Dict, Optional

from nx1_sdk.base import BaseClient
from nx1_sdk.exceptions import NX1ValidationError
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
    
    Usage:
        from nx1_sdk import NX1Client, IngestMode, ColumnTransformation, SparkDataType
        
        client = NX1Client(
            api_key="your-psk-key",
            host="https://aiapi.example.nx1cloud.com"
        )
        
        # Health check
        health = client.health.ping()
        
        # Ask a question
        response = client.queries.ask(
            domain="Sales Data",
            prompt="Show me top 10 customers"
        )
        
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
        verify_ssl: bool = True,
        timeout: int = 30,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the NX1 client.
        
        Args:
            api_key: API key (PSK) for authentication. 
                     Falls back to NX1_API_KEY or LAKEHOUSE_API_KEY env vars.
            host: API host URL. 
                  Falls back to NX1_HOST or LAKEHOUSE_HOST env vars.
            verify_ssl: Whether to verify SSL certificates. Default True.
            timeout: Request timeout in seconds. Default 30.
            logger: Custom logger instance.
        
        Raises:
            NX1ValidationError: If api_key or host is not provided and not in env vars.
        """
        self.api_key = (
            api_key 
            or os.environ.get('NX1_API_KEY') 
            or os.environ.get('LAKEHOUSE_API_KEY')
        )
        self.host = (
            host 
            or os.environ.get('NX1_HOST') 
            or os.environ.get('LAKEHOUSE_HOST')
        )
        
        if not self.api_key:
            raise NX1ValidationError(
                "API key required. Set NX1_API_KEY environment variable or pass api_key parameter."
            )
        
        if not self.host:
            raise NX1ValidationError(
                "Host URL required. Set NX1_HOST environment variable or pass host parameter."
            )
        
        # Initialize base HTTP client
        self._client = BaseClient(
            api_key=self.api_key,
            host=self.host,
            verify_ssl=verify_ssl,
            timeout=timeout,
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
    **kwargs
) -> NX1Client:
    """
    Create an NX1 client instance.
    
    This is a convenience function that creates an NX1Client instance.
    Environment variables NX1_API_KEY and NX1_HOST are used if not provided.
    
    Args:
        api_key: API key (PSK) for authentication.
        host: API host URL.
        **kwargs: Additional arguments passed to NX1Client.
    
    Returns:
        NX1Client instance.
    """
    return NX1Client(api_key=api_key, host=host, **kwargs)
