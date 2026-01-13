"""Service clients for each API endpoint group."""

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from nx1_sdk.base import BaseClient
from nx1_sdk.constants import CONTENT_TYPES, FILE_EXTENSION_MAP, FILE_FORMAT_OPTIONS
from nx1_sdk.enums import (
    AppComponentType,
    IngestMode,
    IngestType,
    JdbcType,
    JobStatus,
)
from nx1_sdk.exceptions import NX1APIError, NX1Error, NX1TimeoutError, NX1ValidationError


class HealthClient:
    """Health check endpoints."""
    
    def __init__(self, client: BaseClient):
        self._client = client
    
    def ping(self) -> Dict[str, Any]:
        """Health check for the API."""
        return self._client.get("api", "health", "ping")
    
    def version(self) -> str:
        """Get API version."""
        response = self.ping()
        return response.get("version", "unknown")
    
    def build_number(self) -> str:
        """Get API build number."""
        response = self.ping()
        return response.get("build_number", "unknown")


class MetastoreClient:
    """Metastore endpoints - catalogs, schemas, tables, columns, tags, domains."""
    
    def __init__(self, client: BaseClient):
        self._client = client
    
    def get_domains(self) -> Dict[str, Any]:
        """Get list of available domains."""
        return self._client.get("api", "metastore", "domains")
    
    def get_catalogs(self) -> Dict[str, Any]:
        """Get all catalogs."""
        return self._client.get("api", "metastore", "catalogs")
    
    def get_schemas(self, catalog: str) -> Dict[str, Any]:
        """Get schemas for a catalog."""
        return self._client.get("api", "metastore", "catalogs", catalog, "schemas")
    
    def get_tables(self, catalog: str, schema: str) -> Dict[str, Any]:
        """Get tables for a schema."""
        return self._client.get("api", "metastore", "catalogs", catalog, "schemas", schema, "tables")
    
    def get_columns(self, catalog: str, schema: str, table: str) -> Dict[str, Any]:
        """Get columns for a table."""
        return self._client.get("api", "metastore", "catalogs", catalog, "schemas", schema, "tables", table, "columns")
    
    def get_engines(self) -> Dict[str, Any]:
        """Get supported query engines."""
        return self._client.get("api", "metastore", "engines")
    
    def get_tags(self) -> List[str]:
        """Get all available tags from DataHub."""
        return self._client.get("api", "metastore", "tags")
    
    def set_table_tag(self, tag: str, table: str) -> Dict[str, Any]:
        """Assign a tag to a specific table."""
        return self._client.post("api", "metastore", "tags", tag, "tables", table)
    
    def delete_table_tag(self, tag: str, table: str) -> Dict[str, Any]:
        """Remove a tag from a specific table."""
        return self._client.delete("api", "metastore", "tags", tag, "tables", table)
    
    def set_table_domain(self, domain: str, table: str) -> Dict[str, Any]:
        """Assign a domain to a specific table."""
        return self._client.post("api", "metastore", "domains", domain, "tables", table)
    
    def delete_table_domain(self, domain: str, table: str) -> Dict[str, Any]:
        """Remove a domain from a specific table."""
        return self._client.delete("api", "metastore", "domains", domain, "tables", table)


class QueriesClient:
    """Query endpoints - ask, suggest, schedule."""
    
    def __init__(self, client: BaseClient):
        self._client = client
    
    def ask(self, domain: str, prompt: str) -> Dict[str, Any]:
        """Ask a natural language question about a data domain."""
        return self._client.post(
            "api", "query", "ask",
            json_data={"domain": domain, "prompt": prompt}
        )
    
    def suggest(self, domain: str) -> List[Dict[str, Any]]:
        """Generate suggested questions for a domain."""
        return self._client.post(
            "api", "query", "suggest",
            json_data={"domain": domain}
        )
    
    def schedule(
        self,
        name: str,
        cron: str,
        card_url: Optional[str] = None,
        query_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Schedule a query to run on a cron schedule."""
        payload = {"name": name, "cron": cron}
        if card_url:
            payload["card_url"] = card_url
        if query_id:
            payload["query_id"] = query_id
        return self._client.post("api", "query", "schedule", json_data=payload)
    
    def get_queries(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Retrieve a list of queries."""
        return self._client.get("api", "query", params={"limit": limit})
    
    def delete_query(self, query_id: str) -> bool:
        """Delete a query by ID."""
        return self._client.delete("api", "query", query_id)
    
    def create_from_suggestion(self, suggestion_id: str) -> Dict[str, Any]:
        """Create a query from a suggestion."""
        return self._client.post(
            "api", "query",
            json_data={"suggestion_id": suggestion_id}
        )
    
    def submit(self, query: str, domain: Optional[str] = None) -> Dict[str, Any]:
        """Submit a direct query."""
        payload = {"query": query}
        if domain:
            payload["domain"] = domain
        return self._client.post("api", "query", "submit", json_data=payload)
    
    def get_embed_token(self, query_id: str) -> str:
        """Get embed access token for a visualization."""
        return self._client.post(
            "api", "query", "embed", "token",
            json_data={"query_id": query_id}
        )


class DataEngineeringClient:
    """Data engineering endpoints."""
    
    def __init__(self, client: BaseClient):
        self._client = client
    
    def ask(
        self,
        domain: str,
        prompt: str,
        target_schema: Optional[str] = None,
        target_table: Optional[str] = None
    ) -> Dict[str, Any]:
        """Ask a data engineering question."""
        payload = {"domain": domain, "prompt": prompt}
        if target_schema:
            payload["target_schema"] = target_schema
        if target_table:
            payload["target_table"] = target_table
        return self._client.post("api", "dataeng", "ask", json_data=payload)
    
    def schedule(
        self,
        query_id: str,
        mode: str,
        cron: Optional[str] = None,
        name: Optional[str] = None,
        target_schema: Optional[str] = None,
        target_table: Optional[str] = None,
        merge_keys: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Schedule a data engineering query."""
        payload = {"query_id": query_id, "mode": mode}
        if cron:
            payload["cron"] = cron
        if name:
            payload["name"] = name
        if target_schema:
            payload["target_schema"] = target_schema
        if target_table:
            payload["target_table"] = target_table
        if merge_keys:
            payload["merge"] = ",".join(merge_keys)
        return self._client.post("api", "dataeng", "schedule", json_data=payload)


class JobsClient:
    """Jobs management endpoints."""
    
    def __init__(self, client: BaseClient):
        self._client = client
    
    def get_all(self) -> List[Dict[str, Any]]:
        """Get all jobs."""
        return self._client.get("api", "jobs")
    
    def get(self, job_id: str) -> Dict[str, Any]:
        """Get a specific job."""
        return self._client.get("api", "jobs", job_id)
    
    def delete(self, job_id: str) -> str:
        """Delete a job."""
        return self._client.delete("api", "jobs", job_id)
    
    def trigger(self, job_id: str) -> str:
        """Manually trigger a job."""
        return self._client.post("api", "jobs", job_id, "trigger")
    
    def update_status(
        self,
        job_id: str,
        status: Union[str, JobStatus],
        status_message: Optional[str] = None
    ) -> Dict[str, Any]:
        """Update job status."""
        payload = {
            "status": status.value if isinstance(status, JobStatus) else status
        }
        if status_message:
            payload["status_message"] = status_message
        return self._client.put("api", "jobs", job_id, "status", json_data=payload)
    
    def create(
        self,
        job_name: str,
        job_type: str,
        schedule: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create a new job."""
        payload = {"job_name": job_name, "job_type": job_type}
        if schedule:
            payload["schedule"] = schedule
        if properties:
            payload["properties_json"] = json.dumps(properties)
        return self._client.post("api", "jobs", json_data=payload)
    
    def wait_for_completion(
        self,
        job_id: str,
        max_wait: int = 300,
        poll_interval: int = 5
    ) -> Dict[str, Any]:
        """Wait for job completion with polling."""
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            status = self.get(job_id)
            job_status = status.get('status', 'UNKNOWN').lower()
            
            if job_status in ['completed', 'success']:
                return status
            elif job_status in ['failed', 'error']:
                raise NX1APIError(
                    f"Job {job_id} failed: {status.get('status_message', 'Unknown error')}"
                )
            
            time.sleep(poll_interval)
        
        raise NX1TimeoutError(f"Job {job_id} did not complete within {max_wait} seconds")


class FilesClient:
    """Files management endpoints."""
    
    def __init__(self, client: BaseClient):
        self._client = client
    
    def upload(self, file_path: str, name: Optional[str] = None) -> Dict[str, Any]:
        """Upload a local file."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        file_name = name or path.name
        extension = path.suffix.lower().lstrip('.')
        content_type = CONTENT_TYPES.get(extension, 'application/octet-stream')
        
        with open(file_path, 'rb') as f:
            files = {'file': (file_name, f, content_type)}
            return self._client.post("api", "files", "file", files=files)
    
    def upload_from_url(self, source_url: str, name: Optional[str] = None) -> Dict[str, Any]:
        """Upload a file from URL."""
        payload = {"source_url": source_url}
        if name:
            payload["name"] = name
        return self._client.post("api", "files", "url", json_data=payload)
    
    def get_all(self) -> List[Dict[str, Any]]:
        """Get all files."""
        return self._client.get("api", "files")
    
    def get(self, file_id: str) -> Dict[str, Any]:
        """Get a specific file."""
        return self._client.get("api", "files", file_id)
    
    def delete(self, file_id: str) -> str:
        """Delete a file."""
        return self._client.delete("api", "files", file_id)
    
    def download(self, file_id: str) -> bytes:
        """Download file content."""
        return self._client.get("api", "files", file_id, "download")


class DataIngestionClient:
    """Data ingestion endpoints with file upload pipeline."""
    
    def __init__(
        self,
        client: BaseClient,
        files_client: Optional[FilesClient] = None,
        jobs_client: Optional[JobsClient] = None
    ):
        self._client = client
        self._files_client = files_client
        self._jobs_client = jobs_client
    
    def _set_clients(self, files_client: FilesClient, jobs_client: JobsClient):
        """Set dependent clients (called after NX1Client init)."""
        self._files_client = files_client
        self._jobs_client = jobs_client
    
    @staticmethod
    def detect_file_format(file_path: str) -> str:
        """Detect file format from file extension."""
        extension = Path(file_path).suffix.lower().lstrip('.')
        if extension not in FILE_EXTENSION_MAP:
            raise NX1ValidationError(
                f"Unsupported file format: {extension}. "
                f"Supported formats: {', '.join(FILE_EXTENSION_MAP.keys())}"
            )
        return FILE_EXTENSION_MAP[extension]
    
    @staticmethod
    def build_file_options(
        file_format: str,
        header: Optional[str] = None,
        infer_schema: Optional[str] = None,
        delimiter: Optional[str] = None,
        quote: Optional[str] = None,
        date_format: Optional[str] = None,
        timestamp_format: Optional[str] = None,
        sheet_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Build file read options based on format."""
        options: Dict[str, Any] = {}
        format_config = FILE_FORMAT_OPTIONS.get(file_format, {})
        
        if file_format == 'csv':
            options['header'] = header or format_config.get('header', {}).get('default', 'true')
            options['inferSchema'] = infer_schema or format_config.get('inferSchema', {}).get('default', 'true')
            if delimiter:
                options['delimiter'] = delimiter
            if quote:
                options['quote'] = quote
            if date_format:
                options['dateFormat'] = date_format
            if timestamp_format:
                options['timestampFormat'] = timestamp_format
        
        elif file_format in ('xls', 'xlsx'):
            options['header'] = header or format_config.get('header', {}).get('default', 'true')
            options['sheet_name'] = sheet_name or format_config.get('sheet_name', {}).get('default', '0')
        
        return options
    
    def submit(
        self,
        name: str,
        ingesttype: Union[str, IngestType],
        schema_name: str,
        table: str,
        mode: Union[str, IngestMode] = IngestMode.OVERWRITE,
        # File options
        file_path: Optional[str] = None,
        file_id: Optional[str] = None,
        file_format: Optional[str] = None,
        file_read_options: Optional[Dict[str, Any]] = None,
        # JDBC options
        jdbc_url: Optional[str] = None,
        jdbc_username: Optional[str] = None,
        jdbc_password: Optional[str] = None,
        jdbc_driver: Optional[str] = None,
        jdbc_type: Optional[Union[str, JdbcType]] = None,
        jdbc_schema: Optional[str] = None,
        jdbc_table: Optional[str] = None,
        jdbc_query: Optional[str] = None,
        # Lakehouse options
        lakehouse_catalog: Optional[str] = None,
        lakehouse_sourceschema: Optional[str] = None,
        lakehouse_sourcetable: Optional[str] = None,
        lakehouse_conditions: Optional[str] = None,
        # Merge options
        merge_keys: Optional[Union[str, List[str]]] = None,
        # Column transformations
        column_transformations: Optional[List[Dict[str, Any]]] = None,
        # Schedule
        schedule: Optional[str] = None,
        # Metadata
        domain: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Submit a data ingestion job."""
        payload: Dict[str, Any] = {
            "name": name,
            "ingesttype": ingesttype.value if isinstance(ingesttype, IngestType) else ingesttype,
            "schema_name": schema_name,
            "table": table,
            "mode": mode.value if isinstance(mode, IngestMode) else mode
        }
        
        # File options
        if file_path:
            payload["file_path"] = file_path
        if file_id:
            payload["file_id"] = str(file_id)
        if file_format:
            payload["file_format"] = file_format
        if file_read_options:
            payload["file_read_options"] = file_read_options
        
        # JDBC options
        if jdbc_url:
            payload["jdbc_url"] = jdbc_url
        if jdbc_username:
            payload["jdbc_username"] = jdbc_username
        if jdbc_password:
            payload["jdbc_password"] = jdbc_password
        if jdbc_driver:
            payload["jdbc_driver"] = jdbc_driver
        if jdbc_type:
            payload["jdbc_type"] = jdbc_type.value if isinstance(jdbc_type, JdbcType) else jdbc_type
        if jdbc_schema:
            payload["jdbc_schema"] = jdbc_schema
        if jdbc_table:
            payload["jdbc_table"] = jdbc_table
        if jdbc_query:
            payload["jdbc_query"] = jdbc_query
        
        # Lakehouse options
        if lakehouse_catalog:
            payload["lakehouse_catalog"] = lakehouse_catalog
        if lakehouse_sourceschema:
            payload["lakehouse_sourceschema"] = lakehouse_sourceschema
        if lakehouse_sourcetable:
            payload["lakehouse_sourcetable"] = lakehouse_sourcetable
        if lakehouse_conditions:
            payload["lakehouse_conditions"] = lakehouse_conditions
        
        # Merge keys
        if merge_keys:
            if isinstance(merge_keys, list):
                payload["merge"] = ",".join(merge_keys)
            else:
                payload["merge"] = merge_keys
        
        # Column transformations
        if column_transformations:
            payload["column_transformations"] = column_transformations
        
        # Schedule
        if schedule:
            payload["schedule"] = schedule
        
        # Metadata
        if domain:
            payload["domain"] = domain
        if tags:
            payload["tags"] = tags
        
        return self._client.post("api", "dataingest", "submit", json_data=payload)
    
    def get_file_ingestion_options(self) -> Dict[str, Any]:
        """Get available file format options."""
        return self._client.get("api", "dataingest", "file_ingestion_options")
    
    def ingest_local_file(
        self,
        file_path: str,
        table: str,
        schema_name: str,
        mode: Union[str, IngestMode] = IngestMode.OVERWRITE,
        merge_keys: Optional[List[str]] = None,
        column_transformations: Optional[List[Dict[str, Any]]] = None,
        # Job options
        job_name: Optional[str] = None,
        # File options
        header: str = "true",
        infer_schema: str = "true",
        delimiter: str = ",",
        quote: str = '"',
        date_format: Optional[str] = None,
        timestamp_format: Optional[str] = None,
        sheet_name: Optional[str] = None,
        # Metadata
        domain: Optional[str] = None,
        tags: Optional[List[str]] = None,
        # Execution options
        wait_for_completion: bool = True,
        max_wait: int = 300,
        poll_interval: int = 5,
        verbose: bool = True
    ) -> str:
        """
        Complete pipeline: Upload local file and ingest into lakehouse.
        
        This combines:
        1. File upload to S3
        2. Ingestion job submission
        3. Job triggering
        4. Optional wait for completion
        
        Args:
            file_path: Local file path
            table: Target table name
            schema_name: Target schema/database name
            mode: Write mode (append, overwrite, merge)
            merge_keys: Keys for merge mode
            column_transformations: List of transformations (cast, rename, encrypt)
            job_name: Optional custom job name. If not provided, generates one.
            header: CSV has header row
            infer_schema: Auto-infer schema
            delimiter: CSV delimiter
            quote: CSV quote character
            date_format: Date format string
            timestamp_format: Timestamp format string
            sheet_name: Excel sheet name/index
            domain: Domain for metadata
            tags: Tags for metadata
            wait_for_completion: Wait for job to finish
            max_wait: Max wait time in seconds
            poll_interval: Poll interval in seconds
            verbose: Print progress messages
        
        Returns:
            Job ID string
        """
        if not self._files_client or not self._jobs_client:
            raise NX1Error("Files and Jobs clients not initialized")
        
        def log(msg: str) -> None:
            if verbose:
                print(msg)
        
        log(f"Starting ingestion pipeline for table '{schema_name}.{table}'")
        log(f"File: {file_path}")
        log(f"Mode: {mode.value if isinstance(mode, IngestMode) else mode}")
        
        # Validate file exists
        if not Path(file_path).exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Detect file format
        file_format = self.detect_file_format(file_path)
        log(f"Detected format: {file_format}")
        
        # Build file options
        file_options = self.build_file_options(
            file_format=file_format,
            header=header,
            infer_schema=infer_schema,
            delimiter=delimiter,
            quote=quote,
            date_format=date_format,
            timestamp_format=timestamp_format,
            sheet_name=sheet_name
        )
        log(f"File options: {json.dumps(file_options)}")
        
        # Step 1: Upload file
        log("\n1. Uploading file...")
        upload_response = self._files_client.upload(file_path)
        s3_url = upload_response.get('s3_url')
        file_id = upload_response.get('id')
        log(f"   ✓ Uploaded: {s3_url}")
        log(f"   File ID: {file_id}")
        
        # Step 2: Submit ingestion job
        log("\n2. Submitting ingestion job...")
        # Use custom job name if provided, otherwise generate one
        actual_job_name = job_name if job_name else f"ingestion_{table}_{int(time.time())}"
        
        ingestion_response = self.submit(
            name=actual_job_name,
            ingesttype=IngestType.FILE,
            schema_name=schema_name,
            table=table,
            mode=mode,
            file_path=s3_url,
            file_format=file_format,
            file_read_options=file_options,
            merge_keys=merge_keys,
            column_transformations=column_transformations,
            domain=domain,
            tags=tags
        )
        
        if ingestion_response.get('error'):
            raise NX1APIError(f"Ingestion submission failed: {ingestion_response['error']}")
        
        job_id = ingestion_response.get('job_id')
        flow_url = ingestion_response.get('flow_url')
        
        if not job_id:
            raise NX1APIError("Failed to get job_id from ingestion submission")
        
        log(f"   ✓ Job submitted: {job_id}")
        if flow_url:
            log(f"   Flow URL: {flow_url}")
        
        # Step 3: Trigger job
        log("\n3. Triggering job...")
        try:
            self._jobs_client.trigger(str(job_id))
            log("   ✓ Job triggered")
        except Exception as e:
            log(f"   Note: Job trigger returned: {e} (may auto-start)")
        
        # Step 4: Wait for completion
        if wait_for_completion:
            log(f"\n4. Waiting for completion (max {max_wait}s)...")
            try:
                final_status = self._jobs_client.wait_for_completion(
                    str(job_id),
                    max_wait=max_wait,
                    poll_interval=poll_interval
                )
                log(f"\n✅ Job completed successfully!")
                log(f"   Status: {final_status.get('status')}")
            except NX1TimeoutError:
                log(f"\n⏱️ Job did not complete within {max_wait}s")
                log("   Job may still be running - check Airflow UI")
            except Exception as e:
                log(f"\n❌ Job failed: {e}")
                raise
        
        return str(job_id)


class DataMirroringClient:
    """Data mirroring endpoints."""
    
    def __init__(self, client: BaseClient):
        self._client = client
    
    def create(
        self,
        job_name: str,
        source_catalog: str,
        source_schema: str,
        source_table: str,
        target_catalog: str,
        target_schema: str,
        target_table: str,
        schedule: Optional[str] = None,
        mode: str = "overwrite",
        merge_keys: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Create a data mirroring job."""
        payload = {
            "job_name": job_name,
            "source_catalog": source_catalog,
            "source_schema": source_schema,
            "source_table": source_table,
            "target_catalog": target_catalog,
            "target_schema": target_schema,
            "target_table": target_table,
            "mode": mode
        }
        if schedule:
            payload["schedule"] = schedule
        if merge_keys:
            payload["merge"] = ",".join(merge_keys)
        return self._client.post("api", "datamirroring", json_data=payload)
    
    def delete(self, job_id: str) -> str:
        """Delete a data mirroring job."""
        return self._client.delete("api", "datamirroring", job_id)


class DataQualityClient:
    """Data quality endpoints."""
    
    def __init__(self, client: BaseClient):
        self._client = client
    
    def suggest(self, table: str, dq_request: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get data quality suggestions for a table."""
        payload = {"table": table}
        if dq_request:
            payload["dq_request"] = dq_request
        return self._client.post("api", "dq", "suggest", json_data=payload)
    
    def accept(self, dq_id: str) -> bool:
        """Accept a data quality suggestion."""
        return self._client.post("api", "dq", "accept", json_data={"dq_id": dq_id})
    
    def get_rules_by_table(self, table: str, only_accepted: bool = False) -> List[Dict[str, Any]]:
        """Get data quality rules for a table."""
        return self._client.get(
            "api", "dq", "rules", "tables", table,
            params={"only_accepted": only_accepted}
        )
    
    def get_rule(self, dq_id: str) -> Dict[str, Any]:
        """Get a specific data quality rule."""
        return self._client.get("api", "dq", "rules", "ids", dq_id)
    
    def delete_rule(self, dq_id: str) -> str:
        """Delete a data quality rule."""
        return self._client.delete("api", "dq", "rules", "ids", dq_id)
    
    def run_report_by_table(self, table: str) -> List[Dict[str, Any]]:
        """Run data quality report for a table."""
        return self._client.post("api", "dq", "rules", "tables", table, "report")
    
    def run_report_by_rule(self, rule_id: str) -> Dict[str, Any]:
        """Run a single data quality rule."""
        return self._client.post("api", "dq", "rules", rule_id, "report")
    
    def report_assertions(self, report_items: List[Dict[str, Any]]) -> bool:
        """Report assertion results back to DataHub."""
        return self._client.post(
            "api", "dq", "rules", "report",
            json_data={"report_items": report_items}
        )


class S3Client:
    """S3 bucket management endpoints."""
    
    def __init__(self, client: BaseClient):
        self._client = client
    
    def create_bucket(
        self,
        bucket: str,
        endpoint: str,
        accesskey: str,
        secretkey: str
    ) -> Dict[str, Any]:
        """Create a new S3 bucket record."""
        return self._client.post("api", "s3", "buckets", json_data={
            "bucket": bucket,
            "endpoint": endpoint,
            "accesskey": accesskey,
            "secretkey": secretkey
        })
    
    def get_buckets(self) -> List[Dict[str, Any]]:
        """Get all S3 bucket records."""
        return self._client.get("api", "s3", "buckets")
    
    def get_bucket(self, bucket_name: str) -> Dict[str, Any]:
        """Get a specific S3 bucket."""
        return self._client.get("api", "s3", "buckets", bucket_name)
    
    def update_bucket(
        self,
        bucket_name: str,
        endpoint: Optional[str] = None,
        accesskey: Optional[str] = None,
        secretkey: Optional[str] = None
    ) -> Dict[str, Any]:
        """Update an S3 bucket."""
        payload: Dict[str, str] = {}
        if endpoint:
            payload["endpoint"] = endpoint
        if accesskey:
            payload["accesskey"] = accesskey
        if secretkey:
            payload["secretkey"] = secretkey
        return self._client.put("api", "s3", "buckets", bucket_name, json_data=payload)
    
    def delete_bucket(self, bucket_name: str) -> str:
        """Delete an S3 bucket record."""
        return self._client.delete("api", "s3", "buckets", bucket_name)
    
    def refresh(self) -> Dict[str, Any]:
        """Manually refresh all S3 buckets."""
        return self._client.post("api", "s3", "refresh")


class DataProductsClient:
    """Data products (views) endpoints."""
    
    def __init__(self, client: BaseClient):
        self._client = client
    
    def get_all(self, roles: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all data products."""
        params = {}
        if roles:
            params["roles"] = roles
        return self._client.get("api", "dataproduct", params=params)
    
    def create(
        self,
        name: str,
        role_name: str,
        domain_urn: Optional[str] = None,
        query: Optional[str] = None,
        suggestion_id: Optional[str] = None,
        query_id: Optional[str] = None,
        is_view: bool = False
    ) -> Dict[str, Any]:
        """Create a data product."""
        payload: Dict[str, Any] = {
            "name": name,
            "role_name": role_name,
            "is_view": is_view
        }
        if domain_urn:
            payload["domain_urn"] = domain_urn
        if query:
            payload["query"] = query
        if suggestion_id:
            payload["suggestion_id"] = suggestion_id
        if query_id:
            payload["query_id"] = query_id
        return self._client.post("api", "dataproduct", json_data=payload)
    
    def get(self, dataproduct_id: str) -> Dict[str, Any]:
        """Get a specific data product."""
        return self._client.get("api", "dataproduct", dataproduct_id)
    
    def delete(self, dataproduct_id: str) -> bool:
        """Delete a data product."""
        return self._client.delete("api", "dataproduct", dataproduct_id)
    
    def create_psk(
        self,
        dataproduct_id: str,
        description: str,
        validity_days: int = 30
    ) -> Dict[str, Any]:
        """Create a pre-shared key for a data product."""
        return self._client.post(
            "api", "dataproduct", dataproduct_id, "psk",
            json_data={"description": description, "validity_days": validity_days}
        )
    
    def get_all_psks(self, include_deleted: bool = False) -> List[Dict[str, Any]]:
        """Get all pre-shared keys."""
        return self._client.get(
            "api", "dataproduct", "psk",
            params={"include_deleted": include_deleted}
        )
    
    def delete_psk(self, psk_id: str) -> bool:
        """Delete a pre-shared key."""
        return self._client.delete("api", "dataproduct", "psk", psk_id)
    
    def extend_psk(self, psk_id: str, validity_days: int) -> bool:
        """Extend the validity of a pre-shared key."""
        return self._client.post(
            "api", "dataproduct", "psk", psk_id, "extend",
            json_data={"validity_days": validity_days}
        )


class DataConsumerClient:
    """Data consumer endpoints."""
    
    def __init__(self, client: BaseClient):
        self._client = client
    
    def get_data(
        self,
        view_name: str,
        psk: Optional[str] = None,
        filter: Optional[str] = None,
        order: Optional[str] = None,
        start: int = 0,
        limit: int = 100
    ) -> Dict[str, Any]:
        """Retrieve data from a view."""
        params: Dict[str, Any] = {"start": start, "limit": limit}
        if psk:
            params["psk"] = psk
        if filter:
            params["filter"] = filter
        if order:
            params["order"] = order
        return self._client.get("api", "data", view_name, params=params)


class AppsClient:
    """Apps management endpoints - full CRUD for apps, versions, roles, components."""
    
    def __init__(self, client: BaseClient):
        self._client = client
    
    # App Operations
    def create(self, app_name: str) -> Dict[str, Any]:
        """Create a new app."""
        return self._client.post("api", "app", json_data={"app_name": app_name})
    
    def get_all(self) -> List[Dict[str, Any]]:
        """Get all apps."""
        return self._client.get("api", "app")
    
    def get(self, app_id: str) -> Dict[str, Any]:
        """Get a specific app."""
        return self._client.get("api", "app", app_id)
    
    def update(self, app_id: str, app_name: str) -> Dict[str, Any]:
        """Update an app."""
        return self._client.put("api", "app", app_id, json_data={"app_name": app_name})
    
    def delete(self, app_id: str) -> Dict[str, Any]:
        """Delete an app and all associated data."""
        return self._client.delete("api", "app", app_id)
    
    # App Role Operations
    def create_role(self, app_id: str, role_name: str) -> Dict[str, Any]:
        """Create an app role."""
        return self._client.post(
            "api", "app", app_id, "roles",
            json_data={"role_name": role_name}
        )
    
    def get_roles(self, app_id: str) -> List[Dict[str, Any]]:
        """Get app roles."""
        return self._client.get("api", "app", app_id, "roles")
    
    def delete_role(self, app_id: str, role_id: str) -> Dict[str, Any]:
        """Delete an app role."""
        return self._client.delete("api", "app", app_id, "roles", role_id)
    
    # App Version Operations
    def create_version(self, app_id: str, version_name: str) -> Dict[str, Any]:
        """Create an app version."""
        return self._client.post(
            "api", "app", app_id, "versions",
            json_data={"version_name": version_name}
        )
    
    def get_versions(self, app_id: str) -> List[Dict[str, Any]]:
        """Get app versions."""
        return self._client.get("api", "app", app_id, "versions")
    
    def get_version(self, app_id: str, version_id: str) -> Dict[str, Any]:
        """Get a specific app version."""
        return self._client.get("api", "app", app_id, "versions", version_id)
    
    def delete_version(self, version_id: str) -> Dict[str, Any]:
        """Delete an app version (cannot delete ACTIVE versions)."""
        return self._client.delete("api", "app", "versions", version_id)
    
    def activate_version(self, version_id: str) -> Dict[str, Any]:
        """Activate an app version."""
        return self._client.post("api", "app", "versions", version_id, "activate")
    
    # App Component Operations
    def get_components(self, version_id: str) -> List[Dict[str, Any]]:
        """Get components for an app version."""
        return self._client.get("api", "app", "versions", version_id, "components")
    
    def add_component(
        self,
        version_id: str,
        component_type: Union[str, AppComponentType],
        file_path: str,
        filename: Optional[str] = None,
        properties_json: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Add a component to an app version."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        comp_type = component_type.value if isinstance(component_type, AppComponentType) else component_type
        fname = filename or path.name
        
        with open(file_path, 'rb') as f:
            files = {'file': (fname, f)}
            data: Dict[str, str] = {
                'component_type': comp_type,
                'filename': fname
            }
            if properties_json:
                data['properties_json'] = json.dumps(properties_json)
            
            return self._client.post(
                "api", "app", "versions", version_id, "components",
                files=files,
                data=data
            )
    
    def add_dag(
        self,
        version_id: str,
        file_path: str,
        filename: Optional[str] = None
    ) -> Dict[str, Any]:
        """Add a DAG component to an app version."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        fname = filename or path.name
        
        with open(file_path, 'rb') as f:
            files = {'file': (fname, f)}
            data = {'filename': fname}
            return self._client.post(
                "api", "app", "versions", version_id, "dags",
                files=files,
                data=data
            )
    
    def delete_component(self, component_id: str) -> Dict[str, Any]:
        """Delete an app component."""
        return self._client.delete("api", "app", "components", component_id)


class CrewsClient:
    """AI Crews endpoints."""
    
    def __init__(self, client: BaseClient):
        self._client = client
    
    def list_crews(self) -> List[Dict[str, Any]]:
        """List available crews."""
        return self._client.get("api", "crews")
    
    def run_crew(self, crew_name: str, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Run a crew with inputs."""
        return self._client.post("api", "crews", crew_name, "run", json_data=inputs)
    
    def get_crew_status(self, task_id: str) -> Dict[str, Any]:
        """Get crew task status."""
        return self._client.get("api", "crews", "tasks", task_id)


class DataSharesClient:
    """Data shares endpoints."""
    
    def __init__(self, client: BaseClient):
        self._client = client
    
    def get_all(self) -> List[Dict[str, Any]]:
        """Get all data shares."""
        return self._client.get("api", "data_shares")
    
    def create(
        self,
        name: str,
        description: Optional[str] = None,
        tables: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Create a data share."""
        payload: Dict[str, Any] = {"name": name}
        if description:
            payload["description"] = description
        if tables:
            payload["tables"] = tables
        return self._client.post("api", "data_shares", json_data=payload)
    
    def get(self, share_id: str) -> Dict[str, Any]:
        """Get a specific data share."""
        return self._client.get("api", "data_shares", share_id)
    
    def delete(self, share_id: str) -> Dict[str, Any]:
        """Delete a data share."""
        return self._client.delete("api", "data_shares", share_id)


class WorkerClient:
    """Worker/Celery task endpoints."""
    
    def __init__(self, client: BaseClient):
        self._client = client
    
    def get_status(self) -> Dict[str, Any]:
        """Get worker status."""
        return self._client.get("api", "worker", "status")
    
    def get_task(self, task_id: str) -> Dict[str, Any]:
        """Get task status."""
        return self._client.get("api", "worker", "tasks", task_id)
