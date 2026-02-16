# NX1 SDK

A comprehensive Python SDK for the NX1 NLP Agent API.

## Features

- **Complete API Coverage**: All 17+ API endpoints including metastore, queries, ingestion, data quality, jobs, apps, and more
- **File Ingestion Pipeline**: Combined upload + ingest workflow with a single method call
- **Column Transformations**: Cast, rename, and encrypt columns during ingestion
- **Profile Management**: Store multiple API configurations in `~/.nx1/profiles`
- **Type Safety**: Full type hints and enums for better IDE support
- **CLI Tool**: Command-line interface for all operations
- **Multiple Output Formats**: JSON, YAML, and table output

## Installation

```bash
pip install nx1-sdk
```

Or install from source:

```bash
git clone https://github.com/nx1/nx1-sdk-python.git
cd nx1-sdk-python
pip install -e .
```

## Quick Start

### Python API

```python
from nx1_sdk import NX1Client, IngestMode, ColumnTransformation, SparkDataType

# Initialize client (uses env vars or default profile)
client = NX1Client()

# Or with explicit credentials
client = NX1Client(
    api_key="your-psk-key",
    host="https://aiapi.example.nx1cloud.com"
)

# Or with a named profile
client = NX1Client(profile="dev")

# Health check
health = client.health.ping()
print(f"API Version: {health['version']}")

# Ask a natural language question
response = client.queries.ask(
    domain="Sales Data",
    prompt="Show me top 10 customers by revenue"
)

# Ingest a local CSV file (complete pipeline)
job_id = client.ingestion.ingest_local_file(
    file_path="/path/to/data.csv",
    table="customers",
    schema_name="staging",
    job_name="my_custom_job",  # Optional - auto-generated if not provided
    mode=IngestMode.OVERWRITE,
    column_transformations=[
        ColumnTransformation.cast("created_at", SparkDataType.TIMESTAMP),
        ColumnTransformation.rename("cust_id", "customer_id"),
        ColumnTransformation.encrypt("ssn"),
    ]
)
```

### Configuration Priority

The SDK resolves configuration in this order (highest priority first):

1. **Explicit parameters** (`api_key`, `host`)
2. **Environment variables** (`NX1_API_KEY`, `NX1_HOST`)
3. **Profile** (`~/.nx1/profiles`)

### Environment Variables

```bash
export NX1_API_KEY="your-psk-key"
export NX1_HOST="https://aiapi.example.nx1cloud.com"
```

### Profiles

Store multiple API configurations in `~/.nx1/profiles` (YAML format):

```yaml
# ~/.nx1/profiles
default:
  host: https://api.production.nx1cloud.com
  api_key: psk_prod_xxxxx
  verify_ssl: true
  timeout: 30

dev:
  host: https://api.dev.nx1cloud.com
  api_key: psk_dev_xxxxx
  verify_ssl: false

staging:
  host: https://api.staging.nx1cloud.com
  api_key: psk_staging_xxxxx
```

Use profiles in code:

```python
# Use default profile
client = NX1Client()

# Use named profile
client = NX1Client(profile="dev")

# Explicit credentials override profile
client = NX1Client(profile="dev", api_key="override_key")
```

Manage profiles via CLI:

```bash
# List profiles
nx1 profile list

# Add/update a profile
nx1 profile add --name dev --host https://api.dev.example.com --api-key psk_xxx

# Remove a profile
nx1 profile remove dev

# Show profile details
nx1 profile show dev

# Show profiles file path
nx1 profile path
```

### CLI Usage

```bash
# Health check
nx1 ping

# List domains (with different output formats)
nx1 domains
nx1 domains -o yaml
nx1 domains -o table

# Ask a question
nx1 ask --domain "Sales Data" --prompt "Show top 10 customers"

# Use a specific profile
nx1 --profile dev domains
nx1 -p staging jobs list

# Ingest a local file with transformations
nx1 ingest-file --file data.csv --table customers --schema staging \
    --name my_ingestion_job \
    --cast "date_col:date" --rename "old_name:new_name" --encrypt ssn

# List jobs
nx1 jobs list -o table

# App management
nx1 apps list
nx1 apps create --name my-app
nx1 apps versions --app-id <uuid>

# Full app deployment (create/find app, create version, upload components, activate)
nx1 apps deploy --app-name my-app --version 1.0 \
    --dags /path/to/dag1.py,/path/to/dag2.py \
    --artifacts /path/to/artifact1.py,/path/to/artifact2.py
```

## API Reference

### Service Clients

| Client | Description |
|--------|-------------|
| `client.health` | Health checks and version info |
| `client.metastore` | Catalogs, schemas, tables, columns, tags, domains |
| `client.queries` | Natural language queries, suggestions, scheduling |
| `client.data_engineering` | Data engineering pipelines |
| `client.ingestion` | Data ingestion with file upload pipeline |
| `client.mirroring` | Data mirroring jobs |
| `client.data_quality` | DQ rules, suggestions, reports |
| `client.jobs` | Job management and monitoring |
| `client.files` | File upload and management |
| `client.s3` | S3 bucket management |
| `client.data_products` | Data products and pre-shared keys |
| `client.data` | Data consumption from views |
| `client.apps` | App management (versions, roles, components) |
| `client.crews` | AI crew orchestration |
| `client.data_shares` | Data sharing |
| `client.worker` | Celery worker tasks |

### Column Transformations

```python
from nx1_sdk import ColumnTransformation, SparkDataType

# Cast a column to a different type
ColumnTransformation.cast("date_column", SparkDataType.DATE)
ColumnTransformation.cast("amount", SparkDataType.DECIMAL)

# Rename a column
ColumnTransformation.rename("old_name", "new_name")

# Encrypt a column (uses vault_encrypt UDF)
ColumnTransformation.encrypt("ssn")
ColumnTransformation.encrypt("credit_card", encryption_key_name="pci_key")
```

### Ingestion Modes

```python
from nx1_sdk import IngestMode

IngestMode.APPEND      # Add new rows
IngestMode.OVERWRITE   # Replace all data
IngestMode.MERGE       # Upsert based on merge keys
```

### Full Ingestion Example

```python
from nx1_sdk import (
    NX1Client,
    IngestType,
    IngestMode,
    ColumnTransformation,
    SparkDataType
)

client = NX1Client()

# Method 1: Local file with full pipeline (upload + ingest + wait)
job_id = client.ingestion.ingest_local_file(
    file_path="/data/employees.csv",
    table="employees",
    schema_name="hr",
    mode=IngestMode.MERGE,
    merge_keys=["employee_id"],
    column_transformations=[
        ColumnTransformation.cast("hire_date", SparkDataType.DATE),
        ColumnTransformation.cast("salary", SparkDataType.DECIMAL),
        ColumnTransformation.encrypt("ssn"),
    ],
    header="true",
    delimiter=",",
    domain="HR",
    tags=["employees", "pii"],
    wait_for_completion=True,
    max_wait=600
)

# Method 2: Direct S3 path submission
response = client.ingestion.submit(
    name="daily_sales_load",
    ingesttype=IngestType.FILE,
    schema_name="analytics",
    table="sales",
    mode=IngestMode.APPEND,
    file_path="s3://data-bucket/sales/2024/data.parquet",
    file_format="parquet",
    schedule="0 6 * * *"  # Daily at 6 AM
)

# Method 3: JDBC ingestion
response = client.ingestion.submit(
    name="oracle_sync",
    ingesttype=IngestType.JDBC,
    schema_name="staging",
    table="customers",
    mode=IngestMode.OVERWRITE,
    jdbc_url="jdbc:oracle:thin:@//host:1521/db",
    jdbc_username="user",
    jdbc_password="pass",
    jdbc_type="table",
    jdbc_schema="PROD",
    jdbc_table="CUSTOMERS"
)
```

### App Management Example

```python
# Create an app
app = client.apps.create("my-data-app")
app_id = app["id"]

# Create a role
role = client.apps.create_role(app_id, "my-app-users")

# Create a version
version = client.apps.create_version(app_id, "v1.0.0")
version_id = version["id"]

# Add a DAG component
client.apps.add_dag(version_id, "/path/to/my_dag.py")

# Add an artifact component
client.apps.add_component(version_id, "artifact", "/path/to/artifact.py")

# Activate the version
client.apps.activate_version(version_id)
```

### App Deployment Example

Full deployment workflow — finds or creates the app, creates a new version, uploads DAGs and artifacts, and activates:

```python
import sys

client = NX1Client()
apps_manager = client.apps

app_name = "my-data-app"
version = "1.0"
dags = "/path/to/dag1.py,/path/to/dag2.py"
artifacts = "/path/to/artifact1.py,/path/to/artifact2.py"

# Find or create the app
apps = apps_manager.get_all()
existing = [a for a in apps if a['app_name'] == app_name]
if existing:
    app_id = existing[0]['id']
else:
    app_id = apps_manager.create(app_name)['id']

# Ensure the version doesn't already exist
versions = apps_manager.get_versions(app_id)
if version in [v['version_name'] for v in versions]:
    print(f"Version {version} already exists")
    sys.exit(1)

# Create version
version_id = apps_manager.create_version(app_id, version)['id']

# Upload artifacts
for path in artifacts.split(','):
    apps_manager.add_component(version_id, "artifact", path.strip())

# Upload DAGs
for path in dags.split(','):
    apps_manager.add_dag(version_id, path.strip())

# Activate
apps_manager.activate_version(version_id)
```

Or use the CLI shorthand:

```bash
nx1 apps deploy --app-name my-data-app --version 1.0 \
    --dags /path/to/dag1.py,/path/to/dag2.py \
    --artifacts /path/to/artifact1.py,/path/to/artifact2.py
```

## Error Handling

```python
from nx1_sdk import NX1Client, NX1APIError, NX1TimeoutError, NX1ValidationError

client = NX1Client()

try:
    result = client.queries.ask("Sales", "Show revenue")
except NX1ValidationError as e:
    print(f"Invalid input: {e}")
except NX1APIError as e:
    print(f"API error: {e}")
    print(f"Status code: {e.status_code}")
    print(f"Response: {e.response}")
except NX1TimeoutError as e:
    print(f"Request timed out: {e}")
```

## Configuration Options

```python
client = NX1Client(
    api_key="your-key",           # Or NX1_API_KEY env var
    host="https://api.example.com", # Or NX1_HOST env var
    verify_ssl=True,              # SSL certificate verification
    timeout=30,                   # Request timeout in seconds
    logger=custom_logger          # Custom logging.Logger instance
)
```

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black src/
isort src/

# Type checking
mypy src/
```

## License

MIT License - see LICENSE file for details.
