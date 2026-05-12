# nx1-sdk

Python SDK and CLI for the NX1 NLP Agent API, providing programmatic and command-line access to all NX1 platform services (metastore, queries, ingestion, data quality, jobs, apps, crews, mirroring, and more).

## Stack

- Python 3.9+
- `requests` for HTTP
- `pyyaml` for config/profile management
- `tabulate` for CLI table output
- `argparse` CLI (entry point: `nx1`)
- Build: setuptools
- Test: pytest, pytest-cov
- Lint/format: black (line-length 100), isort (profile: black)
- Type checking: mypy

## Layout

```
src/nx1_sdk/
  __init__.py          # Public API re-exports
  client.py            # NX1Client - main entry point, composes all service clients
  base.py              # BaseClient - HTTP request layer (auth, retries, error handling)
  cli.py               # CLI entry point (nx1 command), argparse-based
  enums.py             # All enums (IngestMode, IngestType, SparkDataType, JobStatus, etc.)
  constants.py         # File format options, extension maps, content types
  exceptions.py        # Exception hierarchy: NX1Error > NX1APIError, NX1ValidationError, NX1TimeoutError
  profiles.py          # Profile management (~/.nx1/profiles YAML)
  transformations.py   # ColumnTransformation helper (cast, rename, encrypt)
  py.typed             # PEP 561 marker
  services/
    nx1_service.py     # All NX1 API service clients (Health, Metastore, Queries, Ingestion, Jobs, Apps, etc.)
    airflow_trigger_service.py  # Airflow DAG triggering client
    kyuubi_service.py  # Kyuubi Spark batch submission client
    superset_service.py        # Superset client
    jupyterhub_services.py     # JupyterHub client
tests/
  test_sdk.py          # Unit tests (transformations, client init, enums, ingestion helpers)
.github/workflows/
  app_deployment.yml   # GitHub Actions workflow for NX1 app deployment
```

## Commands

```bash
# Install (editable, with dev deps)
pip install -e ".[dev]"

# Run tests
pytest

# Run tests with coverage
pytest --cov=nx1_sdk --cov-report=term-missing

# Format
black src/ tests/
isort src/ tests/

# Type check
mypy src/

# CLI smoke test (requires NX1_API_KEY and NX1_HOST)
nx1 ping
```

## Conventions

### Code Style
- black with line-length 100
- isort with black profile
- Full type hints on all public APIs
- py.typed marker for PEP 561 compliance

### Architecture
- `NX1Client` is the single entry point; it composes domain-specific service clients
- Each service client lives in `services/nx1_service.py` and receives a `BaseClient` instance
- `BaseClient` handles all HTTP concerns: auth headers, URL building, SSL, timeouts, error mapping
- Configuration resolution follows strict priority: explicit args > env vars > profiles
- Enums use `str, Enum` pattern for JSON serialization

### Naming
- Service clients: `{Domain}Client` (e.g., `MetastoreClient`, `AppsClient`, `DataIngestionClient`)
- Exceptions: `NX1{Type}Error` (e.g., `NX1APIError`, `NX1ValidationError`)
- Enums: PascalCase class, UPPER_CASE members
- CLI subcommands: kebab-case (e.g., `ingest-file`, `create-version`)
- Module files: snake_case

### Error Handling
- All custom exceptions inherit from `NX1Error`
- `NX1APIError` carries `status_code` and `response` attributes
- HTTP errors are caught in `BaseClient._request()` and re-raised as `NX1APIError`
- Timeouts become `NX1TimeoutError`
- Invalid config/input raises `NX1ValidationError`

### Testing
- pytest with `-v --cov=nx1_sdk --cov-report=term-missing` (configured in pyproject.toml)
- Tests use `unittest.mock.Mock` and `monkeypatch` for env vars
- Test file naming: `test_*.py` in `tests/`

## Patterns

### Adding a New Service Client

1. Add a new class in `services/nx1_service.py` following the `{Domain}Client` pattern
2. Accept `BaseClient` in `__init__` and use `self._client.get/post/put/delete` for requests
3. Wire it into `NX1Client.__init__()` in `client.py`
4. Export from `__init__.py` if needed
5. Add CLI subcommands in `cli.py` and dispatch in `_execute_command()`

### Adding a New Enum

1. Add to `enums.py` using `class MyEnum(str, Enum)` pattern
2. Export from `__init__.py`

### Adding CLI Subcommands

1. Create a subparser in `main()` in `cli.py`
2. Use `parents=[parent_parser]` to inherit global args
3. Add a handler function `_handle_{domain}(client, args)`
4. Dispatch from `_execute_command()`

### Authentication

- PSK-based via `Authorization-PSK` header (not Bearer)
- Config priority: explicit params > `NX1_API_KEY`/`NX1_HOST` env vars > `~/.nx1/profiles` YAML
- Legacy env vars `LAKEHOUSE_API_KEY`/`LAKEHOUSE_HOST` also supported

### File Ingestion Pipeline

`ingest_local_file()` orchestrates: upload file via `FilesClient` -> submit ingestion job -> optionally wait for completion via `JobsClient`. The `DataIngestionClient` receives `FilesClient` and `JobsClient` references via `_set_clients()`.

### Adding Constants

1. File format support: add to `FILE_FORMAT_OPTIONS`, `FILE_EXTENSION_MAP`, and `CONTENT_TYPES` in `constants.py`
2. All three dicts must stay in sync for a format to work end-to-end

### URL Building

`BaseClient._build_url()` joins path components with `/`. Service methods pass each path segment as a separate argument: `self._client.get("api", "metastore", "catalogs", catalog, "schemas")`. Do not pre-join paths.

### CLI Output

All CLI commands return data from `_execute_command()` or handler functions. The `format_output()` function handles JSON/YAML/table rendering. Print side-effect messages (success/error) directly; return `None` to suppress format_output.

## Gotchas

- `BaseClient` forces HTTPS: it replaces `http://` with `https://` and adds `https://` if no scheme. Do not try to use plain HTTP.
- `xlsx` maps to format `"xls"` internally (see `FILE_EXTENSION_MAP`). Both use the same Spark reader.
- The `DataIngestionClient` depends on `FilesClient` and `JobsClient` being injected via `_set_clients()` after construction. This coupling exists because `NX1Client` orchestrates the wiring.
- CLI uses emojis in error/success messages. Keep this pattern when adding new CLI output.
- `cli.py` has its own `resolve_config(args)` function (YAML-based config merge) separate from `profiles.resolve_config()`. They serve different purposes.
- The `services/` directory has non-NX1 clients (Airflow, Kyuubi, Superset, JupyterHub) that bypass `NX1Client` and have their own auth. These are invoked directly from CLI handlers, not through the standard service client pattern.
- Test coverage is minimal. Only `ColumnTransformation`, client init, enums, and ingestion format detection are tested. Service client methods have no tests.
- The workflow file (`app_deployment.yml`) is a template/example with hardcoded references. It does not represent actual CI for this repo.
