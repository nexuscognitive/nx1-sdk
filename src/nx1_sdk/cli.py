"""Command-line interface for the NX1 SDK."""

import argparse
import json
import logging
import sys
from typing import Any, Dict, List, Optional
import os
import getpass

import yaml
from tabulate import tabulate

from nx1_sdk.client import NX1Client
from nx1_sdk.services.airflow_trigger_service import AirflowTriggererClient
from nx1_sdk.services.kyuubi_service import KyuubiBatchSubmitterClient
from nx1_sdk.exceptions import NX1APIError, NX1TimeoutError, NX1ValidationError
from nx1_sdk.transformations import ColumnTransformation
from nx1_sdk.profiles import (
    list_profiles,
    save_profile,
    delete_profile,
    get_profile_path,
)

def load_yaml_config(config_file):
    """Load configuration from YAML file."""
    with open(config_file, "r") as f:
        return yaml.safe_load(f)

def get_password(args, username, env_var, logger_name=None):
    """Get password from args, env var, or prompt."""
    password = getattr(args, "password", None)

    if not password:
        password = os.environ.get(env_var)
        if password and logger_name:
            logging.getLogger(logger_name).debug(
                f"Using password from {env_var} environment variable"
            )

    if not password:
        password = getpass.getpass(f"Password for {username}: ")

    return password

def resolve_config(args):
    """
    Load YAML config and merge into args.
    Priority: CLI > YAML
    Mutates args in-place.
    """
    yaml_config = None
    if getattr(args, "config_file", None):
        yaml_config = load_yaml_config(args.config_file)

    if not yaml_config:
        return args

    for key, value in yaml_config.items():
        # If CLI did NOT provide this value, take from YAML
        if not hasattr(args, key) or getattr(args, key) is None:
            setattr(args, key, value)

def validate_required(args, required_fields):
    missing = [
        field for field in required_fields
        if getattr(args, field, None) is None
    ]
    if missing:
        raise ValueError(
            f"Missing required arguments: {', '.join(missing)}"
        )

def format_output(data: Any, output_format: str = "json") -> str:
    """Format output data in the specified format."""
    if output_format == "yaml":
        return yaml.dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True)
    elif output_format == "table":
        return format_as_table(data)
    else:  # json (default)
        return json.dumps(data, indent=2, default=str)


def format_as_table(data: Any) -> str:
    """Format data as a table."""
    if isinstance(data, list):
        if not data:
            return "No data"
        if isinstance(data[0], dict):
            headers = list(data[0].keys())
            rows = [[_truncate(row.get(h, "")) for h in headers] for row in data]
            return tabulate(rows, headers=headers, tablefmt="simple")
        else:
            return tabulate([[item] for item in data], tablefmt="simple")
    elif isinstance(data, dict):
        rows = [[k, _truncate(v)] for k, v in data.items()]
        return tabulate(rows, headers=["Key", "Value"], tablefmt="simple")
    else:
        return str(data)


def _truncate(value: Any, max_length: int = 80) -> str:
    """Truncate long values for table display."""
    str_val = str(value) if value is not None else ""
    if len(str_val) > max_length:
        return str_val[:max_length - 3] + "..."
    return str_val


# Global arguments parser (shared by all subcommands)
def add_global_arguments(parser: argparse.ArgumentParser) -> None:
    """Add global arguments to a parser."""
    parser.add_argument("--api-key", dest="api_key", help="API key")
    parser.add_argument("--host", help="API host")
    parser.add_argument("--profile", "-p", help="Profile name from ~/.nx1/profiles")
    parser.add_argument("--no-verify-ssl", action="store_true", help="Disable SSL verification")
    parser.add_argument("--config-file", help="YAML config file")
    parser.add_argument("--timeout", type=int, default=None, help="Request timeout (default: 30)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("-o", "--output", choices=["json", "yaml", "table"], default="json", help="Output format")


def main():
    """CLI entry point for NX1 SDK operations."""
    
    # Parent parser with global arguments (will be inherited by all subparsers)
    parent_parser = argparse.ArgumentParser(add_help=False)
    add_global_arguments(parent_parser)
    
    # Main parser
    parser = argparse.ArgumentParser(
        description="NX1 NLP Agent SDK CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  nx1 ping
  nx1 domains -o table
  nx1 --timeout 60 jobs list
  nx1 jobs list --timeout 60 -o table
  nx1 s3 --profile dev list
  nx1 --profile staging -o yaml domains
  nx1 ingest-file --file data.csv --table customers --schema staging --timeout 300

Global options (can appear anywhere):
  --api-key KEY       API key (or NX1_API_KEY env var)
  --host URL          API host (or NX1_HOST env var)  
  --profile, -p NAME  Profile from ~/.nx1/profiles
  --timeout SECS      Request timeout (default: 30)
  --no-verify-ssl     Disable SSL verification
  --config-file       YAML config file
  -o, --output FMT    Output: json, yaml, table (default: json)
  -v, --verbose       Verbose output

Configuration Priority:
  1. CLI arguments
  2. Environment variables (NX1_API_KEY, NX1_HOST)
  3. Profile (~/.nx1/profiles)
"""
    )
    
    # Add global arguments to main parser too
    add_global_arguments(parser)
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # -------------------------------------------------------------------------
    # Health commands
    # -------------------------------------------------------------------------
    subparsers.add_parser("ping", parents=[parent_parser], help="Health check")
    subparsers.add_parser("version", parents=[parent_parser], help="Get API version")
    
    # -------------------------------------------------------------------------
    # Metastore commands
    # -------------------------------------------------------------------------
    subparsers.add_parser("domains", parents=[parent_parser], help="List domains")
    subparsers.add_parser("catalogs", parents=[parent_parser], help="List catalogs")
    subparsers.add_parser("tags", parents=[parent_parser], help="List all tags")
    subparsers.add_parser("engines", parents=[parent_parser], help="List supported engines")
    
    schemas_p = subparsers.add_parser("schemas", parents=[parent_parser], help="List schemas")
    schemas_p.add_argument("--catalog")
    
    tables_p = subparsers.add_parser("tables", parents=[parent_parser], help="List tables")
    tables_p.add_argument("--catalog")
    tables_p.add_argument("--schema")
    
    columns_p = subparsers.add_parser("columns", parents=[parent_parser], help="List columns")
    columns_p.add_argument("--catalog")
    columns_p.add_argument("--schema")
    columns_p.add_argument("--table")
    
    # -------------------------------------------------------------------------
    # Query commands
    # -------------------------------------------------------------------------
    ask_p = subparsers.add_parser("ask", parents=[parent_parser], help="Ask a question")
    ask_p.add_argument("--domain")
    ask_p.add_argument("--prompt")
    
    suggest_p = subparsers.add_parser("suggest", parents=[parent_parser], help="Get suggestions")
    suggest_p.add_argument("--domain")
    
    # -------------------------------------------------------------------------
    # File Ingestion
    # -------------------------------------------------------------------------
    ingest_file_p = subparsers.add_parser("ingest-file", parents=[parent_parser], help="Upload and ingest local file")
    ingest_file_p.add_argument("--file", dest="file_path")
    ingest_file_p.add_argument("--table")
    ingest_file_p.add_argument("--schema", dest="schema_name")
    ingest_file_p.add_argument("--name", dest="job_name", help="Custom job name")
    ingest_file_p.add_argument("--mode", default="overwrite", choices=["append", "overwrite", "merge"])
    ingest_file_p.add_argument("--merge-keys", help="Comma-separated merge keys")
    ingest_file_p.add_argument("--delimiter", default=",")
    ingest_file_p.add_argument("--header", default="true", choices=["true", "false"])
    ingest_file_p.add_argument("--domain", dest="ingest_domain")
    ingest_file_p.add_argument("--tags", help="Comma-separated tags")
    ingest_file_p.add_argument("--cast", action="append", metavar="COL:TYPE")
    ingest_file_p.add_argument("--rename", action="append", metavar="OLD:NEW")
    ingest_file_p.add_argument("--encrypt", action="append", metavar="COL")
    ingest_file_p.add_argument("--no-wait", action="store_true")
    ingest_file_p.add_argument("--max-wait", type=int, default=300)
    
    # Direct Ingestion
    ingest_p = subparsers.add_parser("ingest", parents=[parent_parser], help="Submit ingestion from S3")
    ingest_p.add_argument("--name")
    ingest_p.add_argument("--table")
    ingest_p.add_argument("--schema", dest="schema_name")
    ingest_p.add_argument("--type", dest="ingesttype", default="file", choices=["file", "jdbc", "lakehouse"])
    ingest_p.add_argument("--mode", default="overwrite", choices=["append", "overwrite", "merge"])
    ingest_p.add_argument("--file-path")
    ingest_p.add_argument("--file-id")
    ingest_p.add_argument("--format", dest="file_format", default="csv")
    ingest_p.add_argument("--delimiter", default=",")
    ingest_p.add_argument("--header", default="true", choices=["true", "false"])
    ingest_p.add_argument("--merge-keys")
    ingest_p.add_argument("--domain", dest="ingest_domain")
    ingest_p.add_argument("--tags")
    ingest_p.add_argument("--schedule")
    
    subparsers.add_parser("ingest-options", parents=[parent_parser], help="Get file ingestion options")
    
    # -------------------------------------------------------------------------
    # Jobs commands
    # -------------------------------------------------------------------------
    jobs_p = subparsers.add_parser("jobs", parents=[parent_parser], help="Job management")
    jobs_sub = jobs_p.add_subparsers(dest="jobs_command")
    jobs_sub.add_parser("list", parents=[parent_parser])
    jobs_get = jobs_sub.add_parser("get", parents=[parent_parser])
    jobs_get.add_argument("job_id")
    jobs_del = jobs_sub.add_parser("delete", parents=[parent_parser])
    jobs_del.add_argument("job_id")
    jobs_trig = jobs_sub.add_parser("trigger", parents=[parent_parser])
    jobs_trig.add_argument("job_id")
    jobs_wait = jobs_sub.add_parser("wait", parents=[parent_parser])
    jobs_wait.add_argument("job_id")
    jobs_wait.add_argument("--max-wait", type=int, default=300)
    jobs_wait.add_argument("--poll-interval", type=int, default=5)
    
    # -------------------------------------------------------------------------
    # Files commands
    # -------------------------------------------------------------------------
    files_p = subparsers.add_parser("files", parents=[parent_parser], help="File management")
    files_sub = files_p.add_subparsers(dest="files_command")
    files_sub.add_parser("list", parents=[parent_parser])
    files_up = files_sub.add_parser("upload", parents=[parent_parser])
    files_up.add_argument("file_path")
    files_up.add_argument("--name")
    files_url = files_sub.add_parser("upload-url", parents=[parent_parser])
    files_url.add_argument("url")
    files_url.add_argument("--name")
    files_get = files_sub.add_parser("get", parents=[parent_parser])
    files_get.add_argument("file_id")
    files_del = files_sub.add_parser("delete", parents=[parent_parser])
    files_del.add_argument("file_id")
    
    # -------------------------------------------------------------------------
    # S3 commands
    # -------------------------------------------------------------------------
    s3_p = subparsers.add_parser("s3", parents=[parent_parser], help="S3 bucket management")
    s3_sub = s3_p.add_subparsers(dest="s3_command")
    s3_sub.add_parser("list", parents=[parent_parser])
    s3_sub.add_parser("refresh", parents=[parent_parser])
    s3_get = s3_sub.add_parser("get", parents=[parent_parser])
    s3_get.add_argument("bucket_name")
    s3_create = s3_sub.add_parser("create", parents=[parent_parser])
    s3_create.add_argument("--bucket")
    s3_create.add_argument("--endpoint")
    s3_create.add_argument("--access-key")
    s3_create.add_argument("--secret-key")
    s3_del = s3_sub.add_parser("delete", parents=[parent_parser])
    s3_del.add_argument("bucket_name")
    
    # -------------------------------------------------------------------------
    # DQ commands
    # -------------------------------------------------------------------------
    dq_p = subparsers.add_parser("dq", parents=[parent_parser], help="Data quality")
    dq_sub = dq_p.add_subparsers(dest="dq_command")
    dq_suggest = dq_sub.add_parser("suggest", parents=[parent_parser])
    dq_suggest.add_argument("--table")
    dq_suggest.add_argument("--request")
    dq_rules = dq_sub.add_parser("rules", parents=[parent_parser])
    dq_rules.add_argument("--table")
    dq_rules.add_argument("--accepted-only", action="store_true")
    dq_run = dq_sub.add_parser("run", parents=[parent_parser])
    dq_run.add_argument("--table")
    dq_acc = dq_sub.add_parser("accept", parents=[parent_parser])
    dq_acc.add_argument("dq_id")
    dq_del = dq_sub.add_parser("delete", parents=[parent_parser])
    dq_del.add_argument("dq_id")
    
    # -------------------------------------------------------------------------
    # Data Products commands
    # -------------------------------------------------------------------------
    dp_p = subparsers.add_parser("dataproducts", parents=[parent_parser], help="Data products")
    dp_sub = dp_p.add_subparsers(dest="dp_command")
    dp_sub.add_parser("list", parents=[parent_parser])
    dp_get = dp_sub.add_parser("get", parents=[parent_parser])
    dp_get.add_argument("dataproduct_id")
    dp_del = dp_sub.add_parser("delete", parents=[parent_parser])
    dp_del.add_argument("dataproduct_id")
    
    # -------------------------------------------------------------------------
    # Apps commands
    # -------------------------------------------------------------------------
    apps_p = subparsers.add_parser("apps", parents=[parent_parser], help="Apps management")
    apps_sub = apps_p.add_subparsers(dest="apps_command")
    apps_sub.add_parser("list", parents=[parent_parser])
    apps_create = apps_sub.add_parser("create", parents=[parent_parser])
    apps_create.add_argument("--name")
    apps_get = apps_sub.add_parser("get", parents=[parent_parser])
    apps_get.add_argument("app_id")
    apps_del = apps_sub.add_parser("delete", parents=[parent_parser])
    apps_del.add_argument("app_id")
    apps_vers = apps_sub.add_parser("versions", parents=[parent_parser])
    apps_vers.add_argument("--app-id")
    apps_cv = apps_sub.add_parser("create-version", parents=[parent_parser])
    apps_cv.add_argument("--app-id")
    apps_cv.add_argument("--name")
    apps_act = apps_sub.add_parser("activate", parents=[parent_parser])
    apps_act.add_argument("version_id")
    apps_roles = apps_sub.add_parser("roles", parents=[parent_parser])
    apps_roles.add_argument("--app-id")
    apps_cr = apps_sub.add_parser("create-role", parents=[parent_parser])
    apps_cr.add_argument("--app-id")
    apps_cr.add_argument("--name")
    apps_comp = apps_sub.add_parser("components", parents=[parent_parser])
    apps_comp.add_argument("--version-id")
    apps_dag = apps_sub.add_parser("add-dag", parents=[parent_parser])
    apps_dag.add_argument("--version-id")
    apps_dag.add_argument("--file")
    apps_dag.add_argument("--name")
    
    # -------------------------------------------------------------------------
    # Mirror commands
    # -------------------------------------------------------------------------
    mirror_p = subparsers.add_parser("mirror", parents=[parent_parser], help="Create mirroring job")
    mirror_p.add_argument("--name")
    mirror_p.add_argument("--source-catalog")
    mirror_p.add_argument("--source-schema")
    mirror_p.add_argument("--source-table")
    mirror_p.add_argument("--target-catalog")
    mirror_p.add_argument("--target-schema")
    mirror_p.add_argument("--target-table")
    mirror_p.add_argument("--mode", default="overwrite", choices=["append", "overwrite", "merge"])
    mirror_p.add_argument("--schedule")
    mirror_p.add_argument("--merge-keys")
    
    # -------------------------------------------------------------------------
    # Shares commands
    # -------------------------------------------------------------------------
    shares_p = subparsers.add_parser("shares", parents=[parent_parser], help="Data shares")
    shares_sub = shares_p.add_subparsers(dest="shares_command")
    shares_sub.add_parser("list", parents=[parent_parser])
    shares_get = shares_sub.add_parser("get", parents=[parent_parser])
    shares_get.add_argument("share_id")
    shares_del = shares_sub.add_parser("delete", parents=[parent_parser])
    shares_del.add_argument("share_id")
    
    # -------------------------------------------------------------------------
    # Crews commands
    # -------------------------------------------------------------------------
    crews_p = subparsers.add_parser("crews", parents=[parent_parser], help="AI Crews")
    crews_sub = crews_p.add_subparsers(dest="crews_command")
    crews_sub.add_parser("list", parents=[parent_parser])
    crews_st = crews_sub.add_parser("status", parents=[parent_parser])
    crews_st.add_argument("task_id")
    
    # -------------------------------------------------------------------------
    # Profile management commands
    # -------------------------------------------------------------------------
    profile_p = subparsers.add_parser("profile", help="Manage profiles (~/.nx1/profiles)")
    profile_sub = profile_p.add_subparsers(dest="profile_command")
    profile_sub.add_parser("list", parents=[parent_parser], help="List all profiles")
    profile_sub.add_parser("path", parents=[parent_parser], help="Show profiles file path")
    profile_add = profile_sub.add_parser("add", help="Add/update a profile")
    profile_add.add_argument("--name", help="Profile name")
    profile_add.add_argument("--host", help="API host URL")
    profile_add.add_argument("--api-key", help="API key (PSK)")
    profile_add.add_argument("--no-verify-ssl", action="store_true", help="Disable SSL verification")
    profile_add.add_argument("--timeout", type=int, default=30, help="Request timeout")
    profile_add.add_argument("-o", "--output", choices=["json", "yaml", "table"], default="json")
    profile_rm = profile_sub.add_parser("remove", help="Remove a profile")
    profile_rm.add_argument("name", help="Profile name to remove")
    profile_show = profile_sub.add_parser("show", parents=[parent_parser], help="Show a profile")
    profile_show.add_argument("name", help="Profile name to show")

    # -------------------------------------------------------------------------
    # Airflow Triggerer commands
    # -------------------------------------------------------------------------
    airflow_p = subparsers.add_parser("airflow", parents=[parent_parser], help="Trigger Airflow DAGs")
    airflow_p.add_argument("--url", help="Airflow base URL")
    airflow_p.add_argument("--dag", help="DAG ID")
    airflow_p.add_argument("--username", help="Airflow username")
    airflow_p.add_argument("--password", help="Airflow password")
    airflow_p.add_argument("--conf", default="{}", help="DAG run config (JSON)")
    airflow_p.add_argument("--debug", action="store_true", help="Enable debug logging")

    # -------------------------------------------------------------------------
    # PKyuubi commands
    # -------------------------------------------------------------------------
    kyuubi_p = subparsers.add_parser("kyuubi", parents=[parent_parser], help="Submit Kyuubi batch job")
    kyuubi_p.add_argument("--server", help="Kyuubi server URL")
    kyuubi_p.add_argument("--history-server", help="Spark history server URL")
    kyuubi_p.add_argument("--username", help="Username")
    kyuubi_p.add_argument("--password", help="Password")
    kyuubi_p.add_argument("--resource", help="Jar / Py file")
    kyuubi_p.add_argument("--classname", help="Main class")
    kyuubi_p.add_argument("--name", help="Job name")
    kyuubi_p.add_argument("--queue", help="YuniKorn queue")
    kyuubi_p.add_argument("--args", help="Job arguments")
    kyuubi_p.add_argument("--conf", help="Spark configs")
    kyuubi_p.add_argument("--pyfiles", help="PyFiles")
    kyuubi_p.add_argument("--jars", help="Jars")
    kyuubi_p.add_argument("--files", help="Files")
    kyuubi_p.add_argument("--show-logs", action="store_true")
    kyuubi_p.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    # -------------------------------------------------------------------------
    # Parse arguments
    # -------------------------------------------------------------------------
    args = parser.parse_args()
    resolve_config(args)

    if not args.command:
        parser.print_help()
        return
    
    # Setup logging
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    try:
        # Handle profile management commands first (don't need client)
        if args.command == "profile":
            _handle_profile_command(args)
            return
        
        # Create client with resolved configuration based on command
        if args.command == "airflow":
            result = _handle_airflow(args)
        elif args.command == "kyuubi":
            result = _handle_kyuubi(args)
        else:
            client = NX1Client(
                api_key=args.api_key,
                host=args.host,
                profile=args.profile,
                verify_ssl=False if args.no_verify_ssl else None,
                timeout=args.timeout
            )
            result = _execute_command(client, args)
        
        if result is not None:
            print(format_output(result, args.output))
    
    except NX1ValidationError as e:
        print(f"❌ Validation Error: {e}", file=sys.stderr)
        sys.exit(1)
    except NX1APIError as e:
        print(f"❌ API Error: {e}", file=sys.stderr)
        sys.exit(1)
    except NX1TimeoutError as e:
        print(f"⏱️ Timeout: {e}", file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"❌ File not found: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        if hasattr(args, 'verbose') and args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def _handle_profile_command(args) -> None:
    """Handle profile management commands."""
    validate_required(args, ["name"])
    cmd = args.profile_command
    output_fmt = getattr(args, 'output', 'json')
    
    if cmd == "list" or not cmd:
        profiles = list_profiles()
        if not profiles:
            print(f"No profiles found. Create one with: nx1 profile add --name <n> --host <url> --api-key <key>")
            print(f"Profiles file: {get_profile_path()}")
            return
        # Hide API keys in output
        safe_profiles = {}
        for name, config in profiles.items():
            safe_config = dict(config)
            if "api_key" in safe_config:
                key = safe_config["api_key"]
                safe_config["api_key"] = key[:8] + "..." + key[-4:] if len(key) > 12 else "***"
            safe_profiles[name] = safe_config
        print(format_output(safe_profiles, output_fmt))
    
    elif cmd == "path":
        print(get_profile_path())
    
    elif cmd == "add":
        save_profile(
            profile_name=args.name,
            host=args.host,
            api_key=args.api_key,
            verify_ssl=not args.no_verify_ssl,
            timeout=args.timeout
        )
        print(f"✅ Profile '{args.name}' saved to {get_profile_path()}")
    
    elif cmd == "remove":
        if delete_profile(args.name):
            print(f"✅ Profile '{args.name}' removed")
        else:
            print(f"❌ Profile '{args.name}' not found")
    
    elif cmd == "show":
        from nx1_sdk.profiles import get_profile
        profile_config = get_profile(args.name)
        safe_config = dict(profile_config)
        if "api_key" in safe_config:
            key = safe_config["api_key"]
            safe_config["api_key"] = key[:8] + "..." + key[-4:] if len(key) > 12 else "***"
        print(format_output(safe_config, output_fmt))


def _execute_command(client: NX1Client, args) -> Optional[Any]:
    """Execute the requested command and return result."""
    
    # Health commands
    if args.command == "ping":
        return client.health.ping()
    elif args.command == "version":
        print(client.health.version())
        return None
    
    # Metastore commands
    elif args.command == "domains":
        return client.metastore.get_domains()
    elif args.command == "catalogs":
        return client.metastore.get_catalogs()
    elif args.command == "schemas":
        validate_required(args, ["catalog"])
        return client.metastore.get_schemas(args.catalog)
    elif args.command == "tables":
        validate_required(args, ["catalog","schema"])
        return client.metastore.get_tables(args.catalog, args.schema)
    elif args.command == "columns":
        validate_required(args, ["catalog","schema","table"])
        return client.metastore.get_columns(args.catalog, args.schema, args.table)
    elif args.command == "tags":
        return client.metastore.get_tags()
    elif args.command == "engines":
        return client.metastore.get_engines()
    
    # Query commands
    elif args.command == "ask":
        validate_required(args, ["domain","prompt"])
        return client.queries.ask(args.domain, args.prompt)
    elif args.command == "suggest":
        validate_required(args, ["domain"])
        return client.queries.suggest(args.domain)
    
    # Ingest local file
    elif args.command == "ingest-file":
        return _handle_ingest_file(client, args)
    
    # Direct ingestion
    elif args.command == "ingest":
        return _handle_ingest(client, args)
    
    elif args.command == "ingest-options":
        return client.ingestion.get_file_ingestion_options()
    
    # Jobs commands
    elif args.command == "jobs":
        return _handle_jobs(client, args)
    
    # Files commands
    elif args.command == "files":
        return _handle_files(client, args)
    
    # S3 commands
    elif args.command == "s3":
        return _handle_s3(client, args)
    
    # DQ commands
    elif args.command == "dq":
        return _handle_dq(client, args)
    
    # Data Products commands
    elif args.command == "dataproducts":
        return _handle_dataproducts(client, args)
    
    # Apps commands
    elif args.command == "apps":
        return _handle_apps(client, args)
    
    # Mirror commands
    elif args.command == "mirror":
        return _handle_mirror(client, args)
    
    # Shares commands
    elif args.command == "shares":
        return _handle_shares(client, args)
    
    # Crews commands
    elif args.command == "crews":
        return _handle_crews(client, args)
    
    return None


def _handle_ingest_file(client: NX1Client, args) -> None:
    """Handle ingest-file command."""
    validate_required(args, ["file_path", "table", "schema_name"])
    transformations = []
    if args.cast:
        for c in args.cast:
            for item in c.split(','):
                col, dtype = item.strip().split(':', 1)
                transformations.append(ColumnTransformation.cast(col.strip(), dtype.strip()))
    
    if args.rename:
        for r in args.rename:
            for item in r.split(','):
                old, new = item.strip().split(':', 1)
                transformations.append(ColumnTransformation.rename(old.strip(), new.strip()))
    
    if args.encrypt:
        for e in args.encrypt:
            for col in e.split(','):
                transformations.append(ColumnTransformation.encrypt(col.strip()))
    
    merge_keys = args.merge_keys.split(",") if args.merge_keys else None
    tags = args.tags.split(",") if args.tags else None
    domain = getattr(args, 'ingest_domain', None)
    
    job_id = client.ingestion.ingest_local_file(
        file_path=args.file_path,
        table=args.table,
        schema_name=args.schema_name,
        mode=args.mode,
        merge_keys=merge_keys,
        column_transformations=transformations if transformations else None,
        job_name=args.job_name,
        header=args.header,
        delimiter=args.delimiter,
        domain=domain,
        tags=tags,
        wait_for_completion=not args.no_wait,
        max_wait=args.max_wait,
        verbose=True
    )
    print(f"\n🎉 Pipeline completed! Job ID: {job_id}")
    return None


def _handle_ingest(client: NX1Client, args) -> None:
    """Handle ingest command."""
    validate_required(args, ["name","table","schema_name"])
    file_opts = {"header": args.header, "inferSchema": "true", "delimiter": args.delimiter}
    merge_keys = args.merge_keys.split(",") if args.merge_keys else None
    tags = args.tags.split(",") if args.tags else None
    domain = getattr(args, 'ingest_domain', None)
    
    result = client.ingestion.submit(
        name=args.name,
        ingesttype=args.ingesttype,
        schema_name=args.schema_name,
        table=args.table,
        mode=args.mode,
        file_path=args.file_path,
        file_id=args.file_id,
        file_format=args.file_format,
        file_read_options=file_opts,
        merge_keys=merge_keys,
        domain=domain,
        tags=tags,
        schedule=args.schedule
    )
    print(f"Job ID: {result.get('job_id')}, Flow URL: {result.get('flow_url', 'N/A')}")
    return None


def _handle_jobs(client: NX1Client, args) -> Optional[Any]:
    """Handle jobs commands."""
    cmd = args.jobs_command
    if cmd in ("get","delete","trigger","wait"):
        validate_required(args, ["job_id"])
    if cmd == "list" or not cmd:
        return client.jobs.get_all()
    elif cmd == "get":
        return client.jobs.get(args.job_id)
    elif cmd == "delete":
        client.jobs.delete(args.job_id)
        print(f"✅ Deleted: {args.job_id}")
    elif cmd == "trigger":
        client.jobs.trigger(args.job_id)
        print(f"✅ Triggered: {args.job_id}")
    elif cmd == "wait":
        result = client.jobs.wait_for_completion(args.job_id, args.max_wait, args.poll_interval)
        print(f"✅ Completed: {result.get('status')}")
        return result
    return None


def _handle_files(client: NX1Client, args) -> Optional[Any]:
    """Handle files commands."""
    cmd = args.files_command
    if cmd in ("get","delete"):
        validate_required(args, ["file_id"])
    if cmd == "list" or not cmd:
        return client.files.get_all()
    elif cmd == "upload":
        validate_required(args, ["file_path","name"])
        result = client.files.upload(args.file_path, args.name)
        print(f"✅ Uploaded: {result.get('id')}, S3: {result.get('s3_url')}")
    elif cmd == "upload-url":
        validate_required(args, ["url","name"])
        result = client.files.upload_from_url(args.url, args.name)
        print(f"✅ Uploaded: {result.get('id')}, S3: {result.get('s3_url')}")
    elif cmd == "get":
        return client.files.get(args.file_id)
    elif cmd == "delete":
        client.files.delete(args.file_id)
        print(f"✅ Deleted: {args.file_id}")
    return None


def _handle_s3(client: NX1Client, args) -> Optional[Any]:
    """Handle s3 commands."""
    cmd = args.s3_command
    if cmd in ("get","delete"):
        validate_required(args, ["bucket_name"])
    if cmd == "list" or not cmd:
        return client.s3.get_buckets()
    elif cmd == "refresh":
        client.s3.refresh()
        print("✅ Refreshed")
    elif cmd == "get":
        return client.s3.get_bucket(args.bucket_name)
    elif cmd == "create":
        validate_required(args, ["bucket","endpoint","access_key","secret_key"])
        client.s3.create_bucket(args.bucket, args.endpoint, args.access_key, args.secret_key)
        print(f"✅ Created: {args.bucket}")
    elif cmd == "delete":
        client.s3.delete_bucket(args.bucket_name)
        print(f"✅ Deleted: {args.bucket_name}")
    return None


def _handle_dq(client: NX1Client, args) -> Optional[Any]:
    """Handle dq commands."""
    cmd = args.dq_command
    if cmd == "suggest":
        return client.data_quality.suggest(args.table, getattr(args, 'request', None))
    elif cmd == "rules":
        return client.data_quality.get_rules_by_table(args.table, args.accepted_only)
    elif cmd == "run":
        return client.data_quality.run_report_by_table(args.table)
    elif cmd == "accept":
        client.data_quality.accept(args.dq_id)
        print(f"✅ Accepted: {args.dq_id}")
    elif cmd == "delete":
        client.data_quality.delete_rule(args.dq_id)
        print(f"✅ Deleted: {args.dq_id}")
    return None


def _handle_dataproducts(client: NX1Client, args) -> Optional[Any]:
    """Handle dataproducts commands."""
    cmd = args.dp_command
    if cmd == "list" or not cmd:
        return client.data_products.get_all()
    elif cmd == "get":
        return client.data_products.get(args.dataproduct_id)
    elif cmd == "delete":
        client.data_products.delete(args.dataproduct_id)
        print(f"✅ Deleted")
    return None


def _handle_apps(client: NX1Client, args) -> Optional[Any]:
    """Handle apps commands."""
    cmd = args.apps_command
    if cmd == "list" or not cmd:
        return client.apps.get_all()
    elif cmd == "create":
        result = client.apps.create(args.name)
        print(f"✅ Created: {result.get('id')}")
        return result
    elif cmd == "get":
        return client.apps.get(args.app_id)
    elif cmd == "delete":
        client.apps.delete(args.app_id)
        print(f"✅ Deleted")
    elif cmd == "versions":
        return client.apps.get_versions(args.app_id)
    elif cmd == "create-version":
        result = client.apps.create_version(args.app_id, args.name)
        print(f"✅ Version created: {result.get('id')}")
        return result
    elif cmd == "activate":
        client.apps.activate_version(args.version_id)
        print(f"✅ Activated")
    elif cmd == "roles":
        return client.apps.get_roles(args.app_id)
    elif cmd == "create-role":
        result = client.apps.create_role(args.app_id, args.name)
        print(f"✅ Role created: {result.get('id')}")
        return result
    elif cmd == "components":
        return client.apps.get_components(args.version_id)
    elif cmd == "add-dag":
        result = client.apps.add_dag(args.version_id, args.file, args.name)
        print(f"✅ DAG added: {result.get('id')}")
        return result
    return None


def _handle_mirror(client: NX1Client, args) -> Optional[Any]:
    """Handle mirror command."""
    validate_required(args, ["name","source_catalog","source_schema","source_table","target_catalog","target_schema","target_table"])
    merge_keys = args.merge_keys.split(",") if args.merge_keys else None
    result = client.mirroring.create(
        job_name=args.name,
        source_catalog=args.source_catalog,
        source_schema=args.source_schema,
        source_table=args.source_table,
        target_catalog=args.target_catalog,
        target_schema=args.target_schema,
        target_table=args.target_table,
        mode=args.mode,
        schedule=args.schedule,
        merge_keys=merge_keys
    )
    print(f"✅ Mirroring job created: {result.get('job_id')}")
    return result


def _handle_shares(client: NX1Client, args) -> Optional[Any]:
    """Handle shares commands."""
    cmd = args.shares_command
    if cmd in ("get","delete"):
        validate_required(args, ["share_id"])
    if cmd == "list" or not cmd:
        return client.data_shares.get_all()
    elif cmd == "get":
        return client.data_shares.get(args.share_id)
    elif cmd == "delete":
        client.data_shares.delete(args.share_id)
        print(f"✅ Deleted")
    return None


def _handle_crews(client: NX1Client, args) -> Optional[Any]:
    """Handle crews commands."""
    cmd = args.crews_command
    if cmd == "list" or not cmd:
        return client.crews.list_crews()
    elif cmd == "status":
        validate_required(args, ["task_id"])
        return client.crews.get_crew_status(args.task_id)
    return None

def _handle_airflow(args):
    validate_required(args, ["url", "username", "dag"])

    password = get_password(
        args,
        args.username,
        env_var="AIRFLOW_TRIGGER_PASSWORD",
    )

    conf_dict = json.loads(getattr(args, "conf", "{}"))

    client = AirflowTriggererClient(
        airflow_url=args.url,
        username=args.username,
        password=password,
    )

    dag_run_id = client.trigger_dag(args.dag, conf_dict)
    final_state = client.monitor_dag(args.dag, dag_run_id)

    return {
        "dag_run_id": dag_run_id,
        "final_state": final_state,
    }


def _handle_kyuubi(args):
    validate_required(args, ["server", "username", "resource", "name"])

    inject_yunikorn_spark_configs(args)

    password = get_password(
        args,
        args.username,
        env_var="KYUUBI_SUBMIT_PASSWORD",
    )

    submitter = KyuubiBatchSubmitterClient(
        server=args.server,
        username=args.username,
        password=password,
        history_server=getattr(args, "history_server", None),
    )

    batch_id = submitter.submit_batch(
        resource=args.resource,
        classname=getattr(args, "classname", None),
        name=args.name,
        args=getattr(args, "args", None),
        conf=getattr(args, "conf", None),
        pyfiles=getattr(args, "pyfiles", None),
        jars=getattr(args, "jars", None),
        files=getattr(args, "files", None),
    )

    final_state = submitter.monitor_job(
        batch_id,
        show_logs=getattr(args, "show_logs", False),
    )

    return {
        "batch_id": batch_id,
        "final_state": final_state,
    }

if __name__ == "__main__":
    main()
