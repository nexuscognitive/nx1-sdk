"""Command-line interface for the NX1 SDK."""

import argparse
import json
import logging
import sys

from nx1_sdk.client import NX1Client
from nx1_sdk.exceptions import NX1APIError, NX1TimeoutError, NX1ValidationError
from nx1_sdk.transformations import ColumnTransformation


def main():
    """CLI entry point for NX1 SDK operations."""
    parser = argparse.ArgumentParser(
        description="NX1 NLP Agent SDK CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  nx1 ping
  nx1 domains
  nx1 ask --domain "Sales Data" --prompt "Show top 10 customers"
  nx1 ingest-file --file data.csv --table customers --schema staging
  nx1 ingest-file --file data.csv --table employees --schema hr --cast "hire_date:date" --encrypt ssn
  nx1 jobs list
  nx1 apps list
  nx1 apps create --name my-app

Environment Variables:
  NX1_API_KEY    API key for authentication
  NX1_HOST       API host URL
"""
    )
    
    # Global arguments
    parser.add_argument("--api-key", help="API key (or set NX1_API_KEY env var)")
    parser.add_argument("--host", help="API host (or set NX1_HOST env var)")
    parser.add_argument("--no-verify-ssl", action="store_true", help="Disable SSL verification")
    parser.add_argument("--timeout", type=int, default=30, help="Request timeout in seconds")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Health commands
    subparsers.add_parser("ping", help="Health check")
    subparsers.add_parser("version", help="Get API version")
    
    # Metastore commands
    subparsers.add_parser("domains", help="List domains")
    subparsers.add_parser("catalogs", help="List catalogs")
    subparsers.add_parser("tags", help="List all tags")
    subparsers.add_parser("engines", help="List supported engines")
    
    schemas_p = subparsers.add_parser("schemas", help="List schemas")
    schemas_p.add_argument("--catalog", required=True)
    
    tables_p = subparsers.add_parser("tables", help="List tables")
    tables_p.add_argument("--catalog", required=True)
    tables_p.add_argument("--schema", required=True)
    
    columns_p = subparsers.add_parser("columns", help="List columns")
    columns_p.add_argument("--catalog", required=True)
    columns_p.add_argument("--schema", required=True)
    columns_p.add_argument("--table", required=True)
    
    # Query commands
    ask_p = subparsers.add_parser("ask", help="Ask a question")
    ask_p.add_argument("--domain", required=True)
    ask_p.add_argument("--prompt", required=True)
    
    suggest_p = subparsers.add_parser("suggest", help="Get suggestions")
    suggest_p.add_argument("--domain", required=True)
    
    # File Ingestion
    ingest_file_p = subparsers.add_parser("ingest-file", help="Upload and ingest local file")
    ingest_file_p.add_argument("--file", required=True, dest="file_path")
    ingest_file_p.add_argument("--table", required=True)
    ingest_file_p.add_argument("--schema", required=True, dest="schema_name")
    ingest_file_p.add_argument("--mode", default="overwrite", choices=["append", "overwrite", "merge"])
    ingest_file_p.add_argument("--merge-keys", help="Comma-separated merge keys")
    ingest_file_p.add_argument("--delimiter", default=",")
    ingest_file_p.add_argument("--header", default="true", choices=["true", "false"])
    ingest_file_p.add_argument("--domain")
    ingest_file_p.add_argument("--tags", help="Comma-separated tags")
    ingest_file_p.add_argument("--cast", action="append", metavar="COL:TYPE")
    ingest_file_p.add_argument("--rename", action="append", metavar="OLD:NEW")
    ingest_file_p.add_argument("--encrypt", action="append", metavar="COL")
    ingest_file_p.add_argument("--no-wait", action="store_true")
    ingest_file_p.add_argument("--max-wait", type=int, default=300)
    
    # Direct Ingestion
    ingest_p = subparsers.add_parser("ingest", help="Submit ingestion from S3")
    ingest_p.add_argument("--name", required=True)
    ingest_p.add_argument("--table", required=True)
    ingest_p.add_argument("--schema", required=True, dest="schema_name")
    ingest_p.add_argument("--type", dest="ingesttype", default="file", choices=["file", "jdbc", "lakehouse"])
    ingest_p.add_argument("--mode", default="overwrite", choices=["append", "overwrite", "merge"])
    ingest_p.add_argument("--file-path")
    ingest_p.add_argument("--file-id")
    ingest_p.add_argument("--format", dest="file_format", default="csv")
    ingest_p.add_argument("--delimiter", default=",")
    ingest_p.add_argument("--header", default="true", choices=["true", "false"])
    ingest_p.add_argument("--merge-keys")
    ingest_p.add_argument("--domain")
    ingest_p.add_argument("--tags")
    ingest_p.add_argument("--schedule")
    
    subparsers.add_parser("ingest-options", help="Get file ingestion options")
    
    # Jobs commands
    jobs_p = subparsers.add_parser("jobs", help="Job management")
    jobs_sub = jobs_p.add_subparsers(dest="jobs_command")
    jobs_sub.add_parser("list")
    jobs_get = jobs_sub.add_parser("get")
    jobs_get.add_argument("job_id")
    jobs_del = jobs_sub.add_parser("delete")
    jobs_del.add_argument("job_id")
    jobs_trig = jobs_sub.add_parser("trigger")
    jobs_trig.add_argument("job_id")
    jobs_wait = jobs_sub.add_parser("wait")
    jobs_wait.add_argument("job_id")
    jobs_wait.add_argument("--max-wait", type=int, default=300)
    jobs_wait.add_argument("--poll-interval", type=int, default=5)
    
    # Files commands
    files_p = subparsers.add_parser("files", help="File management")
    files_sub = files_p.add_subparsers(dest="files_command")
    files_sub.add_parser("list")
    files_up = files_sub.add_parser("upload")
    files_up.add_argument("file_path")
    files_up.add_argument("--name")
    files_url = files_sub.add_parser("upload-url")
    files_url.add_argument("url")
    files_url.add_argument("--name")
    files_get = files_sub.add_parser("get")
    files_get.add_argument("file_id")
    files_del = files_sub.add_parser("delete")
    files_del.add_argument("file_id")
    
    # S3 commands
    s3_p = subparsers.add_parser("s3", help="S3 bucket management")
    s3_sub = s3_p.add_subparsers(dest="s3_command")
    s3_sub.add_parser("list")
    s3_sub.add_parser("refresh")
    s3_get = s3_sub.add_parser("get")
    s3_get.add_argument("bucket_name")
    s3_create = s3_sub.add_parser("create")
    s3_create.add_argument("--bucket", required=True)
    s3_create.add_argument("--endpoint", required=True)
    s3_create.add_argument("--access-key", required=True)
    s3_create.add_argument("--secret-key", required=True)
    s3_del = s3_sub.add_parser("delete")
    s3_del.add_argument("bucket_name")
    
    # DQ commands
    dq_p = subparsers.add_parser("dq", help="Data quality")
    dq_sub = dq_p.add_subparsers(dest="dq_command")
    dq_suggest = dq_sub.add_parser("suggest")
    dq_suggest.add_argument("--table", required=True)
    dq_suggest.add_argument("--request")
    dq_rules = dq_sub.add_parser("rules")
    dq_rules.add_argument("--table", required=True)
    dq_rules.add_argument("--accepted-only", action="store_true")
    dq_run = dq_sub.add_parser("run")
    dq_run.add_argument("--table", required=True)
    dq_acc = dq_sub.add_parser("accept")
    dq_acc.add_argument("dq_id")
    dq_del = dq_sub.add_parser("delete")
    dq_del.add_argument("dq_id")
    
    # Data Products commands
    dp_p = subparsers.add_parser("dataproducts", help="Data products")
    dp_sub = dp_p.add_subparsers(dest="dp_command")
    dp_sub.add_parser("list")
    dp_get = dp_sub.add_parser("get")
    dp_get.add_argument("dataproduct_id")
    dp_del = dp_sub.add_parser("delete")
    dp_del.add_argument("dataproduct_id")
    
    # Apps commands
    apps_p = subparsers.add_parser("apps", help="Apps management")
    apps_sub = apps_p.add_subparsers(dest="apps_command")
    apps_sub.add_parser("list")
    apps_create = apps_sub.add_parser("create")
    apps_create.add_argument("--name", required=True)
    apps_get = apps_sub.add_parser("get")
    apps_get.add_argument("app_id")
    apps_del = apps_sub.add_parser("delete")
    apps_del.add_argument("app_id")
    apps_vers = apps_sub.add_parser("versions")
    apps_vers.add_argument("--app-id", required=True)
    apps_cv = apps_sub.add_parser("create-version")
    apps_cv.add_argument("--app-id", required=True)
    apps_cv.add_argument("--name", required=True)
    apps_act = apps_sub.add_parser("activate")
    apps_act.add_argument("version_id")
    apps_roles = apps_sub.add_parser("roles")
    apps_roles.add_argument("--app-id", required=True)
    apps_cr = apps_sub.add_parser("create-role")
    apps_cr.add_argument("--app-id", required=True)
    apps_cr.add_argument("--name", required=True)
    apps_comp = apps_sub.add_parser("components")
    apps_comp.add_argument("--version-id", required=True)
    apps_dag = apps_sub.add_parser("add-dag")
    apps_dag.add_argument("--version-id", required=True)
    apps_dag.add_argument("--file", required=True)
    apps_dag.add_argument("--name")
    
    # Mirror commands
    mirror_p = subparsers.add_parser("mirror", help="Create mirroring job")
    mirror_p.add_argument("--name", required=True)
    mirror_p.add_argument("--source-catalog", required=True)
    mirror_p.add_argument("--source-schema", required=True)
    mirror_p.add_argument("--source-table", required=True)
    mirror_p.add_argument("--target-catalog", required=True)
    mirror_p.add_argument("--target-schema", required=True)
    mirror_p.add_argument("--target-table", required=True)
    mirror_p.add_argument("--mode", default="overwrite", choices=["append", "overwrite", "merge"])
    mirror_p.add_argument("--schedule")
    mirror_p.add_argument("--merge-keys")
    
    # Shares commands
    shares_p = subparsers.add_parser("shares", help="Data shares")
    shares_sub = shares_p.add_subparsers(dest="shares_command")
    shares_sub.add_parser("list")
    shares_get = shares_sub.add_parser("get")
    shares_get.add_argument("share_id")
    shares_del = shares_sub.add_parser("delete")
    shares_del.add_argument("share_id")
    
    # Crews commands
    crews_p = subparsers.add_parser("crews", help="AI Crews")
    crews_sub = crews_p.add_subparsers(dest="crews_command")
    crews_sub.add_parser("list")
    crews_st = crews_sub.add_parser("status")
    crews_st.add_argument("task_id")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    try:
        client = NX1Client(
            api_key=args.api_key,
            host=args.host,
            verify_ssl=not args.no_verify_ssl,
            timeout=args.timeout
        )
        
        result = None
        
        if args.command == "ping":
            result = client.health.ping()
        elif args.command == "version":
            print(client.health.version())
            return
        elif args.command == "domains":
            result = client.metastore.get_domains()
        elif args.command == "catalogs":
            result = client.metastore.get_catalogs()
        elif args.command == "schemas":
            result = client.metastore.get_schemas(args.catalog)
        elif args.command == "tables":
            result = client.metastore.get_tables(args.catalog, args.schema)
        elif args.command == "columns":
            result = client.metastore.get_columns(args.catalog, args.schema, args.table)
        elif args.command == "tags":
            result = client.metastore.get_tags()
        elif args.command == "engines":
            result = client.metastore.get_engines()
        elif args.command == "ask":
            result = client.queries.ask(args.domain, args.prompt)
        elif args.command == "suggest":
            result = client.queries.suggest(args.domain)
        
        elif args.command == "ingest-file":
            transformations = []
            if args.cast:
                for c in args.cast:
                    col, dtype = c.split(":", 1)
                    transformations.append(ColumnTransformation.cast(col, dtype))
            if args.rename:
                for r in args.rename:
                    old, new = r.split(":", 1)
                    transformations.append(ColumnTransformation.rename(old, new))
            if args.encrypt:
                for e in args.encrypt:
                    transformations.append(ColumnTransformation.encrypt(e))
            
            merge_keys = args.merge_keys.split(",") if args.merge_keys else None
            tags = args.tags.split(",") if args.tags else None
            
            job_id = client.ingestion.ingest_local_file(
                file_path=args.file_path, table=args.table, schema_name=args.schema_name,
                mode=args.mode, merge_keys=merge_keys,
                column_transformations=transformations if transformations else None,
                header=args.header, delimiter=args.delimiter, domain=args.domain, tags=tags,
                wait_for_completion=not args.no_wait, max_wait=args.max_wait, verbose=True
            )
            print(f"\n🎉 Pipeline completed! Job ID: {job_id}")
            return
        
        elif args.command == "ingest":
            file_opts = {"header": args.header, "inferSchema": "true", "delimiter": args.delimiter}
            merge_keys = args.merge_keys.split(",") if args.merge_keys else None
            tags = args.tags.split(",") if args.tags else None
            result = client.ingestion.submit(
                name=args.name, ingesttype=args.ingesttype, schema_name=args.schema_name,
                table=args.table, mode=args.mode, file_path=args.file_path, file_id=args.file_id,
                file_format=args.file_format, file_read_options=file_opts, merge_keys=merge_keys,
                domain=args.domain, tags=tags, schedule=args.schedule
            )
            print(f"Job ID: {result.get('job_id')}, Flow URL: {result.get('flow_url', 'N/A')}")
            return
        
        elif args.command == "ingest-options":
            result = client.ingestion.get_file_ingestion_options()
        
        elif args.command == "jobs":
            cmd = args.jobs_command
            if cmd == "list" or not cmd:
                result = client.jobs.get_all()
            elif cmd == "get":
                result = client.jobs.get(args.job_id)
            elif cmd == "delete":
                client.jobs.delete(args.job_id); print(f"✅ Deleted: {args.job_id}"); return
            elif cmd == "trigger":
                client.jobs.trigger(args.job_id); print(f"✅ Triggered: {args.job_id}"); return
            elif cmd == "wait":
                result = client.jobs.wait_for_completion(args.job_id, args.max_wait, args.poll_interval)
                print(f"✅ Completed: {result.get('status')}")
        
        elif args.command == "files":
            cmd = args.files_command
            if cmd == "list" or not cmd:
                result = client.files.get_all()
            elif cmd == "upload":
                result = client.files.upload(args.file_path, args.name)
                print(f"✅ Uploaded: {result.get('id')}, S3: {result.get('s3_url')}"); return
            elif cmd == "upload-url":
                result = client.files.upload_from_url(args.url, args.name)
                print(f"✅ Uploaded: {result.get('id')}, S3: {result.get('s3_url')}"); return
            elif cmd == "get":
                result = client.files.get(args.file_id)
            elif cmd == "delete":
                client.files.delete(args.file_id); print(f"✅ Deleted: {args.file_id}"); return
        
        elif args.command == "s3":
            cmd = args.s3_command
            if cmd == "list" or not cmd:
                result = client.s3.get_buckets()
            elif cmd == "refresh":
                client.s3.refresh(); print("✅ Refreshed"); return
            elif cmd == "get":
                result = client.s3.get_bucket(args.bucket_name)
            elif cmd == "create":
                client.s3.create_bucket(args.bucket, args.endpoint, args.access_key, args.secret_key)
                print(f"✅ Created: {args.bucket}"); return
            elif cmd == "delete":
                client.s3.delete_bucket(args.bucket_name); print(f"✅ Deleted: {args.bucket_name}"); return
        
        elif args.command == "dq":
            cmd = args.dq_command
            if cmd == "suggest":
                result = client.data_quality.suggest(args.table, getattr(args, 'request', None))
            elif cmd == "rules":
                result = client.data_quality.get_rules_by_table(args.table, args.accepted_only)
            elif cmd == "run":
                result = client.data_quality.run_report_by_table(args.table)
            elif cmd == "accept":
                client.data_quality.accept(args.dq_id); print(f"✅ Accepted: {args.dq_id}"); return
            elif cmd == "delete":
                client.data_quality.delete_rule(args.dq_id); print(f"✅ Deleted: {args.dq_id}"); return
        
        elif args.command == "dataproducts":
            cmd = args.dp_command
            if cmd == "list" or not cmd:
                result = client.data_products.get_all()
            elif cmd == "get":
                result = client.data_products.get(args.dataproduct_id)
            elif cmd == "delete":
                client.data_products.delete(args.dataproduct_id); print(f"✅ Deleted"); return
        
        elif args.command == "apps":
            cmd = args.apps_command
            if cmd == "list" or not cmd:
                result = client.apps.get_all()
            elif cmd == "create":
                result = client.apps.create(args.name); print(f"✅ Created: {result.get('id')}")
            elif cmd == "get":
                result = client.apps.get(args.app_id)
            elif cmd == "delete":
                client.apps.delete(args.app_id); print(f"✅ Deleted"); return
            elif cmd == "versions":
                result = client.apps.get_versions(args.app_id)
            elif cmd == "create-version":
                result = client.apps.create_version(args.app_id, args.name)
                print(f"✅ Version created: {result.get('id')}")
            elif cmd == "activate":
                client.apps.activate_version(args.version_id); print(f"✅ Activated"); return
            elif cmd == "roles":
                result = client.apps.get_roles(args.app_id)
            elif cmd == "create-role":
                result = client.apps.create_role(args.app_id, args.name)
                print(f"✅ Role created: {result.get('id')}")
            elif cmd == "components":
                result = client.apps.get_components(args.version_id)
            elif cmd == "add-dag":
                result = client.apps.add_dag(args.version_id, args.file, args.name)
                print(f"✅ DAG added: {result.get('id')}")
        
        elif args.command == "mirror":
            merge_keys = args.merge_keys.split(",") if args.merge_keys else None
            result = client.mirroring.create(
                job_name=args.name, source_catalog=args.source_catalog,
                source_schema=args.source_schema, source_table=args.source_table,
                target_catalog=args.target_catalog, target_schema=args.target_schema,
                target_table=args.target_table, mode=args.mode, schedule=args.schedule,
                merge_keys=merge_keys
            )
            print(f"✅ Mirroring job created: {result.get('job_id')}")
        
        elif args.command == "shares":
            cmd = args.shares_command
            if cmd == "list" or not cmd:
                result = client.data_shares.get_all()
            elif cmd == "get":
                result = client.data_shares.get(args.share_id)
            elif cmd == "delete":
                client.data_shares.delete(args.share_id); print(f"✅ Deleted"); return
        
        elif args.command == "crews":
            cmd = args.crews_command
            if cmd == "list" or not cmd:
                result = client.crews.list_crews()
            elif cmd == "status":
                result = client.crews.get_crew_status(args.task_id)
        
        if result is not None:
            print(json.dumps(result, indent=2, default=str))
    
    except NX1ValidationError as e:
        print(f"❌ Validation Error: {e}", file=sys.stderr); sys.exit(1)
    except NX1APIError as e:
        print(f"❌ API Error: {e}", file=sys.stderr); sys.exit(1)
    except NX1TimeoutError as e:
        print(f"⏱️ Timeout: {e}", file=sys.stderr); sys.exit(1)
    except FileNotFoundError as e:
        print(f"❌ File not found: {e}", file=sys.stderr); sys.exit(1)
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted", file=sys.stderr); sys.exit(130)
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        if args.verbose:
            import traceback; traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
