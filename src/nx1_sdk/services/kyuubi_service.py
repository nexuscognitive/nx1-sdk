import requests
import json
import os
import sys
import time
import logging
from urllib.parse import urljoin, urlparse
from datetime import datetime
import urllib3


class KyuubiBatchSubmitterClient:
    # Known remote URI schemes that don't need uploading
    REMOTE_SCHEMES = {
        'hdfs', 's3', 's3a', 's3n', 'gs',
        'wasb', 'wasbs', 'abfs', 'abfss',
        'http', 'https', 'ftp', 'local'
    }

    # Python file extensions
    PYTHON_EXTENSIONS = {'.py', '.zip', '.egg'}

    def __init__(self, server, username, password, logger, history_server=None):
        self.server = server
        self.username = username
        self.password = password
        self.logger = logger or logging.getLogger(__name__)
        self.history_server = history_server

        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.verify = False  # Disable SSL verification
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def is_local_file(self, path):
        """
        Determine if a path refers to a local file that needs uploading.
        Returns True for local files, False for remote URIs.
        """
        if not path:
            return False
        
        # Parse the path to check for URI scheme
        parsed = urlparse(path)
        
        # If it has a known remote scheme, it's not local
        if parsed.scheme and parsed.scheme.lower() in self.REMOTE_SCHEMES:
            return False
        
        # If it has a scheme we don't recognize, treat it as remote
        if parsed.scheme and len(parsed.scheme) > 1:  # len > 1 to exclude Windows drive letters
            self.logger.debug(f"Unknown scheme '{parsed.scheme}' for path '{path}', treating as remote")
            return False
        
        # Check if the file exists locally
        expanded_path = os.path.expanduser(os.path.expandvars(path))
        if os.path.isfile(expanded_path):
            return True
        
        # If file doesn't exist locally, assume it's a remote path without scheme
        self.logger.debug(f"Path '{path}' not found locally, treating as remote")
        return False
    
    def expand_path(self, path):
        """Expand user home and environment variables in path."""
        return os.path.expanduser(os.path.expandvars(path))
    
    def get_file_extension(self, path):
        """Get lowercase file extension from path or URI."""
        # Handle URIs by extracting the path component
        parsed = urlparse(path)
        if parsed.scheme:
            path = parsed.path
        return os.path.splitext(path)[1].lower()
    
    def is_python_resource(self, resource):
        """Determine if the resource is a Python file based on extension."""
        ext = self.get_file_extension(resource)
        return ext in self.PYTHON_EXTENSIONS
    
    def parse_conf(self, conf_string):
        """Parse comma-separated key=value pairs into a dictionary"""
        conf_dict = {}
        if conf_string:
            for item in conf_string.split(','):
                item = item.strip()
                if '=' in item:
                    key, value = item.split('=', 1)
                    conf_dict[key.strip()] = value.strip()
        return conf_dict
    
    def format_history_url(self, app_id):
        """Format Spark History Server URL"""
        if self.history_server and app_id:
            history_base = self.history_server.rstrip('/')
            if not history_base.startswith(('http://', 'https://')):
                history_base = f"http://{history_base}"
            if ':18080' not in history_base and not any(f":{p}" in history_base for p in range(1, 65536)):
                history_base = f"{history_base}:18080"
            return f"{history_base}/history/{app_id}/"
        return None
    
    def classify_paths(self, paths):
        """
        Classify a list of paths into local files and remote URIs.
        Returns (local_files, remote_uris) where local_files is list of (original_path, expanded_path)
        """
        if not paths:
            return [], []
        
        local_files = []
        remote_uris = []
        
        for path in paths:
            path = path.strip()
            if not path:
                continue
            if self.is_local_file(path):
                local_files.append((path, self.expand_path(path)))
            else:
                remote_uris.append(path)
        
        return local_files, remote_uris
        
    def submit_batch(self, resource, classname, name, args, conf, py_files, jars, files=None):
        """Submit a batch job to Kyuubi using multipart form data if local files present."""
        url = urljoin(self.server, "/api/v1/batches")
        
        # Determine batch type based on resource extension
        is_pyspark = self.is_python_resource(resource)
        batch_type = "PYSPARK" if is_pyspark else "SPARK"
        self.logger.info(f"Detected batch type: {batch_type}")
        
        # Determine if main resource is local
        resource_is_local = self.is_local_file(resource)
        resource_expanded = self.expand_path(resource) if resource_is_local else None
        
        # Classify pyFiles and jars
        py_files_list = []
        if py_files:
            if isinstance(py_files, str):
                py_files_list = [f.strip() for f in py_files.split(',') if f.strip()]
            else:
                py_files_list = [f for f in py_files if f]
        
        jars_list = []
        if jars:
            if isinstance(jars, str):
                jars_list = [j.strip() for j in jars.split(',') if j.strip()]
            else:
                jars_list = [j for j in jars if j]
        
        files_list = []
        if files:
            if isinstance(files, str):
                files_list = [f.strip() for f in files.split(',') if f.strip()]
            else:
                files_list = [f for f in files if f]
        
        local_py_files, remote_py_files = self.classify_paths(py_files_list)
        local_jars, remote_jars = self.classify_paths(jars_list)
        local_files, remote_files = self.classify_paths(files_list)
        
        # Check if we need multipart upload
        has_local_files = resource_is_local or local_py_files or local_jars or local_files
        
        # Build batch request object
        batch_request = {
            "batchType": batch_type,
            "name": name,
            "args": args.split() if isinstance(args, str) and args else args or []
        }
        
        # Only set resource in JSON if it's remote
        if not resource_is_local:
            batch_request["resource"] = resource
        
        # Set className only for JARs (not for PySpark)
        if classname and not is_pyspark:
            batch_request["className"] = classname

        # Handle conf
        if isinstance(conf, str):
            conf_dict = self.parse_conf(conf)
        else:
            conf_dict = conf or {}
        conf_dict["spark.submit.deployMode"] = "cluster"
        
        # Add remote pyFiles to conf (works for both JSON and multipart submissions)
        if remote_py_files:
            conf_dict["spark.submit.pyFiles"] = ",".join(remote_py_files)
        
        # Add remote jars to conf (works for both JSON and multipart submissions)
        if remote_jars:
            conf_dict["spark.jars"] = ",".join(remote_jars)
        
        # Add remote files to conf (works for both JSON and multipart submissions)
        if remote_files:
            conf_dict["spark.files"] = ",".join(remote_files)
        
        batch_request["conf"] = conf_dict
        
        self.logger.info(f"Submitting job: {name}")
        
        if has_local_files:
            self.logger.info("Detected local files - using multipart upload")
            response = self._submit_multipart(url, batch_request, resource_expanded, local_py_files, local_jars, local_files)
        else:
            self.logger.info("All resources are remote - using JSON submission")
            response = self._submit_json(url, batch_request)
        
        if response.status_code not in [200, 201]:
            self.logger.error(f"Error submitting job: {response.status_code}")
            self.logger.error(response.text)
            raise Exception(f"Failed to submit batch: {response.status_code}")
        
        batch = response.json()
        return batch['id']
    
    def _submit_json(self, url, batch_request):
        """Submit batch using JSON (no file uploads)."""
        self.logger.debug(f"Batch config: {json.dumps(batch_request, indent=2)}")
        return self.session.post(
            url,
            headers={"Content-Type": "application/json"},
            json=batch_request
        )
    
    def _submit_multipart(self, url, batch_request, resource_file, local_py_files, local_jars, local_files):
        """Submit batch using multipart form data with file uploads."""
        
        # WORKAROUND: Kyuubi has a validation bug where it checks 
        # `require(resource != null)` BEFORE processing the uploaded file.
        # The server code flow is:
        #   1. Validate batchRequest (fails if resource is null)
        #   2. Process uploaded file
        #   3. Call batchRequest.setResource(tempFile.getPath) - overwrites our value
        # So we must provide a placeholder value to pass validation, even though
        # Kyuubi will overwrite it with the actual uploaded file path.
        if resource_file:
            batch_request["resource"] = "placeholder"
        
        # Build extraResourcesMap to tell Kyuubi which Spark config key to bind
        # each uploaded extra file to.
        # Format: {spark_config_key: "filename1,filename2,..."}
        # Kyuubi will APPEND the uploaded file paths to any existing conf value.
        extra_resources_map = {}
        
        if local_py_files:
            pyfile_names = [os.path.basename(expanded) for _, expanded in local_py_files]
            extra_resources_map["spark.submit.pyFiles"] = ",".join(pyfile_names)
            
        if local_jars:
            jar_names = [os.path.basename(expanded) for _, expanded in local_jars]
            extra_resources_map["spark.jars"] = ",".join(jar_names)
        
        if local_files:
            file_names = [os.path.basename(expanded) for _, expanded in local_files]
            extra_resources_map["spark.files"] = ",".join(file_names)
        
        if extra_resources_map:
            batch_request["extraResourcesMap"] = extra_resources_map
            self.logger.info(f"Extra resources map: {extra_resources_map}")
        
        self.logger.debug(f"Batch request: {json.dumps(batch_request, indent=2)}")
        
        # Build multipart form data
        files = []
        opened_files = []  # Track opened files for cleanup
        
        try:
            # Add batch request as JSON part
            files.append(
                ('batchRequest', (None, json.dumps(batch_request), 'application/json'))
            )
            
            # Add main resource file if local
            if resource_file:
                self.logger.info(f"Uploading main resource: {resource_file}")
                f = open(resource_file, 'rb')
                opened_files.append(f)
                files.append(
                    ('resourceFile', (os.path.basename(resource_file), f, 'application/octet-stream'))
                )
            
            # Add extra resource files (pyFiles, jars, and files)
            # The form field name must be the filename, as Kyuubi looks up files by name:
            # formDataMultiPart.getField(fileName)
            for original_path, expanded_path in local_py_files:
                filename = os.path.basename(expanded_path)
                self.logger.info(f"Uploading pyFile: {expanded_path}")
                f = open(expanded_path, 'rb')
                opened_files.append(f)
                files.append(
                    (filename, (filename, f, 'application/octet-stream'))
                )
            
            for original_path, expanded_path in local_jars:
                filename = os.path.basename(expanded_path)
                self.logger.info(f"Uploading jar: {expanded_path}")
                f = open(expanded_path, 'rb')
                opened_files.append(f)
                files.append(
                    (filename, (filename, f, 'application/octet-stream'))
                )
            
            for original_path, expanded_path in local_files:
                filename = os.path.basename(expanded_path)
                self.logger.info(f"Uploading file: {expanded_path}")
                f = open(expanded_path, 'rb')
                opened_files.append(f)
                files.append(
                    (filename, (filename, f, 'application/octet-stream'))
                )
            
            # Send multipart request
            response = self.session.post(url, files=files)
            return response
            
        finally:
            # Clean up opened files
            for f in opened_files:
                f.close()
    
    def cancel_batch(self, batch_id):
        """Cancel a batch job"""
        url = urljoin(self.server, f"/api/v1/batches/{batch_id}")
        self.logger.info(f"Cancelling batch {batch_id}...")
        response = self.session.delete(url)
        if response.status_code in [200, 204]:
            self.logger.info(f"Batch {batch_id} cancelled successfully")
            return True
        else:
            self.logger.error(f"Error cancelling batch: {response.status_code}")
            self.logger.error(response.text)
            return False
    
    def get_batch_status(self, batch_id):
        """Get batch status"""
        url = urljoin(self.server, f"/api/v1/batches/{batch_id}")
        response = self.session.get(url)
        if response.status_code != 200:
            self.logger.error(f"Error getting status: {response.status_code}")
            return None
        return response.json()
    
    def get_batch_logs(self, batch_id, from_index=0, size=1000):
        """Get batch logs"""
        url = urljoin(self.server, f"/api/v1/batches/{batch_id}/localLog")
        params = {"from": from_index, "size": size}
        response = self.session.get(url, params=params)
        if response.status_code != 200:
            self.logger.warning(f"Error getting logs: {response.status_code}")
            return None
        return response.json()
    
    def print_all_logs(self, batch_id):
        """Retrieve and print all logs"""
        self.logger.info("Retrieving job logs...")
        print("\n" + "="*60 + " JOB OUTPUT " + "="*60)
        
        from_index = 0
        size = 1000
        total_printed = 0
        
        while True:
            log_data = self.get_batch_logs(batch_id, from_index, size)
            if not log_data:
                self.logger.warning("No log data available")
                break
            log_rows = log_data.get('logRowSet', [])
            for row in log_rows:
                print(row)
                total_printed += 1
            total = log_data.get('total', 0)
            if from_index + len(log_rows) >= total:
                break
            from_index += len(log_rows)
        
        print("="*132 + "\n")
        self.logger.info(f"Retrieved {total_printed} log lines")
    
    def monitor_job(self, batch_id, show_logs=True):
        """Monitor job until completion"""
        self.logger.info(f"Monitoring batch {batch_id}")
        
        start_time = datetime.now()
        previous_state = None
        previous_app_state = None
        spinner = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
        spinner_idx = 0
        
        while True:
            try:
                status = self.get_batch_status(batch_id)
                
                if status:
                    state = status.get('state', 'UNKNOWN')
                    app_state = status.get('appState', '')
                    app_id = status.get('appId', '')
                    app_url = status.get('appUrl', '')
                    elapsed = (datetime.now() - start_time).seconds
                    
                    if state not in ['FINISHED', 'ERROR', 'CANCELLED']:
                        sys.stdout.write(f'\r{spinner[spinner_idx % len(spinner)]} State: {state} | App State: {app_state or "N/A"} (elapsed: {elapsed}s)')
                        sys.stdout.flush()
                        spinner_idx += 1
                    
                    if state != previous_state or app_state != previous_app_state:
                        sys.stdout.write('\r' + ' '*80 + '\r')
                        self.logger.info(f"Batch state: {state} | App state: {app_state or 'N/A'}")
                        previous_state = state
                        previous_app_state = app_state
                        
                        if app_id:
                            self.logger.info(f"Spark App ID: {app_id}")
                            history_url = self.format_history_url(app_id)
                            if history_url:
                                self.logger.info(f"Spark History URL: {history_url}")
                            elif app_url:
                                self.logger.info(f"Spark UI URL: {app_url}")
                    
                    if state == 'FINISHED':
                        sys.stdout.write('\r' + ' '*80 + '\r')
                        elapsed_min = elapsed // 60
                        elapsed_sec = elapsed % 60
                        self.logger.info(f"Job completed in {elapsed_min}m {elapsed_sec}s")
                        if app_state:
                            self.logger.info(f"Final application state: {app_state}")
                        app_diagnostic = status.get('appDiagnostic', '')
                        if app_diagnostic:
                            self.logger.info(f"Application diagnostics: {app_diagnostic}")
                        if show_logs:
                            self.print_all_logs(batch_id)
                        else:
                            self.logger.info("Logs available but not displayed (use --show-logs to see them)")
                        return app_state if app_state else state
                    
                    elif state in ['ERROR', 'CANCELLED']:
                        sys.stdout.write('\r' + ' '*80 + '\r')
                        self.logger.error(f"Batch terminated with state: {state}")
                        app_diagnostic = status.get('appDiagnostic', '')
                        if app_diagnostic:
                            self.logger.error(f"Application diagnostics: {app_diagnostic}")
                        if show_logs:
                            self.print_all_logs(batch_id)
                        return state
                
            except Exception as e:
                self.logger.error(f"Error monitoring job: {e}")
            
            time.sleep(10)