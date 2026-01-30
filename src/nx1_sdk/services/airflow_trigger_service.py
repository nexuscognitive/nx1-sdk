import requests
import json
import sys
import time
import logging
import urllib3
from datetime import datetime, timezone
from urllib.parse import urljoin
from typing import Optional

class AirflowTriggererClient:
    def __init__(self, airflow_url, username, password, logger: Optional[logging.Logger] = None):
        self.airflow_url = airflow_url.rstrip("/")
        self.username = username
        self.password = password
        self.logger = logger or logging.getLogger(__name__)

        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.verify = False
        self.token = self._fetch_token()
        
        self.headers = {"Content-Type": "application/json","Accept": "application/json","Authorization": f"Bearer {self.token}"}
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def _fetch_token(self) -> str:

        token_url = f"{self.airflow_url}/auth/token"
        payload = {"username": self.username, "password": self.password}

        resp = requests.post(token_url, json=payload, timeout=10)
        resp.raise_for_status()
        token = resp.json().get("access_token")
        if not token:
            raise RuntimeError("Airflow token endpoint returned no token")

        self.logger.debug("Retrieved Airflow 3 auth token")
        return token

    def trigger_dag(self, dag_id, conf=None):
        trigger_url = urljoin(
            self.airflow_url,
            f"/api/v2/dags/{dag_id}/dagRuns"
        )

        payload = {"logical_date": datetime.now(timezone.utc).isoformat()}

        self.logger.info(f"Triggering DAG: {dag_id}")
        self.logger.debug(f"Trigger payload: {json.dumps(payload, indent=2)}")

        response = requests.post(
            trigger_url,
            headers=self.headers,
            json=payload
        )

        if response.status_code not in (200, 201):
            self.logger.error(f"Failed to trigger DAG. HTTP {response.status_code}")
            self.logger.error(response.text)
            raise Exception(f"Failed to trigger DAG: {response.status_code}")

        data = response.json()
        dag_run_id = data.get("dag_run_id", "N/A")
        state = data.get("state", "queued")

        encoded_run_id = (
            dag_run_id.replace(":", "%3A")
                      .replace("+", "%2B")
        )

        dag_run_url = urljoin(
            self.airflow_url,
            f"/dags/{dag_id}/grid?dag_run_id={encoded_run_id}"
        )

        self.logger.info(
            f"DAG triggered successfully! "
            f"Run ID: {dag_run_id}, Initial state: {state}"
        )
        self.logger.info(f"DAG run URL: {dag_run_url}")

        return dag_run_id

    def get_dag_run_status(self, dag_id, dag_run_id):
        encoded_dag_run_id = (
            dag_run_id.replace(":", "%3A")
                      .replace("+", "%2B")
        )
        dag_run_url = urljoin(
            self.airflow_url,
            f"/api/v2/dags/{dag_id}/dagRuns/{encoded_dag_run_id}"
        )

        response = requests.get(
            dag_run_url,
            headers=self.headers
        )

        if response.status_code != 200:
            self.logger.error(
                f"Error fetching DAG run status: HTTP {response.status_code}"
            )
            self.logger.error(response.text)
            return None
        
        return response.json()

    def monitor_dag(self, dag_id, dag_run_id, poll_interval=3):
        self.logger.info(f"Monitoring DAG run: {dag_run_id}")

        start_time = datetime.now()
        previous_state = None

        spinner = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
        spinner_idx = 0

        terminal_states = {
            'success',
            'failed',
            'upstream_failed',
            'skipped'
        }

        while True:
            try:
                run_info = self.get_dag_run_status(dag_id, dag_run_id)
                if not run_info:
                    time.sleep(poll_interval)
                    continue

                state = run_info.get("state", "unknown").lower()
                elapsed = (datetime.now() - start_time).seconds

                if state not in terminal_states:
                    sys.stdout.write(
                        f"\r{spinner[spinner_idx % len(spinner)]} "
                        f"State: {state} (elapsed: {elapsed}s)"
                    )
                    sys.stdout.flush()
                    spinner_idx += 1
                else:
                    sys.stdout.write('\r' + ' ' * 80 + '\r')
                    sys.stdout.flush()
                    self.logger.info(f"DAG reached terminal state: {state}")
                    minutes, seconds = divmod(elapsed, 60)
                    self.logger.info(f"Total time: {minutes}m {seconds}s")
                    return state

                if state != previous_state:
                    sys.stdout.write('\r' + ' ' * 80 + '\r')
                    sys.stdout.flush()
                    self.logger.info(f"DAG state changed: {state}")
                    previous_state = state

            except Exception as e:
                print(f"Error monitoring DAG: {e}")
                self.logger.error(f"Error monitoring DAG: {e}")

            time.sleep(poll_interval)