"""JupyterHub service client for managing notebooks, kernels, and terminals."""

import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from urllib.parse import quote

import requests
import websocket

from nx1_sdk.exceptions import NX1APIError, NX1ValidationError


class JupyterHubClient:
    """
    Client for JupyterHub REST API and WebSocket interfaces.

    Covers:
      - Server lifecycle  : start / stop / status
      - Terminal API      : launch, list, run commands via WebSocket
      - Session API       : create notebook session, list sessions
      - Kernel channels   : execute code via WebSocket (Jupyter messaging protocol)

    Usage::

        client = JupyterHubClient(
            base_url="https://jupyter-gzt1.nx1poc.nx1cloud.com",
            token="<api-token>",
            username="asaxena@nexuscognitive.com",
        )

        client.start_server()
        client.create_session("MyNewNotebook.ipynb")   # kernel_id saved automatically
        output = client.run_kernel_code('print("Hello from API")')
        term     = client.launch_terminal()
        out      = client.run_terminal_command(term["name"], "python --version")
        client.stop_server()
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        username: str,
        verify_ssl: bool = True,
        timeout: int = 30,
        logger: Optional[logging.Logger] = None,
    ):
        if not base_url:
            raise NX1ValidationError("base_url is required.")
        if not token:
            raise NX1ValidationError("token is required.")
        if not username:
            raise NX1ValidationError("username is required.")

        self.base_url = base_url.rstrip("/")
        self.token = token
        self.username = username                                     # original — used in payloads/logs
        self._username_encoded = quote(username, safe="")            # hub API: encodes @ → %40
        self._username_user_encoded = quote(username, safe="@")      # user server routes: @ preserved

        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.log = logger or logging.getLogger(__name__)

        self.kernel_id: Optional[str] = None

        self._session = requests.Session()
        self._session.headers.update(self._auth_headers())

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _auth_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"token {self.token}",
            "Content-Type": "application/json",
        }

    @property
    def _hub_api(self) -> str:
        return f"{self.base_url}/hub/api"

    @property
    def _user_api(self) -> str:
        # JupyterHub proxy routes for user servers use the literal '@' in the path;
        # only other special characters (spaces, slashes, etc.) are percent-encoded.
        return f"{self.base_url}/user/{self._username_user_encoded}"

    def _ws_url(self, path: str) -> str:
        """Convert base_url to wss:// and append path + token query param."""
        base = self.base_url
        if base.startswith("https://"):
            ws_base = "wss://" + base[len("https://"):]
        elif base.startswith("http://"):
            ws_base = "ws://" + base[len("http://"):]
        else:
            ws_base = base
        sep = "&" if "?" in path else "?"
        return f"{ws_base}{path}{sep}token={self.token}"

    def _raise_for_status(self, resp: requests.Response, context: str) -> None:
        if not resp.ok:
            raise NX1APIError(
                f"{context} failed — HTTP {resp.status_code}: {resp.text}",
                status_code=resp.status_code,
            )

    # ------------------------------------------------------------------
    # Server lifecycle
    # ------------------------------------------------------------------

    def is_server_running(self) -> bool:
        """
        Check whether the user's single-user server is currently running.

        Hits GET /hub/api/users/{username} and inspects the ``server`` field.

        Returns:
            True if the server is running, False otherwise.
        """
        url = f"{self._hub_api}/users/{self._username_encoded}"
        self.log.debug("GET %s", url)
        resp = self._session.get(url, verify=self.verify_ssl, timeout=self.timeout)
        self._raise_for_status(resp, f"GET {url}")
        data = resp.json()
        server = data.get("server") or data.get("servers", {})
        # server is a non-empty string/dict when running
        if isinstance(server, dict):
            running = bool(server)
        else:
            running = bool(server)
        self.log.info("Server running: %s", running)
        return running

    def start_server(self) -> Dict[str, Any]:
        """
        Start (or spawn) the user's JupyterHub single-user server.

        POST /hub/api/users/{username}/server

        Returns:
            Parsed JSON response (may be empty dict on 201/202).
        """
        url = f"{self._hub_api}/users/{self._username_encoded}/server"
        self.log.info("POST %s — starting server", url)
        resp = self._session.post(url, json={}, verify=self.verify_ssl, timeout=self.timeout)
        # 201 = created, 202 = accepted (already spawning), 400 = already running
        if resp.status_code not in (201, 202, 400):
            self._raise_for_status(resp, f"POST {url}")
        data = resp.json() if resp.text else {}
        self.log.info("start_server response: %s", data)
        return data

    def stop_server(self) -> None:
        """
        Stop (delete) the user's JupyterHub single-user server.

        DELETE /hub/api/users/{username}/server
        """
        url = f"{self._hub_api}/users/{self._username_encoded}/server"
        self.log.info("DELETE %s — stopping server", url)
        resp = self._session.delete(url, verify=self.verify_ssl, timeout=self.timeout)
        # 204 = stopped, 400 = not running
        if resp.status_code not in (204, 400):
            self._raise_for_status(resp, f"DELETE {url}")
        self.log.info("stop_server status: %d", resp.status_code)

    def wait_for_server(self, poll_interval: float = 3.0, max_wait: float = 120.0) -> bool:
        """
        Poll until the server is running or *max_wait* seconds elapse.

        Returns:
            True if server came up, False on timeout.
        """
        deadline = time.time() + max_wait
        while time.time() < deadline:
            if self.is_server_running():
                return True
            time.sleep(poll_interval)
        return False

    # ------------------------------------------------------------------
    # Terminal API
    # ------------------------------------------------------------------

    def launch_terminal(self) -> Dict[str, Any]:
        """
        Create a new terminal on the user's server.

        POST /user/{username}/api/terminals

        Returns:
            Dict with ``name`` key (terminal identifier).
        """
        url = f"{self._user_api}/api/terminals"
        self.log.info("POST %s — launching terminal", url)
        resp = self._session.post(url, json={}, verify=self.verify_ssl, timeout=self.timeout)
        self._raise_for_status(resp, f"POST {url}")
        data = resp.json()
        self.log.info("Terminal launched: %s", data.get("name"))
        return data

    def list_terminals(self) -> List[Dict[str, Any]]:
        """
        List all active terminals on the user's server.

        GET /user/{username}/api/terminals

        Returns:
            List of terminal dicts, each with a ``name`` key.
        """
        url = f"{self._user_api}/api/terminals"
        self.log.debug("GET %s", url)
        resp = self._session.get(url, verify=self.verify_ssl, timeout=self.timeout)
        self._raise_for_status(resp, f"GET {url}")
        terminals = resp.json()
        self.log.info("Active terminals: %d", len(terminals))
        return terminals

    def run_terminal_command(
        self,
        terminal_name: str,
        command: str,
        timeout: float = 15.0,
        end_marker: str = "$ ",
    ) -> str:
        """
        Run a shell command in an existing terminal via WebSocket.

        Connects to ``wss://{host}/user/{username}/api/terminals/websocket/{name}``,
        sends ``["stdin", "<command>\\r"]``, collects stdout until the shell prompt
        reappears or *timeout* elapses.

        Args:
            terminal_name: Terminal name returned by :meth:`launch_terminal`.
            command: Shell command to execute (without trailing newline).
            timeout: Seconds to wait for output before giving up.
            end_marker: String that signals the shell is ready again (prompt).

        Returns:
            Captured stdout text.
        """
        # Use user-server encoding (@-preserving) for the WebSocket path
        path = f"/user/{self._username_user_encoded}/api/terminals/websocket/{terminal_name}"
        ws_url = self._ws_url(path)
        self.log.info("Connecting to terminal WS: %s", ws_url)

        output_chunks: List[str] = []
        done = False

        def on_message(ws_obj, message):
            nonlocal done
            try:
                msg = json.loads(message)
            except Exception:
                return
            if isinstance(msg, list) and len(msg) >= 2 and msg[0] == "stdout":
                chunk = msg[1]
                output_chunks.append(chunk)
                self.log.debug("terminal stdout: %r", chunk)
                if end_marker in chunk:
                    done = True
                    ws_obj.close()

        def on_open(ws_obj):
            # Send the command followed by carriage return
            payload = json.dumps(["stdin", command + "\r"])
            ws_obj.send(payload)

        def on_error(ws_obj, error):
            self.log.warning("Terminal WS error: %s", error)

        sslopt = {"cert_reqs": 0} if not self.verify_ssl else {}
        ws = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
        )
        ws.run_forever(ping_interval=0, sslopt=sslopt, ping_timeout=None)

        result = "".join(output_chunks)
        self.log.info("Terminal command %r output length: %d chars", command, len(result))
        return result

    def run_terminal_command_with_timeout(
        self,
        terminal_name: str,
        command: str,
        timeout: float = 20.0,
        end_markers: tuple = ("$ ", "# "),
    ) -> str:
        """
        Run a shell command by executing it in the active kernel via subprocess.

        Uses ``run_kernel_code`` with Python's ``subprocess`` module so that no
        separate terminal WebSocket connection is required.  ``terminal_name`` is
        accepted for API compatibility but not used.

        Returns:
            Combined stdout (and stderr on non-zero exit) of the command.
        """
        import shlex

        try:
            parts = shlex.split(command)
        except ValueError:
            parts = command.split()

        cmd_repr = repr(parts)
        kernel_timeout = max(5, int(timeout) - 5)
        code = (
            "import subprocess as _sp, sys as _sys\n"
            f"_r = _sp.run({cmd_repr}, capture_output=True, text=True, timeout={kernel_timeout})\n"
            "print(_r.stdout, end='')\n"
            "if _r.returncode != 0 and _r.stderr:\n"
            "    print(_r.stderr, end='')\n"
        )
        self.log.info("Running command via kernel subprocess: %r", command)
        return self.run_kernel_code(code=code, timeout=timeout)

    # ------------------------------------------------------------------
    # Session / Notebook API
    # ------------------------------------------------------------------

    def create_session(
        self,
        path: str = "MyNewNotebook.ipynb",
        kernel_name: str = "python3",
        session_type: str = "notebook",
    ) -> Dict[str, Any]:
        """
        Create a new notebook session (also starts the kernel).

        POST /user/{username}/api/sessions

        Args:
            path: Notebook path / filename.
            kernel_name: Jupyter kernel to use (default ``python3``).
            session_type: Session type (default ``notebook``).

        Returns:
            Session dict containing ``id``, ``kernel`` (with ``id``), etc.
        """
        url = f"{self._user_api}/api/sessions"
        payload = {
            "path": path,
            "type": session_type,
            "kernel": {"name": kernel_name},
        }
        self.log.info("POST %s — creating session for %s", url, path)
        resp = self._session.post(url, json=payload, verify=self.verify_ssl, timeout=self.timeout)
        self._raise_for_status(resp, f"POST {url}")
        data = resp.json()
        kernel_id = data.get("kernel", {}).get("id")
        if kernel_id:
            self.kernel_id = kernel_id
        self.log.info("Session created: id=%s kernel_id=%s", data.get("id"), kernel_id)
        return data

    def list_sessions(self) -> List[Dict[str, Any]]:
        """
        List all active notebook sessions on the user's server.

        GET /user/{username}/api/sessions

        Returns:
            List of session dicts.
        """
        url = f"{self._user_api}/api/sessions"
        self.log.debug("GET %s", url)
        resp = self._session.get(url, verify=self.verify_ssl, timeout=self.timeout)
        self._raise_for_status(resp, f"GET {url}")
        sessions = resp.json()
        if sessions and not self.kernel_id:
            kernel_id = sessions[0].get("kernel", {}).get("id")
            if kernel_id:
                self.kernel_id = kernel_id
                self.log.info("Saved kernel_id from existing session: %s", kernel_id)
        self.log.info("Active sessions: %d", len(sessions))
        return sessions

    # ------------------------------------------------------------------
    # Kernel WebSocket — execute code
    # ------------------------------------------------------------------

    def run_kernel_code(
        self,
        code: str,
        kernel_id: Optional[str] = None,
        timeout: float = 30.0,
    ) -> str:
        """
        Execute Python code in an existing kernel via the kernel channels WebSocket.

        Connects to ``wss://{host}/user/{username}/api/kernels/{kernel_id}/channels``,
        sends an ``execute_request`` message, and collects all output (stream,
        display_data, execute_result) until the matching ``execute_reply`` arrives
        or *timeout* elapses.

        Args:
            code: Python source code to execute.
            kernel_id: Kernel ID to use. Falls back to ``self.kernel_id`` saved from
                the last :meth:`create_session` or :meth:`list_sessions` call.
            timeout: Seconds to wait before giving up.

        Returns:
            Concatenated text output from the kernel.
        """
        import threading

        kernel_id = kernel_id or self.kernel_id
        if not kernel_id:
            raise NX1ValidationError(
                "kernel_id is required — call create_session() first or pass kernel_id explicitly."
            )

        # Use user-server encoding (@-preserving) for the WebSocket path
        path = f"/user/{self._username_user_encoded}/api/kernels/{kernel_id}/channels"
        ws_url = self._ws_url(path)
        self.log.info("Connecting to kernel WS: %s", ws_url)

        msg_id = str(uuid.uuid4())
        output_parts: List[str] = []
        execution_done = threading.Event()
        ws_ref: list = []

        execute_request = {
            "header": {
                "msg_id": msg_id,
                "msg_type": "execute_request",
                "username": self.username,   # original username for protocol payload
                "session": str(uuid.uuid4()),
                "date": datetime.now(timezone.utc).isoformat(),
                "version": "5.3",
            },
            "parent_header": {},
            "metadata": {},
            "content": {
                "code": code,
                "silent": False,
                "store_history": True,
                "user_expressions": {},
                "allow_stdin": False,
                "stop_on_error": True,
            },
            "buffers": [],
            "channel": "shell",
        }

        def on_open(ws_obj):
            ws_ref.append(ws_obj)
            ws_obj.send(json.dumps(execute_request))
            self.log.debug("execute_request sent (msg_id=%s)", msg_id)

        def on_message(ws_obj, message):
            try:
                msg = json.loads(message)
            except Exception:
                return

            msg_type = msg.get("msg_type") or msg.get("header", {}).get("msg_type", "")
            parent_id = msg.get("parent_header", {}).get("msg_id", "")

            # Only handle replies to our specific request
            if parent_id != msg_id:
                return

            if msg_type == "stream":
                text = msg.get("content", {}).get("text", "")
                output_parts.append(text)
                self.log.debug("kernel stream: %r", text)

            elif msg_type in ("display_data", "execute_result"):
                data = msg.get("content", {}).get("data", {})
                text = data.get("text/plain", "")
                output_parts.append(text)
                self.log.debug("kernel %s: %r", msg_type, text)

            elif msg_type == "error":
                content = msg.get("content", {})
                tb = "\n".join(content.get("traceback", []))
                output_parts.append(
                    f"ERROR {content.get('ename')}: {content.get('evalue')}\n{tb}"
                )

            elif msg_type == "execute_reply":
                self.log.debug(
                    "execute_reply received — status=%s",
                    msg.get("content", {}).get("status"),
                )
                execution_done.set()
                ws_obj.close()

        def on_error(ws_obj, error):
            self.log.warning("Kernel WS error: %s", error)
            execution_done.set()

        sslopt = {"cert_reqs": 0} if not self.verify_ssl else {}
        ws = websocket.WebSocketApp(
            ws_url,
            header={"Authorization": f"token {self.token}"},
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
        )

        t = threading.Thread(target=ws.run_forever, kwargs={"sslopt": sslopt, "ping_interval": 0})
        t.start()
        execution_done.wait(timeout=timeout)
        if not execution_done.is_set():
            self.log.warning("Kernel execution timed out after %ss", timeout)
        if ws_ref:
            ws_ref[0].close()
        t.join(timeout=5)

        result = "".join(output_parts)
        self.log.info("Kernel code output length: %d chars", len(result))
        return result
