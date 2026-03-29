"""Process-based sandbox service implementation.

This service creates sandboxes by spawning separate agent server processes,
each running within a dedicated directory.
"""

import asyncio
import logging
import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import AsyncGenerator

import base62
import httpx
import psutil
from fastapi import Request
from pydantic import BaseModel, ConfigDict, Field

from openhands.agent_server.utils import utc_now
from openhands.app_server.errors import SandboxError
from openhands.app_server.sandbox.sandbox_models import (
    AGENT_SERVER,
    ExposedUrl,
    SandboxInfo,
    SandboxPage,
    SandboxStatus,
)
from openhands.app_server.sandbox.sandbox_service import (
    SandboxService,
    SandboxServiceInjector,
)
from openhands.app_server.sandbox.sandbox_spec_models import SandboxSpecInfo
from openhands.app_server.sandbox.sandbox_spec_service import SandboxSpecService
from openhands.app_server.services.injector import InjectorState
from openhands.app_server.utils.docker_utils import (
    replace_localhost_hostname_for_docker,
)

_logger = logging.getLogger(__name__)


class ProcessInfo(BaseModel):
    """Information about a running process."""

    pid: int
    port: int
    user_id: str | None
    working_dir: str
    session_api_key: str
    created_at: datetime
    sandbox_spec_id: str

    model_config = ConfigDict(frozen=True)


# Global store
_processes: dict[str, ProcessInfo] = {}


@dataclass
class ProcessSandboxService(SandboxService):
    """Sandbox service that spawns separate agent server processes.

    Each sandbox is implemented as a separate Python process running the
    action execution server, with each process:
    - Operating in a dedicated directory
    - Listening on a unique port
    - Having its own session API key
    """

    user_id: str | None
    sandbox_spec_service: SandboxSpecService
    base_working_dir: str
    base_port: int
    python_executable: str
    agent_server_module: str
    health_check_path: str
    httpx_client: httpx.AsyncClient

    def __post_init__(self):
        """Initialize the service after dataclass creation."""
        # Ensure base working directory exists
        os.makedirs(self.base_working_dir, exist_ok=True)

    def _find_unused_port(self) -> int:
        """Find an unused port starting from base_port."""
        port = self.base_port
        while port < self.base_port + 10000:  # Try up to 10000 ports
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(('', port))
                    return port
            except OSError:
                port += 1
        raise SandboxError('No available ports found')

    def _create_sandbox_directory(self, sandbox_id: str) -> str:
        """Create a dedicated directory for the sandbox."""
        sandbox_dir = os.path.join(self.base_working_dir, sandbox_id)
        os.makedirs(sandbox_dir, exist_ok=True)
        return sandbox_dir

    # async def _start_agent_process(
    #     self,
    #     sandbox_id: str,
    #     port: int,
    #     working_dir: str,
    #     session_api_key: str,
    #     sandbox_spec: SandboxSpecInfo,
    # ) -> subprocess.Popen:
    #     """Start the agent server process."""

    #     # Prepare environment variables
    #     env = os.environ.copy()
    #     env.update(sandbox_spec.initial_env)
    #     env['OH_SESSION_API_KEYS_0'] = session_api_key

    #     # Prepare command arguments
    #     cmd = [
    #         self.python_executable,
    #         '-m',
    #         self.agent_server_module,
    #         '--port',
    #         str(port),
    #     ]

    #     _logger.info(
    #         f'Starting agent process for sandbox {sandbox_id}: {" ".join(cmd)}'
    #     )

    #     try:
    #         # Start the process
    #         process = subprocess.Popen(
    #             cmd,
    #             env=env,
    #             cwd=working_dir,
    #             stdout=subprocess.PIPE,
    #             stderr=subprocess.PIPE,
    #         )

    #         # Wait a moment for the process to start
    #         await asyncio.sleep(1)

    #         # Check if process is still running
    #         if process.poll() is not None:
    #             stdout, stderr = process.communicate()
    #             raise SandboxError(f'Agent process failed to start: {stderr.decode()}')

    #         return process

    #     except Exception as e:
    #         raise SandboxError(f'Failed to start agent process: {e}')

    async def _start_agent_process(
        self,
        sandbox_id: str,
        port: int,
        working_dir: str,
        session_api_key: str,
        sandbox_spec: SandboxSpecInfo,
    ) -> subprocess.Popen:
        """Start the agent server process with output redirection."""

        # Prepare environment variables
        env = os.environ.copy()
        env.update(sandbox_spec.initial_env)
        env['SESSION_API_KEY'] = session_api_key

        # 设置日志级别为ERROR，减少输出
        env['LOG_LEVEL'] = 'ERROR'
        env['PYTHONUNBUFFERED'] = '1'

        # 创建日志文件
        log_file_path = os.path.join(working_dir, 'agent_server.log')
        error_log_path = os.path.join(working_dir, 'agent_server_error.log')

        _logger.info(f'Starting agent process for sandbox {sandbox_id}, logs: {log_file_path}')

        # Prepare command arguments
        cmd = [
        self.python_executable,
        '-m',
        self.agent_server_module,
        '--port',
        str(port),
    ]

        # 添加可选参数来减少日志输出（如果agent_server支持）
        # cmd.extend(['--log-level', 'ERROR'])

        try:
            # 打开日志文件
            stdout_file = open(log_file_path, 'wb')
            stderr_file = open(error_log_path, 'wb')

            # Start the process with redirected output
            process = subprocess.Popen(
                cmd,
                env=env,
                cwd=working_dir,
                stdout=stdout_file,
                stderr=stderr_file,
                # 使用管道也可以，但文件更可靠
                # stdout=subprocess.PIPE,
                # stderr=subprocess.PIPE,
            )

            # 保存文件引用以便后续关闭（但Popen会在进程结束时自动关闭）

            # Wait a moment for the process to start
            await asyncio.sleep(2)

            # Check if process is still running
            if process.poll() is not None:
                # 读取错误日志
                try:
                    with open(error_log_path, 'r') as f:
                        error_output = f.read()
                except:
                    error_output = "Could not read error log"

                raise SandboxError(
                    f'Agent process failed to start.\n'
                    f'Exit code: {process.returncode}\n'
                    f'Error log: {error_output[:500]}'
                )

            _logger.info(f'Agent process for sandbox {sandbox_id} started successfully with PID {process.pid}')
            return process

        except Exception as e:
            _logger.error(f'Failed to start agent process for sandbox {sandbox_id}: {e}')
            raise SandboxError(f'Failed to start agent process: {e}')



    # async def _wait_for_server_ready(self, port: int, timeout: int = 120) -> bool:
    #     """Wait for the agent server to be ready."""
    #     start_time = time.time()
    #     while time.time() - start_time < timeout:
    #         try:
    #             url = replace_localhost_hostname_for_docker(
    #                 f'http://localhost:{port}/alive'
    #             )
    #             response = await self.httpx_client.get(url, timeout=5.0)
    #             if response.status_code == 200:
    #                 data = response.json()
    #                 if data.get('status') == 'ok':
    #                     return True
    #         except Exception:
    #             pass
    #         await asyncio.sleep(1)
    #     return False

    async def _wait_for_server_ready(self, port: int, timeout: int = 60) -> bool:
        """Wait for the agent server to be ready with better diagnostics."""
        start_time = time.time()
        last_error = None
        check_count = 0

        _logger.info(f"Waiting for agent server on port {port} to be ready...")

        while time.time() - start_time < timeout:
            check_count += 1
            try:
                url = replace_localhost_hostname_for_docker(
                    f'http://localhost:{port}/alive'
                )
                response = await self.httpx_client.get(url, timeout=5.0)
                if response.status_code == 200:
                    data = response.json()
                    if data.get('status') == 'ok':
                        elapsed = time.time() - start_time
                        _logger.info(f"Agent server on port {port} ready after {elapsed:.1f}s")
                        return True
                    else:
                        _logger.debug(f"Health check returned: {data}")
                else:
                    _logger.debug(f"Health check returned status {response.status_code}")
            except Exception as e:
                last_error = str(e)
                _logger.debug(f"Health check attempt {check_count} failed: {e}")

            # 指数退避，但最大等待2秒
            wait_time = min(0.5 * (2 ** min(check_count, 4)), 2.0)
            await asyncio.sleep(wait_time)

        _logger.error(f"Agent server on port {port} failed to start within {timeout}s")
        if last_error:
            _logger.error(f"Last error: {last_error}")

        return False


    def _get_process_status(self, process_info: ProcessInfo) -> SandboxStatus:
        """Get the status of a process."""
        try:
            process = psutil.Process(process_info.pid)
            is_running = process.is_running()
            _logger.info(f'[DEBUG] Checking process {process_info.pid}: is_running={is_running}')
            if is_running:
                # Any running status means the process is RUNNING
                # STATUS_SLEEPING is the normal state for most processes
                # STATUS_RUNNING only means currently on CPU
                _logger.info(f'[DEBUG] Process {process_info.pid} is RUNNING')
                return SandboxStatus.RUNNING
            else:
                _logger.info(f'[DEBUG] Process {process_info.pid} is MISSING')
                return SandboxStatus.MISSING
        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
            _logger.info(f'[DEBUG] Process {process_info.pid} exception: {e}')
            return SandboxStatus.MISSING

    async def _process_to_sandbox_info(
        self, sandbox_id: str, process_info: ProcessInfo
    ) -> SandboxInfo:
        """Convert process info to sandbox info."""
        status = self._get_process_status(process_info)
        _logger.info(f'[DEBUG] Sandbox {sandbox_id} initial status: {status}')

        exposed_urls = None
        session_api_key = None

        if status == SandboxStatus.RUNNING:
            # Check if server is actually responding
            # Note: This check can fail temporarily during initialization,
            # so we don't change status to ERROR, just leave exposed_urls as None
            try:
                url = replace_localhost_hostname_for_docker(
                    f'http://localhost:{process_info.port}{self.health_check_path}'
                )
                _logger.info(f'[DEBUG] Checking health at {url}')
                response = await self.httpx_client.get(url, timeout=5.0)
                _logger.info(f'[DEBUG] Health check response status: {response.status_code}')
                if response.status_code == 200:
                    exposed_urls = [
                        ExposedUrl(
                            name=AGENT_SERVER,
                            url=f'http://localhost:{process_info.port}',
                            port=process_info.port,
                        ),
                    ]
                    session_api_key = process_info.session_api_key
            except Exception as e:
                # Health check failed - this could be temporary, so don't change status
                _logger.info(f'[DEBUG] Health check failed: {e}')
                pass

        result = SandboxInfo(
            id=sandbox_id,
            created_by_user_id=process_info.user_id,
            sandbox_spec_id=process_info.sandbox_spec_id,
            status=status,
            session_api_key=session_api_key,
            exposed_urls=exposed_urls,
            created_at=process_info.created_at,
        )
        _logger.info(f'[DEBUG] Sandbox {sandbox_id} final status: {result.status}')
        return result

    async def search_sandboxes(
        self,
        page_id: str | None = None,
        limit: int = 100,
    ) -> SandboxPage:
        """Search for sandboxes."""
        # Get all process infos
        all_processes = list(_processes.items())

        # Sort by creation time (newest first)
        all_processes.sort(key=lambda x: x[1].created_at, reverse=True)

        # Apply pagination
        start_idx = 0
        if page_id:
            try:
                start_idx = int(page_id)
            except ValueError:
                start_idx = 0

        end_idx = start_idx + limit
        paginated_processes = all_processes[start_idx:end_idx]

        # Convert to sandbox infos
        items = []
        for sandbox_id, process_info in paginated_processes:
            sandbox_info = await self._process_to_sandbox_info(sandbox_id, process_info)
            items.append(sandbox_info)

        # Determine next page ID
        next_page_id = None
        if end_idx < len(all_processes):
            next_page_id = str(end_idx)

        return SandboxPage(items=items, next_page_id=next_page_id)

    async def get_sandbox(self, sandbox_id: str) -> SandboxInfo | None:
        """Get a single sandbox."""
        process_info = _processes.get(sandbox_id)
        if process_info is None:
            return None

        return await self._process_to_sandbox_info(sandbox_id, process_info)

    async def get_sandbox_by_session_api_key(
        self, session_api_key: str
    ) -> SandboxInfo | None:
        """Get a single sandbox by session API key."""
        # Search through all processes to find one with matching session_api_key
        for sandbox_id, process_info in _processes.items():
            if process_info.session_api_key == session_api_key:
                return await self._process_to_sandbox_info(sandbox_id, process_info)

        return None

    async def start_sandbox(
        self, sandbox_spec_id: str | None = None, sandbox_id: str | None = None
    ) -> SandboxInfo:
        """Start a new sandbox."""
        # Get sandbox spec
        if sandbox_spec_id is None:
            sandbox_spec = await self.sandbox_spec_service.get_default_sandbox_spec()
        else:
            sandbox_spec_maybe = await self.sandbox_spec_service.get_sandbox_spec(
                sandbox_spec_id
            )
            if sandbox_spec_maybe is None:
                raise ValueError('Sandbox Spec not found')
            sandbox_spec = sandbox_spec_maybe

        # Generate unique sandbox ID and session API key
        # Use provided sandbox_id if available, otherwise generate a random one
        if sandbox_id is None:
            sandbox_id = base62.encodebytes(os.urandom(16))
        session_api_key = base62.encodebytes(os.urandom(32))

        # Find available port
        port = self._find_unused_port()

        # Create sandbox directory
        working_dir = self._create_sandbox_directory(sandbox_id)

        # Start the agent process
        process = await self._start_agent_process(
            sandbox_id=sandbox_id,
            port=port,
            working_dir=working_dir,
            session_api_key=session_api_key,
            sandbox_spec=sandbox_spec,
        )

        # Store process info
        process_info = ProcessInfo(
            pid=process.pid,
            port=port,
            user_id=self.user_id,
            working_dir=working_dir,
            session_api_key=session_api_key,
            created_at=utc_now(),
            sandbox_spec_id=sandbox_spec.id,
        )
        _processes[sandbox_id] = process_info

        # Wait for server to be ready
        if not await self._wait_for_server_ready(port):
            # Clean up if server didn't start properly
            await self.delete_sandbox(sandbox_id)
            raise SandboxError('Agent Server Failed to start properly')

        return await self._process_to_sandbox_info(sandbox_id, process_info)

    async def resume_sandbox(self, sandbox_id: str) -> bool:
        """Resume a paused sandbox."""
        process_info = _processes.get(sandbox_id)
        if process_info is None:
            return False

        try:
            process = psutil.Process(process_info.pid)
            if process.status() == psutil.STATUS_STOPPED:
                process.resume()
            return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False

    async def pause_sandbox(self, sandbox_id: str) -> bool:
        """Pause a running sandbox."""
        process_info = _processes.get(sandbox_id)
        if process_info is None:
            return False

        try:
            process = psutil.Process(process_info.pid)
            if process.is_running():
                process.suspend()
            return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False

    async def delete_sandbox(self, sandbox_id: str) -> bool:
        """Delete a sandbox."""
        process_info = _processes.get(sandbox_id)
        if process_info is None:
            return False

        try:
            # Terminate the process
            process = psutil.Process(process_info.pid)
            if process.is_running():
                # Try graceful termination first
                process.terminate()
                try:
                    process.wait(timeout=10)
                except psutil.TimeoutExpired:
                    # Force kill if graceful termination fails
                    process.kill()
                    process.wait(timeout=5)

            # Clean up the working directory
            import shutil

            if os.path.exists(process_info.working_dir):
                shutil.rmtree(process_info.working_dir, ignore_errors=True)

            # Remove from our tracking
            del _processes[sandbox_id]

            return True

        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError) as e:
            _logger.warning(f'Error deleting sandbox {sandbox_id}: {e}')
            # Still remove from tracking even if cleanup failed
            if sandbox_id in _processes:
                del _processes[sandbox_id]
            return True


class ProcessSandboxServiceInjector(SandboxServiceInjector):
    """Dependency injector for process sandbox services."""

    base_working_dir: str = Field(
        default='/tmp/openhands-sandboxes',
        description='Base directory for sandbox working directories',
    )
    base_port: int = Field(
        default=8000, description='Base port number for agent servers'
    )
    python_executable: str = Field(
        default=sys.executable,
        description='Python executable to use for agent processes',
    )
    agent_server_module: str = Field(
        default='openhands.agent_server',
        description='Python module for the agent server',
    )
    health_check_path: str = Field(
        default='/alive', description='Health check endpoint path'
    )

    async def inject(
        self, state: InjectorState, request: Request | None = None
    ) -> AsyncGenerator[SandboxService, None]:
        # Define inline to prevent circular lookup
        from openhands.app_server.config import (
            get_httpx_client,
            get_sandbox_spec_service,
            get_user_context,
        )

        async with (
            get_httpx_client(state, request) as httpx_client,
            get_sandbox_spec_service(state, request) as sandbox_spec_service,
            get_user_context(state, request) as user_context,
        ):
            user_id = await user_context.get_user_id()
            yield ProcessSandboxService(
                user_id=user_id,
                sandbox_spec_service=sandbox_spec_service,
                base_working_dir=self.base_working_dir,
                base_port=self.base_port,
                python_executable=self.python_executable,
                agent_server_module=self.agent_server_module,
                health_check_path=self.health_check_path,
                httpx_client=httpx_client,
            )
