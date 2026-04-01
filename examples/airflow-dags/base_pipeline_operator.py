"""
Base Airflow Operator for pipeline tools (Bronze, dbt).

Shared infrastructure for subprocess-based operators:
- Resolve project paths (Docker vs local dev)
- Run CLI commands via subprocess
- Parse target/run_results.json for structured metrics
- Push statistics to XCom
"""

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator


class BasePipelineOperator(BaseOperator):
    """
    Base operator for pipeline tools that follow the run_results.json pattern.

    Subclasses must implement:
    - _build_command() -> List[str]
    - _get_project_dir() -> Path

    Subclasses may override:
    - _pre_execute_hook(): Custom setup before execution
    - _parse_run_results(): Custom JSON parsing
    - _get_xcom_key() -> str: XCom key for stats
    """

    def __init__(self, parse_results: bool = True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parse_results = parse_results

    # -- Path Resolution (Docker vs Local Dev) --

    @staticmethod
    def _resolve_project_dir(docker_path: str, local_subdir: str) -> Path:
        """Resolve project directory for Docker or local dev."""
        if os.path.exists(docker_path):
            return Path(docker_path)
        project_root = Path(__file__).parent.parent.parent.parent
        return project_root / local_subdir

    @staticmethod
    def _resolve_executable(docker_paths: List[str], fallback: Optional[str] = None) -> str:
        """Find executable from Docker paths or fall back to local."""
        for path in docker_paths:
            if os.path.exists(path):
                return path
        return fallback or sys.executable

    # -- Subprocess Execution --

    def _run_subprocess(
        self, cmd: List[str], cwd: str, env: Optional[Dict[str, str]] = None
    ) -> subprocess.CompletedProcess:
        """Run command, log output, and return result."""
        return subprocess.run(
            cmd, cwd=cwd, env=env, capture_output=True, text=True, check=False
        )

    def _log_output(self, result: subprocess.CompletedProcess) -> None:
        """Log stdout and stderr."""
        if result.stdout:
            self.log.info(f"\n{result.stdout}")
        if result.stderr:
            self.log.warning(f"\n{result.stderr}")

    def _check_result(self, result: subprocess.CompletedProcess, tool_name: str = "Pipeline") -> None:
        """Check return code and raise AirflowException if non-zero."""
        if result.returncode != 0:
            error_output = "(no output)"
            if result.stdout:
                lines = result.stdout.split("\n")
                error_output = "\n".join(lines[-50:])

            raise AirflowException(
                f"{tool_name} failed with exit code {result.returncode}\n\n"
                f"Output (last 50 lines):\n{error_output}\n\n"
                f"STDERR: {result.stderr or '(empty)'}"
            )

    # -- JSON Results Parsing --

    def _read_results_json(self, project_dir: Path, filename: str = "run_results.json") -> Optional[Dict]:
        """Read and parse JSON from target/ directory."""
        results_path = project_dir / "target" / filename
        if not results_path.exists():
            self.log.warning(f"{filename} not found at {results_path}")
            return None

        try:
            with open(results_path, "r") as f:
                return json.load(f)
        except Exception as e:
            self.log.error(f"Failed to parse {filename}: {e}")
            return None

    def _count_results_by_status(
        self, results: List[Dict], item_label: str = "items"
    ) -> Dict[str, Any]:
        """
        Count results by status from run_results.json.

        Shared pattern: iterate results[], check status, extract metrics.
        Used by both dbt and bronze operators.
        """
        stats = {
            f"total_{item_label}": len(results),
            f"successful_{item_label}": 0,
            f"failed_{item_label}": 0,
            f"skipped_{item_label}": 0,
            "execution_time_seconds": 0.0,
            item_label: [],
        }

        for entry in results:
            status = entry.get("status")
            name = entry.get("unique_id", "unknown")
            execution_time = entry.get("execution_time", 0)
            adapter_response = entry.get("adapter_response", {})
            rows_affected = adapter_response.get("rows_affected", 0)

            if status == "success":
                stats[f"successful_{item_label}"] += 1
            elif status == "error":
                stats[f"failed_{item_label}"] += 1
            elif status == "skipped":
                stats[f"skipped_{item_label}"] += 1

            stats["execution_time_seconds"] += execution_time
            stats[item_label].append({
                "name": name,
                "status": status,
                "execution_time": execution_time,
                "rows_affected": rows_affected,
            })

        return stats

    # -- Environment Loading --

    def _load_env(self, component: str) -> None:
        """
        Load secrets and environment variables.

        Works for both Docker and local:
        - Docker: Secrets loaded by docker-entrypoint.sh (no-op)
        - Local: Loads secrets/db.credentials and component/.env
        """
        try:
            project_root = Path("/workspace") if os.path.exists("/workspace") else \
                Path(__file__).resolve().parent.parent.parent.parent

            if str(project_root) not in sys.path:
                sys.path.insert(0, str(project_root))

            from lib.env_loader import load_all_env
            load_all_env(component)
            self.log.info("Environment variables loaded successfully")

        except ImportError:
            self.log.warning("env_loader not found — assuming Docker environment")
        except Exception as e:
            self.log.warning(f"Failed to load environment: {e}")
