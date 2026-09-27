"""Local control plane; Spark data stays in Databricks."""
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import subprocess

from mobility.config import PipelineConfig, source_month


def config_from_env() -> PipelineConfig:
    return PipelineConfig(os.environ.get("DATABRICKS_CATALOG", "workspace"),
                          os.environ.get("MOBILITY_SCHEMA_PREFIX", "mobility"))


def run_remote_job(service: str, month: str, run_id: str, attempt: int, client=None):
    if service not in {"bootstrap", "ingestion", "preparation"}:
        raise ValueError("Unknown remote service")
    source_month(month)
    if client is None:
        from databricks.sdk import WorkspaceClient
        client = WorkspaceClient()
    job_id = int(os.environ[f"MOBILITY_{service.upper()}_JOB_ID"])
    # Network retries share a token; an Airflow retry gets a fresh remote attempt.
    token = hashlib.sha256(f"{service}:{month}:{run_id}:{attempt}".encode()).hexdigest()
    parameters = {} if service == "bootstrap" else {"source_month": month, "run_id": run_id}
    if service == "ingestion":
        parameters["source_mode"] = "volume"
    pending = client.jobs.run_now(job_id=job_id, job_parameters=parameters, idempotency_token=token)
    try:
        result = pending.result(timeout=timedelta(hours=2, minutes=5))
    except TimeoutError:
        # Prevent a timed-out run from writing concurrently with the next retry.
        client.jobs.cancel_run_and_wait(run_id=pending.run_id)
        raise
    return {"databricks_run_id": result.run_id, "run_page_url": result.run_page_url}


def dbt_environment() -> dict:
    environment = os.environ.copy()
    environment["DATABRICKS_HOST"] = environment["DATABRICKS_HOST"].removeprefix("https://").rstrip("/")
    if not environment.get("DATABRICKS_TOKEN"):
        from databricks.sdk import WorkspaceClient
        # Databricks CLI OAuth login is reused by the SDK. The temporary access
        # token is passed only to dbt's process environment, never the command line.
        header = WorkspaceClient().config.authenticate().get("Authorization", "")
        if not header.startswith("Bearer "):
            raise RuntimeError("Authenticate locally with Databricks CLI OAuth or set DATABRICKS_TOKEN")
        environment["DATABRICKS_TOKEN"] = header.removeprefix("Bearer ")
    return environment


def run_dbt(month: str, run_id: str, smoke: bool = False):
    source_month(month)
    root = Path(os.environ["MOBILITY_PROJECT_ROOT"]).resolve()
    executable = Path(os.environ["DBT_EXECUTABLE"])
    if not executable.is_file():
        raise FileNotFoundError("DBT_EXECUTABLE must point to the separate dbt environment")
    evidence_id = hashlib.sha256(run_id.encode()).hexdigest()[:16]
    artifact_dir = root / "artifacts" / evidence_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    base = [str(executable), "--project-dir", str(root / "dbt"),
            "--profiles-dir", str(root / "dbt"), "--target-path", str(artifact_dir / "dbt"),
            "--log-path", str(artifact_dir / "logs")]
    commands = [["build", "--select", "tag:smoke"]] if smoke else [
        ["run-operation", "assert_ready", "--args", json.dumps({"requested_month": month})],
        ["build", "--exclude", "tag:smoke"],
        ["docs", "generate", "--no-compile"],
    ]
    for command in commands:
        subprocess.run(base + command, env=dbt_environment(), check=True, cwd=root, timeout=3600)
    return {"artifacts_directory": str(artifact_dir), "status": "SUCCESS"}
