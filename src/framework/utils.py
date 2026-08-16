"""
Enterprise Framework Utilities

Reusable across:
- Audit Framework
- Logger
- Metrics
- Quarantine
- Schema Validation
"""

from __future__ import annotations

import getpass
import socket
import time
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from .constants import (
    FRAMEWORK_VERSION,
    DEFAULT_EXECUTION_ENGINE,
    ENV_DATABRICKS,
    ENV_LOCAL,
)

# ============================================================
# ID Helpers
# ============================================================


def generate_run_id() -> str:
    """Generate a unique run identifier."""
    return str(uuid.uuid4())


def generate_execution_id() -> str:
    """Generate a unique execution identifier."""
    return str(uuid.uuid4())


def generate_batch_id() -> str:
    """Generate a unique batch identifier."""
    return str(uuid.uuid4())


# ============================================================
# Time Helpers
# ============================================================


def utc_now() -> datetime:
    """Return current UTC timestamp."""
    return datetime.now(timezone.utc)


def start_timer() -> float:
    """Start a high-precision timer."""
    return time.perf_counter()


def elapsed_seconds(start_time: float) -> float:
    """Calculate elapsed time in seconds."""
    return round(time.perf_counter() - start_time, 3)


# ============================================================
# Environment Helpers
# ============================================================


def detect_environment(
    spark: SparkSession | None = None,
) -> str:
    """
    Detect whether the pipeline is running locally
    or in Databricks.
    """
    if spark is None:
        return ENV_LOCAL

    try:
        spark.conf.get("spark.databricks.workspaceUrl")
        return ENV_DATABRICKS
    except Exception:
        return ENV_LOCAL


def execution_engine() -> str:
    """Return execution engine."""
    return DEFAULT_EXECUTION_ENGINE


def job_name(
    spark: SparkSession | None = None,
) -> str | None:
    """Return Databricks job name if available."""

    if spark is None:
        return None

    try:
        return spark.conf.get("spark.databricks.job.name")
    except Exception:
        return None


# ============================================================
# Databricks Helpers
# ============================================================


def workspace_name(
    spark: SparkSession | None = None,
) -> str | None:
    """Return Databricks workspace URL if available."""

    if spark is None:
        return None

    try:
        return spark.conf.get("spark.databricks.workspaceUrl")
    except Exception:
        return None


def cluster_name(
    spark: SparkSession | None = None,
) -> str | None:
    """Return Databricks cluster name if available."""

    if spark is None:
        return None

    try:
        return spark.conf.get("spark.databricks.clusterUsageTags.clusterName")
    except Exception:
        return None


def notebook_path(
    spark: SparkSession | None = None,
) -> str | None:
    """Return notebook path if available."""

    if spark is None:
        return None

    try:
        return spark.conf.get("spark.databricks.notebook.path")
    except Exception:
        return None


def workspace_user(
    spark: SparkSession | None = None,
) -> str | None:
    """Return Databricks workspace user if available."""

    if spark is None:
        return None

    try:
        return spark.conf.get("spark.databricks.userInfo.userName")
    except Exception:
        return None


# ============================================================
# System Helpers
# ============================================================


def hostname() -> str:
    """Return machine hostname."""
    return socket.gethostname()


def user_name() -> str:
    """Return current operating system user."""
    return getpass.getuser()


def framework_version() -> str:
    """Return framework version."""
    return FRAMEWORK_VERSION


__all__ = [
    "generate_run_id",
    "generate_execution_id",
    "generate_batch_id",
    "utc_now",
    "start_timer",
    "elapsed_seconds",
    "detect_environment",
    "execution_engine",
    "job_name",
    "workspace_name",
    "cluster_name",
    "notebook_path",
    "workspace_user",
    "hostname",
    "user_name",
    "framework_version",
]
