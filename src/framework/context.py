"""
Enterprise Framework Context

Initializes and exposes all framework services.

Reusable across:
- Incremental Batch
- Micro Batch
- Streaming
"""

from __future__ import annotations

from pyspark.sql import SparkSession
import os
from .audit import AuditFramework
from .control_table import ControlTable
from .error_logger import ErrorLogger
from .logger import PipelineLogger
from .metrics import PipelineMetrics
from .quarantine import QuarantineManager
from .schema_history import SchemaHistory
from .schema_validator import SchemaValidator
from .constants import (
    EXECUTION_BATCH,
    TRIGGER_MANUAL,
    ENV_DATABRICKS,
)


class FrameworkContext:
    """
    Central entry point for the enterprise framework.

    Creates and exposes all reusable framework components.
    """

    def __init__(
        self,
        spark: SparkSession,
        pipeline_name: str,
        pipeline_type: str,
        control_path: str,
        control_table: str,
        quarantine_path: str,
        schema_history_path: str,
        schema_history_table: str,
        schema_changes_path: str,
        schema_changes_table: str,
        execution_mode: str = EXECUTION_BATCH,
        job_name: str | None = None,
        trigger_type: str = TRIGGER_MANUAL,
        run_id: str | None = None,
        execution_id: str | None = None,
    ):
        self.spark = spark
        self.pipeline_name = pipeline_name
        self.pipeline_type = pipeline_type
        self.execution_mode = execution_mode
        self.job_name = job_name
        self.trigger_type = trigger_type
        self.run_id = run_id
        self.execution_id = execution_id
        self.control_path = control_path
        self.quarantine_path = quarantine_path
        self.schema_history = SchemaHistory(
            spark=spark,
            is_databricks=("DATABRICKS_RUNTIME_VERSION" in os.environ),
            schema_history_path=schema_history_path,
            schema_history_table=schema_history_table,
            schema_changes_path=schema_changes_path,
            schema_changes_table=schema_changes_table,
        )
        ###########################################################
        # Logger
        ###########################################################

        self.logger = PipelineLogger(pipeline_name=pipeline_name)

        ###########################################################
        # Metrics
        ###########################################################

        self.metrics = PipelineMetrics()

        ###########################################################
        # Audit
        ###########################################################

        self.audit = AuditFramework(
            spark=spark,
            pipeline_name=pipeline_name,
            pipeline_type=pipeline_type,
            execution_mode=execution_mode,
            job_name_value=job_name,
            trigger_type=trigger_type,
            run_id=run_id,
            execution_id=execution_id,
        )

        ###########################################################
        # Error Logger
        ###########################################################

        self.error_logger = ErrorLogger(logger=self.logger)

        ###########################################################
        # Schema
        ###########################################################

        self.validator = SchemaValidator()

        ###########################################################
        # Quarantine
        ###########################################################

        self.quarantine = QuarantineManager(
            quarantine_path=quarantine_path,
        )
        self.quarantine.initialize(spark)
        ###########################################################
        # Control Table
        ###########################################################

        self.control = ControlTable(
            spark=spark,
            control_path=control_path,
            control_table=control_table,
            is_databricks=("DATABRICKS_RUNTIME_VERSION" in os.environ),
        )
