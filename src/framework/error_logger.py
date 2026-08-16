"""
Enterprise Error Logger

Reusable across:
- Incremental Batch
- Micro Batch
- Streaming
"""

from __future__ import annotations

import traceback


from .logger import PipelineLogger
from typing import Any


class ErrorLogger:
    """
    Enterprise reusable error logger.
    """

    def __init__(
        self,
        logger: PipelineLogger,
    ):

        self.logger = logger

    ####################################################################
    # Public API
    ####################################################################

    def log_exception(
        self,
        stage: str,
        exception: Exception,
        pipeline_name: str | None = None,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Log an exception and return structured metadata.
        """

        error_record = {
            "pipeline_name": pipeline_name,
            "run_id": run_id,
            "stage": stage,
            "error_type": type(exception).__name__,
            "error_message": str(exception),
            "stack_trace": "".join(
                traceback.format_exception(
                    type(exception),
                    exception,
                    exception.__traceback__,
                )
            ),
        }

        self.logger.exception(
            f"[{stage}] Pipeline failed",
            exception,
        )

        return error_record
