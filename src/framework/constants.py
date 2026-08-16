"""
Enterprise Framework Constants

Reusable across:
- Incremental Batch
- Micro Batch
- Streaming
"""

# ============================================================
# Framework
# ============================================================

FRAMEWORK_VERSION = "1.0.0"
DEFAULT_EXECUTION_ENGINE = "Spark"

# ============================================================
# Environments
# ============================================================

ENV_LOCAL = "local"
ENV_DATABRICKS = "databricks"

# ============================================================
# Pipeline Status
# ============================================================

STATUS_SKIPPED = "SKIPPED"
STATUS_STARTED = "STARTED"
STATUS_RUNNING = "RUNNING"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_WARNING = "WARNING"

# ============================================================
# Pipeline Types
# ============================================================

PIPELINE_INCREMENTAL = "incremental"
PIPELINE_MICROBATCH = "micro_batch"
PIPELINE_STREAMING = "streaming"

# ============================================================
# Execution Modes
# ============================================================

EXECUTION_BATCH = "batch"
EXECUTION_MICROBATCH = "micro_batch"
EXECUTION_STREAMING = "streaming"

# ============================================================
# Load Types
# ============================================================

LOAD_FULL = "full"
LOAD_INCREMENTAL = "incremental"
LOAD_SNAPSHOT = "snapshot"

# ============================================================
# Trigger Types
# ============================================================

TRIGGER_MANUAL = "MANUAL"
TRIGGER_SCHEDULE = "SCHEDULE"
TRIGGER_EVENT = "EVENT"
TRIGGER_API = "API"

# ============================================================
# Common Stages
# ============================================================

STAGE_BRONZE = "BRONZE"
STAGE_SILVER = "SILVER"
STAGE_GOLD = "GOLD"
STAGE_VALIDATION = "VALIDATION"
STAGE_RECONCILIATION = "RECONCILIATION"
STAGE_QUARANTINE = "QUARANTINE"
STAGE_AUDIT = "AUDIT"
STAGE_CONTROL = "CONTROL"

# ============================================================
# Default Values
# ============================================================

DEFAULT_RETRY_COUNT = 0
DEFAULT_FILES_PROCESSED = 0
DEFAULT_ROWS = 0
DEFAULT_DURATION = 0.0

# ============================================================
# Storage
# ============================================================

STORAGE_LOCAL = "local"
STORAGE_DELTA = "delta"
STORAGE_UNITY_VOLUME = "unity_catalog_volume"

# ============================================================
# Audit Result
# ============================================================

RESULT_PASS = "PASS"
RESULT_FAIL = "FAIL"

# ============================================================
# Audit Columns
# ============================================================

COL_PIPELINE = "pipeline"
COL_STAGE = "stage"
COL_STATUS = "status"
COL_MESSAGE = "message"
COL_ROWS = "rows"
COL_DURATION_SECONDS = "duration_seconds"
COL_SNAPSHOT_DAY = "snapshot_day"
COL_PROCESSED_DAY = "processed_day"
COL_CREATED_AT = "created_at"
COL_UPDATED_AT = "updated_at"
COL_FILE_NAME = "file_name"
COL_BATCH_ID = "batch_id"

# ============================================================
# Metadata Keys
# ============================================================

META_CONTROL = "control"
META_AUDIT = "audit"
META_WATERMARK = "watermark"
META_RECONCILIATION = "reconciliation"

# ============================================================
# Merge Operations
# ============================================================

MERGE_INSERT = "INSERT"
MERGE_UPDATE = "UPDATE"
MERGE_DELETE = "DELETE"

# ============================================================
# Validation Types
# ============================================================

VALIDATION_SCHEMA = "schema"
VALIDATION_NULL = "null"
VALIDATION_DUPLICATE = "duplicate"
VALIDATION_ROWCOUNT = "rowcount"
VALIDATION_RECONCILIATION = "reconciliation"

# ============================================================
# Table Names
# ============================================================

TABLE_BRONZE = "bronze"
TABLE_SILVER = "silver"
TABLE_GOLD = "gold"
TABLE_AUDIT = "audit"
TABLE_CONTROL = "control"
TABLE_RECONCILIATION = "reconciliation"
