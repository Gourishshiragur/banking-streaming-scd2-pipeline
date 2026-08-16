from dataclasses import dataclass, field
from pathlib import Path
import os


@dataclass
class StreamingConfig:
    """Runtime configuration shared by local and Databricks streaming runs."""

    base_path: Path | str = Path("data")
    create_local_dirs: bool = True
    stream_source_path: Path = field(init=False)
    checkpoint_path: Path = field(init=False)
    target_table_path: Path = field(init=False)
    event_ledger_path: Path = field(init=False)
    audit_path: Path = field(init=False)
    dead_letter_path: Path = field(init=False)

    entity_key: str = "account_id"
    event_time_col: str = "event_time"
    watermark_duration: str = "10 minutes"
    trigger_interval: str = "5 seconds"
    max_files_per_trigger: int = 5

    tracked_attribute_cols: tuple[str, ...] = (
        "account_status",
        "account_tier",
        "branch_region",
    )

    def __post_init__(self):
        self.base_path = Path(self.base_path)
        self.stream_source_path = self.base_path / "stream_source"
        self.checkpoint_path = self.base_path / "checkpoints" / "account_scd2_stream"
        self.target_table_path = self.base_path / "delta" / "account_state_scd2"
        self.event_ledger_path = self.base_path / "delta" / "account_event_ledger"
        self.audit_path = self.base_path / "delta" / "streaming_audit"
        self.dead_letter_path = self.base_path / "dead_letter"

        # Local filesystem directories are created by Python. Databricks Unity
        # Catalog Volume paths are provisioned through dbutils/Volume setup, not
        # pathlib.mkdir, so the notebook disables this flag.
        is_databricks = bool(os.getenv("DATABRICKS_RUNTIME_VERSION"))
        if self.create_local_dirs and not is_databricks:
            for p in [
                self.stream_source_path,
                self.checkpoint_path.parent,
                self.target_table_path.parent,
                self.dead_letter_path,
            ]:
                p.mkdir(parents=True, exist_ok=True)
