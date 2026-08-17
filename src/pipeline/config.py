from dataclasses import dataclass, field
from pathlib import Path
import os

@dataclass
class StreamingConfig:
    """Streaming-native configuration; batch framework is intentionally not used."""
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
    max_files_per_trigger: int = 20
    tracked_attribute_cols: tuple[str, ...] = ("account_status", "account_tier", "branch_region")

    def __post_init__(self):
        if os.getenv("DATABRICKS_RUNTIME_VERSION"):
            self.base_path = Path(
                os.getenv(
                    "STREAMING_BASE_PATH",
                    "/Volumes/workspace/default/streaming_scd2",
                )
            )
        else:
            self.base_path = Path(self.base_path)

        self.stream_source_path = self.base_path / "stream_source"
        self.checkpoint_path = self.base_path / "checkpoints" / "account_scd2_stream"
        self.target_table_path = self.base_path / "account_state_scd2"
        self.event_ledger_path = self.base_path / "account_event_ledger"
        self.audit_path = self.base_path / "streaming_audit.jsonl"
        self.dead_letter_path = self.base_path / "dead_letter"
        if self.create_local_dirs and not os.getenv("DATABRICKS_RUNTIME_VERSION"):
            for p in [self.stream_source_path, self.checkpoint_path.parent, self.target_table_path.parent, self.dead_letter_path]:
                p.mkdir(parents=True, exist_ok=True)
