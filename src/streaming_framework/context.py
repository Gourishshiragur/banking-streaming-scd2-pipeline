"""Immutable execution context for one Structured Streaming micro-batch."""
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class StreamingBatchContext:
    pipeline_name: str
    batch_id: int
    checkpoint_path: Path
    target_path: Path
    audit_path: Path
