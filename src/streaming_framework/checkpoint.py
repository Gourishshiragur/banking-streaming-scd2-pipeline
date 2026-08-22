"""Checkpoint policy for the streaming framework."""
from pathlib import Path

def validate_checkpoint_path(path: str | Path) -> str:
    value = str(path)
    if not value:
        raise ValueError("checkpoint path must not be empty")
    return value
