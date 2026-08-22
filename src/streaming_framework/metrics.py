"""Metrics object for a single streaming micro-batch.

The framework accepts counts already produced by the processing path. It does
not issue extra Spark actions just to calculate operational metrics.
"""
from dataclasses import dataclass, asdict

@dataclass(frozen=True)
class BatchMetrics:
    input_rows: int
    new_rows: int
    impacted_accounts: int
    history_rows: int
    current_rows: int
    status: str = "SUCCESS"

    def as_dict(self) -> dict:
        return asdict(self)
