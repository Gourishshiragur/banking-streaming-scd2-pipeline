"""Streaming schema policy."""

def critical_columns() -> tuple[str, ...]:
    return ("event_id", "account_id", "event_time", "account_status", "account_tier", "branch_region")
