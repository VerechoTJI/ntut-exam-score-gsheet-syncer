"""Separate backend backoff limits from Google network timeouts (seconds)."""
import math
import os


def seconds(name, default):
    raw = (os.getenv(name) or '').strip() or str(default)
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f'{name} must be a finite number greater than 0 (seconds)') from None
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f'{name} must be a finite number greater than 0 (seconds)')
    return value
