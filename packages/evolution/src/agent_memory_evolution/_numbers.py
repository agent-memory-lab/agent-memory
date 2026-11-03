"""Numeric contracts shared by evaluation inputs and defensive gate checks."""

from math import isfinite


def require_finite(value: object, name: str) -> None:
    # bool is an int subclass, but is not numeric evaluation evidence.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a finite number")
    try:
        finite = isfinite(value)
    except OverflowError:
        finite = False
    if not finite:
        raise ValueError(f"{name} must be a finite number")


def require_count(value: object, name: str, *, minimum: int = 0) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
