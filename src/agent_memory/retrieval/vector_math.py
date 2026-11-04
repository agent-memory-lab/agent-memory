"""Stable arithmetic for already validated, finite embedding vectors."""

from collections.abc import Sequence
from math import hypot


def normalize_l2(values: Sequence[float]) -> tuple[float, ...]:
    """Normalize finite values, preserving zeros for the caller's zero policy.

    Scale before measuring the norm: even hypot can overflow when the true norm
    exceeds the largest float, or lose precision for subnormal components.
    """
    scale = max((abs(value) for value in values), default=0.0)
    if scale == 0.0:
        return tuple(values)
    scaled = tuple(value / scale for value in values)
    norm = hypot(*scaled)
    return tuple(value / norm for value in scaled)
