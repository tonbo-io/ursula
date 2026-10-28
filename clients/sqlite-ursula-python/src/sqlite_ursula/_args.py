"""The checks of the arguments the package takes, shared by ``open`` and the connection's calls: a
value it cannot take is ``invalid_argument`` (``fix``), never a ``TypeError`` from deeper down."""

from __future__ import annotations

import math

from .errors import UrsulaSetupError


def invalid(message: str) -> UrsulaSetupError:
    return UrsulaSetupError(f"invalid_argument: {message}", "invalid_argument", "fix")


def seconds(name: str, value: object, *, positive: bool) -> float:
    """``value``, a number of seconds: positive, or zero or more."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or math.isnan(value):
        raise invalid(f"{name} must be a number of seconds, not {value!r}")
    if value < 0 or (positive and value == 0):
        raise invalid(f"{name} must be {'positive' if positive else 'zero or more'}, not {value!r}")
    return float(value)


def flag(name: str, value: object) -> bool:
    if not isinstance(value, bool):
        raise invalid(f"{name} must be a boolean, not {value!r}")
    return value
