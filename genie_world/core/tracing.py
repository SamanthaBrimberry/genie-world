"""MLflow trace decorator with graceful fallback."""

from __future__ import annotations

from typing import Callable, TypeVar

F = TypeVar("F", bound=Callable)


def trace(
    func: F | None = None,
    *,
    name: str | None = None,
    span_type: str | None = None,
) -> F:
    def decorator(f: F) -> F:
        try:
            import mlflow
            return mlflow.trace(f, name=name, span_type=span_type)
        except ImportError:
            return f

    if func is None:
        return decorator
    else:
        return decorator(func)
