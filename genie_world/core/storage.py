"""Artifact persistence for genie-world.

Supports local filesystem storage. UC Volumes support can be added later
by implementing the same interface.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class LocalStorage:
    """Store artifacts as JSON files on the local filesystem."""

    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def save(self, name: str, artifact: BaseModel) -> str:
        """Save a Pydantic model as JSON. Returns the full path."""
        path = self.base_path / name
        path.write_text(artifact.model_dump_json(indent=2))
        logger.info(f"Saved artifact to {path}")
        return str(path)

    def load(self, name: str, model_class: type[T]) -> T | None:
        """Load a Pydantic model from JSON. Returns None if not found."""
        path = self.base_path / name
        if not path.exists():
            return None
        data = path.read_text()
        return model_class.model_validate_json(data)

    def list_artifacts(self) -> list[str]:
        """List all artifact filenames in the storage directory."""
        return sorted(f.name for f in self.base_path.iterdir() if f.is_file())


def save_artifact(artifact: BaseModel, path: str) -> None:
    """Save a Pydantic model to a JSON file at the given path."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(artifact.model_dump_json(indent=2))


def load_artifact(path: str, model_class: type[T]) -> T | None:
    """Load a Pydantic model from a JSON file. Returns None if not found."""
    p = Path(path)
    if not p.exists():
        return None
    return model_class.model_validate_json(p.read_text())
