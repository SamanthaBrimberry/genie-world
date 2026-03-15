"""Space Builder block for genie-world.

Generates complete Genie Space configurations from SchemaProfile data.
"""

from __future__ import annotations

from pydantic import BaseModel


class BuilderWarning(BaseModel):
    """Warning generated during space building."""
    section: str
    message: str
    detail: str | None = None


class BuildResult(BaseModel):
    """Result from build_space() containing config and any warnings."""
    config: dict
    warnings: list[BuilderWarning]
