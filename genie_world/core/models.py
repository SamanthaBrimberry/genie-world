"""Shared Pydantic models that flow between blocks.

SpaceConfig is stubbed here for type contracts. Full implementation
will be fleshed out when the Builder block is implemented.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class SpaceConfig(BaseModel):
    """Matches the Genie Space serialized_space JSON schema.

    Stubbed with flexible types. Will be fully typed when
    the Builder block is implemented.
    """

    display_name: str
    data_sources: dict[str, Any] | None = None
    instructions: dict[str, Any] | None = None
    benchmarks: dict[str, Any] | None = None
