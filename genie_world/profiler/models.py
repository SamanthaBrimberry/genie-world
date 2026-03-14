"""Pydantic models for the profiler block."""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class DetectionMethod(str, Enum):
    """How a relationship between tables was detected."""

    UC_CONSTRAINT = "uc_constraint"
    LINEAGE = "lineage"
    QUERY_COOCCURRENCE = "query_cooccurrence"
    NAMING_PATTERN = "naming_pattern"
    VALUE_OVERLAP = "value_overlap"


class ColumnProfile(BaseModel):
    """Profile information for a single table column."""

    name: str
    data_type: str
    nullable: bool
    description: str | None = None
    cardinality: int | None = None
    null_percent: float | None = None
    top_values: list[str] | None = None
    min_value: str | None = None
    max_value: str | None = None
    sample_values: list[str] | None = None
    synonyms: list[str] | None = None
    tags: dict[str, str] | None = None
    query_frequency: int | None = None
    co_queried_columns: list[str] | None = None


class TableProfile(BaseModel):
    """Profile information for a single table."""

    catalog: str
    schema_name: str
    table: str
    description: str | None = None
    row_count: int | None = None
    columns: list[ColumnProfile] = []
    query_frequency: int | None = None
    upstream_tables: list[str] | None = None
    downstream_tables: list[str] | None = None


class Relationship(BaseModel):
    """An inferred or known relationship between two table columns."""

    source_table: str
    source_column: str
    target_table: str
    target_column: str
    confidence: float
    detection_method: DetectionMethod


class ProfilingWarning(BaseModel):
    """A non-fatal warning emitted during profiling."""

    table: str
    tier: str
    message: str


class SchemaProfile(BaseModel):
    """Complete profiling result for a Databricks schema."""

    schema_version: str
    catalog: str
    schema_name: str
    tables: list[TableProfile]
    relationships: list[Relationship]
    warnings: list[ProfilingWarning] | None = None
    profiled_at: datetime
