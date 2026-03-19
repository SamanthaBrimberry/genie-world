"""Tier 2 profiler: SQL-based statistical profiling."""

from __future__ import annotations

import logging
import re

from genie_world.core.sql import execute_sql
from genie_world.core.tracing import trace
from genie_world.profiler.models import ColumnProfile, ProfilingWarning, TableProfile

logger = logging.getLogger(__name__)

# Types that support meaningful MIN/MAX comparisons
_NUMERIC_TYPES = {"INT", "LONG", "SHORT", "BYTE", "FLOAT", "DOUBLE", "DECIMAL", "BIGINT", "INTEGER", "SMALLINT", "TINYINT"}
_DATE_TYPES = {"DATE", "TIMESTAMP", "TIMESTAMP_NTZ"}


def _supports_min_max(data_type: str) -> bool:
    """Return True if the column type supports MIN/MAX profiling."""
    upper = data_type.upper().split("(")[0].strip()
    return upper in _NUMERIC_TYPES or upper in _DATE_TYPES


def _build_profile_sql(full_table_name: str, columns: list[ColumnProfile]) -> str:
    """Build a single SQL query that computes stats for all columns.

    Generates:
      - COUNT(*) for total row count
      - COUNT(DISTINCT col) for cardinality
      - SUM(CASE WHEN col IS NULL THEN 1 ELSE 0 END) for null count
      - MIN/MAX for numeric and date columns

    Args:
        full_table_name: Fully-qualified table name (catalog.schema.table).
        columns: List of column profiles to build stats for.

    Returns:
        A SELECT SQL string.
    """
    parts = ["COUNT(*) AS total_count"]

    for col in columns:
        safe = f"`{col.name}`"
        col_key = re.sub(r"[^a-zA-Z0-9_]", "_", col.name)

        parts.append(f"COUNT(DISTINCT {safe}) AS `{col_key}__distinct`")
        parts.append(
            f"SUM(CASE WHEN {safe} IS NULL THEN 1 ELSE 0 END) AS `{col_key}__null_sum`"
        )

        if _supports_min_max(col.data_type):
            parts.append(f"MIN({safe}) AS `{col_key}__min`")
            parts.append(f"MAX({safe}) AS `{col_key}__max`")

    select_clause = ",\n  ".join(parts)
    # Backtick-quote each part of the table name for safety
    parts_quoted = ".".join(f"`{p}`" for p in full_table_name.split("."))
    return f"SELECT\n  {select_clause}\nFROM {parts_quoted}"


@trace
def enrich_table_with_stats(
    table: TableProfile,
    warehouse_id: str,
) -> tuple[TableProfile, list[ProfilingWarning]]:
    """Enrich a TableProfile with SQL-derived statistics.

    Runs a single profiling query against the table and populates:
      - cardinality (COUNT DISTINCT)
      - null_percent
      - min_value / max_value (for numeric/date columns)

    Args:
        table: The TableProfile to enrich (must have columns populated).
        warehouse_id: The Databricks SQL warehouse ID to run queries on.

    Returns:
        A tuple of (enriched TableProfile, list of ProfilingWarning).
        On SQL error the original table is returned with a warning.
    """
    warnings: list[ProfilingWarning] = []
    full_name = f"{table.catalog}.{table.schema_name}.{table.table}"

    if not table.columns:
        return table, warnings

    sql = _build_profile_sql(full_name, table.columns)
    result = execute_sql(sql, warehouse_id=warehouse_id)

    if result.get("error"):
        warnings.append(
            ProfilingWarning(
                table=full_name,
                tier="data",
                message=f"Profile SQL failed: {result['error']}",
            )
        )
        return table, warnings

    if not result.get("data"):
        warnings.append(
            ProfilingWarning(
                table=full_name,
                tier="data",
                message="Profile SQL returned no rows",
            )
        )
        return table, warnings

    # Parse the single result row into a named dict
    col_names = [c["name"] for c in result["columns"]]
    row = result["data"][0]
    row_dict: dict[str, str | None] = dict(zip(col_names, row))

    total_count_raw = row_dict.get("total_count")
    total_count = int(total_count_raw) if total_count_raw is not None else 0

    enriched_columns: list[ColumnProfile] = []
    for col in table.columns:
        col_key = re.sub(r"[^a-zA-Z0-9_]", "_", col.name)

        distinct_raw = row_dict.get(f"{col_key}__distinct")
        null_sum_raw = row_dict.get(f"{col_key}__null_sum")
        min_raw = row_dict.get(f"{col_key}__min")
        max_raw = row_dict.get(f"{col_key}__max")

        cardinality = int(distinct_raw) if distinct_raw is not None else None
        null_percent: float | None = None
        if null_sum_raw is not None and total_count > 0:
            null_percent = (int(null_sum_raw) / total_count) * 100.0

        has_min_max = _supports_min_max(col.data_type)

        enriched_columns.append(
            col.model_copy(
                update={
                    "cardinality": cardinality,
                    "null_percent": null_percent,
                    "min_value": str(min_raw) if has_min_max and min_raw is not None else None,
                    "max_value": str(max_raw) if has_min_max and max_raw is not None else None,
                }
            )
        )

    return table.model_copy(update={"columns": enriched_columns}), warnings
