"""Tier 3 profiler: system tables mining for usage and relationships."""

from __future__ import annotations

import logging
import re

from genie_world.core.sql import execute_sql
from genie_world.core.tracing import trace
from genie_world.profiler.models import (
    DetectionMethod,
    ProfilingWarning,
    Relationship,
    TableProfile,
)

logger = logging.getLogger(__name__)

_VALID_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_]+$")


def _validate_identifier(name: str, label: str) -> str:
    """Validate that a SQL identifier contains only safe characters.

    Args:
        name: The identifier string to validate.
        label: Human-readable label for error messages (e.g. "catalog").

    Returns:
        The original name if valid.

    Raises:
        ValueError: If the name contains disallowed characters.
    """
    if not _VALID_IDENTIFIER.match(name):
        raise ValueError(
            f"Invalid {label}: {name!r}. Must be alphanumeric/underscores only."
        )
    return name


@trace
def get_declared_relationships(
    catalog: str,
    schema: str,
    *,
    warehouse_id: str,
) -> tuple[list[Relationship], list[ProfilingWarning]]:
    """Query Unity Catalog system tables for declared FK relationships.

    Queries ``system.information_schema.table_constraints`` and
    ``key_column_usage`` to find foreign-key constraints defined in UC.

    Args:
        catalog: Databricks catalog name.
        schema: Schema (database) name.
        warehouse_id: The Databricks SQL warehouse ID to run queries on.

    Returns:
        A tuple of (relationships, warnings). On permission error returns
        an empty relationship list with a warning.
    """
    _validate_identifier(catalog, "catalog")
    _validate_identifier(schema, "schema")

    sql = f"""
SELECT
  CONCAT(fk.table_catalog, '.', fk.table_schema, '.', fk.table_name) AS fk_table,
  fk.column_name AS fk_column,
  CONCAT(pk.table_catalog, '.', pk.table_schema, '.', pk.table_name) AS pk_table,
  pk.column_name AS pk_column
FROM system.information_schema.referential_constraints rc
JOIN system.information_schema.key_column_usage fk
  ON fk.constraint_catalog = rc.constraint_catalog
  AND fk.constraint_schema = rc.constraint_schema
  AND fk.constraint_name = rc.constraint_name
JOIN system.information_schema.key_column_usage pk
  ON pk.constraint_catalog = rc.unique_constraint_catalog
  AND pk.constraint_schema = rc.unique_constraint_schema
  AND pk.constraint_name = rc.unique_constraint_name
  AND pk.ordinal_position = fk.ordinal_position
WHERE fk.table_catalog = '{catalog}'
  AND fk.table_schema = '{schema}'
""".strip()

    result = execute_sql(sql, warehouse_id=warehouse_id)

    if result.get("error"):
        warning_table = f"{catalog}.{schema}"
        return [], [
            ProfilingWarning(
                table=warning_table,
                tier="usage",
                message=f"Could not query FK constraints: {result['error']}",
            )
        ]

    relationships: list[Relationship] = []
    for row in result.get("data", []):
        fk_table, fk_column, pk_table, pk_column = row
        relationships.append(
            Relationship(
                source_table=fk_table,
                source_column=fk_column,
                target_table=pk_table,
                target_column=pk_column,
                confidence=1.0,
                detection_method=DetectionMethod.UC_CONSTRAINT,
            )
        )

    return relationships, []


@trace
def enrich_with_usage(
    tables: list[TableProfile],
    catalog: str,
    schema: str,
    *,
    warehouse_id: str,
) -> tuple[list[TableProfile], list[ProfilingWarning]]:
    """Enrich tables with query frequency from system.query.history.

    Queries the last 30 days of query history to count how many times
    each table was referenced. Enriches :attr:`TableProfile.query_frequency`.

    Args:
        tables: The list of TableProfile objects to enrich.
        catalog: Databricks catalog name (used for filtering).
        schema: Schema (database) name (used for filtering).
        warehouse_id: The Databricks SQL warehouse ID to run queries on.

    Returns:
        A tuple of (enriched tables, warnings). On error the original tables
        are returned with a warning.
    """
    _validate_identifier(catalog, "catalog")
    _validate_identifier(schema, "schema")

    if not tables:
        return tables, []

    sql = f"""
SELECT
  table_name,
  COUNT(*) AS query_count
FROM (
  SELECT
    EXPLODE(table_list.table_full_name) AS table_name
  FROM (
    SELECT
      FLATTEN(COLLECT_LIST(read_columns.table)) AS table_list
    FROM system.query.history
    LATERAL VIEW EXPLODE(read_columns) AS read_columns
    WHERE
      execution_status = 'FINISHED'
      AND start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
      AND read_columns.catalog = '{catalog}'
      AND read_columns.schema = '{schema}'
  )
)
GROUP BY table_name
""".strip()

    result = execute_sql(sql, warehouse_id=warehouse_id)

    if result.get("error"):
        warning_table = f"{catalog}.{schema}"
        return tables, [
            ProfilingWarning(
                table=warning_table,
                tier="usage",
                message=f"Could not query usage history: {result['error']}",
            )
        ]

    # Build frequency map: full_table_name -> count
    freq_map: dict[str, int] = {}
    for row in result.get("data", []):
        table_name, query_count = row
        freq_map[table_name] = int(query_count)

    enriched: list[TableProfile] = []
    for table in tables:
        full_name = f"{table.catalog}.{table.schema_name}.{table.table}"
        freq = freq_map.get(full_name)
        if freq is not None:
            enriched.append(table.model_copy(update={"query_frequency": freq}))
        else:
            enriched.append(table)

    return enriched, []
