"""Tier 1 profiler: extracts table and column metadata from Unity Catalog."""

from __future__ import annotations

import logging

from genie_world.core.auth import get_workspace_client
from genie_world.core.tracing import trace
from genie_world.profiler.models import ColumnProfile, ProfilingWarning, TableProfile

logger = logging.getLogger(__name__)


@trace
def profile_table_metadata(catalog: str, schema: str, table: str) -> TableProfile:
    """Fetch metadata for a single table via the Unity Catalog Tables API.

    Args:
        catalog: Databricks catalog name.
        schema: Schema (database) name.
        table: Table name.

    Returns:
        A :class:`TableProfile` populated with UC metadata.
    """
    client = get_workspace_client()
    full_name = f"{catalog}.{schema}.{table}"
    tbl = client.tables.get(full_name)

    columns: list[ColumnProfile] = []
    for col in tbl.columns or []:
        columns.append(
            ColumnProfile(
                name=col.name,
                data_type=col.type_text,
                nullable=bool(col.nullable),
                description=col.comment or None,
            )
        )

    return TableProfile(
        catalog=catalog,
        schema_name=schema,
        table=table,
        description=tbl.comment or None,
        columns=columns,
    )


@trace
def profile_schema_metadata(
    catalog: str,
    schema: str,
    *,
    return_warnings: bool = False,
) -> list[TableProfile] | tuple[list[TableProfile], list[ProfilingWarning]]:
    """Profile all tables in a schema using Unity Catalog metadata.

    Args:
        catalog: Databricks catalog name.
        schema: Schema (database) name.
        return_warnings: When True, return a ``(tables, warnings)`` tuple instead
            of just the list of :class:`TableProfile` objects.

    Returns:
        A list of :class:`TableProfile` objects, or a tuple of (profiles, warnings)
        when *return_warnings* is ``True``.
    """
    client = get_workspace_client()
    table_stubs = list(client.tables.list(catalog_name=catalog, schema_name=schema))

    tables: list[TableProfile] = []
    warnings: list[ProfilingWarning] = []

    for stub in table_stubs:
        table_name = stub.name
        try:
            profile = profile_table_metadata(catalog, schema, table_name)
            tables.append(profile)
        except Exception as exc:
            full_name = f"{catalog}.{schema}.{table_name}"
            logger.warning("Failed to profile table %s: %s", full_name, exc)
            warnings.append(
                ProfilingWarning(
                    table=full_name,
                    tier="metadata",
                    message=str(exc),
                )
            )

    if return_warnings:
        return tables, warnings
    return tables
