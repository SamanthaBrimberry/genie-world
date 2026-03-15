"""Profiler package for genie-world: tiered data profiling for Databricks schemas."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from genie_world.profiler import data_profiler, metadata_profiler, usage_profiler
from genie_world.profiler.models import (
    ProfilingWarning,
    Relationship,
    SchemaProfile,
    TableProfile,
)
from genie_world.profiler.relationship_detector import (
    detect_by_naming_patterns,
    merge_relationships,
)
from genie_world.profiler.synonym_generator import generate_synonyms_for_table

logger = logging.getLogger(__name__)


def profile_schema(
    catalog: str,
    schema: str,
    *,
    deep: bool = False,
    usage: bool = False,
    synonyms: bool = False,
    warehouse_id: str | None = None,
    max_workers: int = 4,
    progress_callback=None,
) -> SchemaProfile:
    """Profile all tables in a Databricks schema.

    Orchestrates the full profiling pipeline:
      1. Tier 1 (always): metadata from Unity Catalog
      2. Tier 2 (if deep + warehouse_id): SQL statistics via ThreadPoolExecutor
      3. Tier 3 (if usage + warehouse_id): query usage from system tables
      4. Relationships: naming-pattern detection + declared FK constraints (if usage)
      5. Synonyms (if synonyms): LLM-generated column synonyms

    Args:
        catalog: Databricks catalog name.
        schema: Schema (database) name.
        deep: Whether to run SQL-based Tier 2 statistical profiling.
        usage: Whether to run Tier 3 usage profiling and declared relationship detection.
        synonyms: Whether to generate LLM-based column synonyms.
        warehouse_id: SQL warehouse ID required for Tier 2 and Tier 3.
        max_workers: Thread pool size for parallel Tier 2 profiling.
        progress_callback: Optional callable(table_name: str) invoked after each table is processed.

    Returns:
        A :class:`SchemaProfile` containing all profiled tables and inferred relationships.
    """
    all_warnings: list[ProfilingWarning] = []

    # --- Tier 1: metadata ---
    tables, meta_warnings = metadata_profiler.profile_schema_metadata(
        catalog, schema, return_warnings=True
    )
    all_warnings.extend(meta_warnings)

    # --- Tier 2: data stats (parallel) ---
    if deep and warehouse_id:
        enriched: list[TableProfile] = []

        def _enrich(tbl: TableProfile) -> tuple[TableProfile, list[ProfilingWarning]]:
            return data_profiler.enrich_table_with_stats(tbl, warehouse_id)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_enrich, tbl): tbl for tbl in tables}
            for future in as_completed(futures):
                tbl_result, tbl_warnings = future.result()
                enriched.append(tbl_result)
                all_warnings.extend(tbl_warnings)
                if progress_callback is not None:
                    progress_callback(tbl_result.table)

        tables = enriched
    elif progress_callback is not None:
        for tbl in tables:
            progress_callback(tbl.table)

    # --- Tier 3: usage ---
    if usage and warehouse_id:
        tables, usage_warnings = usage_profiler.enrich_with_usage(
            tables, catalog, schema, warehouse_id=warehouse_id
        )
        all_warnings.extend(usage_warnings)

    # --- Relationships ---
    named_rels: list[Relationship] = detect_by_naming_patterns(tables)
    declared_rels: list[Relationship] = []

    if usage and warehouse_id:
        declared_rels, rel_warnings = usage_profiler.get_declared_relationships(
            catalog, schema, warehouse_id=warehouse_id
        )
        all_warnings.extend(rel_warnings)

    relationships = merge_relationships(named_rels, declared_rels)

    # --- Synonyms ---
    if synonyms:
        synonym_tables: list[TableProfile] = []
        for tbl in tables:
            enriched_tbl, syn_warnings = generate_synonyms_for_table(tbl)
            synonym_tables.append(enriched_tbl)
            all_warnings.extend(syn_warnings)
        tables = synonym_tables

    return SchemaProfile(
        schema_version="1.0",
        catalog=catalog,
        schema_name=schema,
        tables=tables,
        relationships=relationships,
        warnings=all_warnings if all_warnings else None,
        profiled_at=datetime.now(tz=timezone.utc),
    )


def profile_tables(
    tables: list[str],
    *,
    deep: bool = False,
    usage: bool = False,
    synonyms: bool = False,
    warehouse_id: str | None = None,
    max_workers: int = 4,
    progress_callback=None,
) -> SchemaProfile:
    """Profile a specific list of tables within a single catalog.schema.

    Each table must be specified in "catalog.schema.table" format, and all
    tables must belong to the same catalog.schema.

    Args:
        tables: List of fully-qualified table names ("catalog.schema.table").
        deep: Whether to run SQL-based Tier 2 statistical profiling.
        usage: Whether to run Tier 3 usage profiling and declared relationship detection.
        synonyms: Whether to generate LLM-based column synonyms.
        warehouse_id: SQL warehouse ID required for Tier 2 and Tier 3.
        max_workers: Thread pool size for parallel Tier 2 profiling.
        progress_callback: Optional callable(table_name: str) invoked after each table is processed.

    Returns:
        A :class:`SchemaProfile` containing all profiled tables and inferred relationships.

    Raises:
        ValueError: If table names are not in "catalog.schema.table" format, or if tables
            span more than one catalog.schema.
    """
    if not tables:
        raise ValueError("tables list must not be empty")

    parsed: list[tuple[str, str, str]] = []
    for full_name in tables:
        parts = full_name.split(".")
        if len(parts) != 3:
            raise ValueError(
                f"Table name must be in 'catalog.schema.table' format, got: {full_name!r}"
            )
        parsed.append((parts[0], parts[1], parts[2]))

    catalog = parsed[0][0]
    schema = parsed[0][1]

    for cat, sch, _ in parsed:
        if cat != catalog or sch != schema:
            raise ValueError(
                f"All tables must be in the same catalog.schema. "
                f"Found {cat}.{sch} and {catalog}.{schema}"
            )

    all_warnings: list[ProfilingWarning] = []

    # --- Tier 1: metadata for specific tables ---
    table_profiles: list[TableProfile] = []
    for cat, sch, tbl_name in parsed:
        try:
            profile = metadata_profiler.profile_table_metadata(cat, sch, tbl_name)
            table_profiles.append(profile)
        except Exception as exc:
            full = f"{cat}.{sch}.{tbl_name}"
            logger.warning("Failed to profile table %s: %s", full, exc)
            all_warnings.append(
                ProfilingWarning(table=full, tier="metadata", message=str(exc))
            )

    # --- Tier 2: data stats (parallel) ---
    if deep and warehouse_id:
        enriched: list[TableProfile] = []

        def _enrich(tbl: TableProfile) -> tuple[TableProfile, list[ProfilingWarning]]:
            return data_profiler.enrich_table_with_stats(tbl, warehouse_id)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_enrich, tbl): tbl for tbl in table_profiles}
            for future in as_completed(futures):
                tbl_result, tbl_warnings = future.result()
                enriched.append(tbl_result)
                all_warnings.extend(tbl_warnings)
                if progress_callback is not None:
                    progress_callback(tbl_result.table)

        table_profiles = enriched
    elif progress_callback is not None:
        for tbl in table_profiles:
            progress_callback(tbl.table)

    # --- Tier 3: usage ---
    if usage and warehouse_id:
        table_profiles, usage_warnings = usage_profiler.enrich_with_usage(
            table_profiles, catalog, schema, warehouse_id=warehouse_id
        )
        all_warnings.extend(usage_warnings)

    # --- Relationships ---
    named_rels: list[Relationship] = detect_by_naming_patterns(table_profiles)
    declared_rels: list[Relationship] = []

    if usage and warehouse_id:
        declared_rels, rel_warnings = usage_profiler.get_declared_relationships(
            catalog, schema, warehouse_id=warehouse_id
        )
        all_warnings.extend(rel_warnings)

    relationships = merge_relationships(named_rels, declared_rels)

    # --- Synonyms ---
    if synonyms:
        synonym_tables: list[TableProfile] = []
        for tbl in table_profiles:
            enriched_tbl, syn_warnings = generate_synonyms_for_table(tbl)
            synonym_tables.append(enriched_tbl)
            all_warnings.extend(syn_warnings)
        table_profiles = synonym_tables

    return SchemaProfile(
        schema_version="1.0",
        catalog=catalog,
        schema_name=schema,
        tables=table_profiles,
        relationships=relationships,
        warnings=all_warnings if all_warnings else None,
        profiled_at=datetime.now(tz=timezone.utc),
    )
