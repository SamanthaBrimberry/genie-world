---
sidebar_position: 1
title: Profiler
---

# Profiler

The Profiler scans your Unity Catalog tables and produces a rich schema profile used by the Builder to generate optimal Genie Space configurations.

## Overview

Profiling works in tiers, each adding more depth:

| Tier | What it does | Requires warehouse? |
|------|-------------|---------------------|
| **Metadata** | Column names, types, descriptions from Unity Catalog | No |
| **Data** (`deep=True`) | Cardinality, sample values, null rates via SQL | Yes |
| **Usage** (`usage=True`) | Query patterns from system tables + FK constraint discovery from `system.information_schema` | Yes |
| **Relationships** | Naming convention detection (`_id`, `_key`, `_fk` suffixes) + shared columns, with confidence scores | No (enhanced with declared FKs when `usage=True`) |
| **Synonyms** (`synonyms=True`) | Business-friendly aliases via LLM (stored on each `ColumnProfile`) | Yes (LLM endpoint) |
| **Description Enrichment** (`enrich_descriptions=True`) | Fills in missing column/table descriptions via LLM (preserves existing ones) | Yes (LLM endpoint) |

## Basic Usage

```python
from genie_world.profiler import profile_schema, profile_tables

# Metadata only — no warehouse needed
profile = profile_schema("my_catalog", "my_schema")

# Full profiling with all tiers
profile = profile_schema(
    "my_catalog", "my_schema",
    deep=True,
    usage=True,
    synonyms=True,
    enrich_descriptions=True,
    warehouse_id="your-warehouse-id",
    max_workers=4,
    progress_callback=lambda table_name: print(f"Done: {table_name}"),
)

# Profile specific tables instead of a whole schema
profile = profile_tables(
    ["my_catalog.my_schema.orders", "my_catalog.my_schema.customers"],
    deep=True,
    warehouse_id="your-warehouse-id",
)
```

### `profile_schema` signature

```python
def profile_schema(
    catalog: str,
    schema: str,
    *,
    deep: bool = False,
    usage: bool = False,
    synonyms: bool = False,
    enrich_descriptions: bool = False,
    warehouse_id: str | None = None,
    max_workers: int = 4,
    progress_callback=None,
) -> SchemaProfile
```

### `profile_tables` signature

```python
def profile_tables(
    tables: list[str],          # fully-qualified "catalog.schema.table" names
    *,
    deep: bool = False,
    usage: bool = False,
    synonyms: bool = False,
    enrich_descriptions: bool = False,
    warehouse_id: str | None = None,
    max_workers: int = 4,
    progress_callback=None,
) -> SchemaProfile
```

All tables must belong to the same `catalog.schema`. Raises `ValueError` if not.

## Profile Output

The returned `SchemaProfile` contains:

- **schema_version** — profile format version (currently `"1.0"`)
- **catalog** — the profiled catalog name
- **schema_name** — the profiled schema name
- **profiled_at** — UTC timestamp of when profiling ran
- **tables** — list of `TableProfile` with columns, types, descriptions, and statistics
- **relationships** — detected foreign key and shared-column relationships with confidence scores
- **warnings** — non-fatal warnings emitted during profiling (or `None`)

Synonyms live on individual columns as `ColumnProfile.synonyms`, not as a top-level field.

## Modules

| Module | Purpose |
|--------|---------|
| `metadata_profiler` | Reads Unity Catalog metadata |
| `data_profiler` | Runs SQL queries for column statistics |
| `usage_profiler` | Mines system tables for query patterns and discovers FK constraints from `system.information_schema` |
| `relationship_detector` | Finds relationships via `_id`, `_key`, and `_fk` suffix conventions and shared columns; assigns confidence scores |
| `synonym_generator` | Generates business-friendly aliases with LLM |
| `description_enricher` | Fills in missing table/column descriptions with LLM (preserves existing descriptions) |
