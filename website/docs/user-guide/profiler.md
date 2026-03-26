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
| **Usage** (`deep=True`) | Query patterns from system tables | Yes |
| **Relationships** | Shared column names + naming convention detection | No (enhanced with data) |
| **Synonyms** (`synonyms=True`) | Business-friendly aliases via LLM | Yes (LLM endpoint) |
| **Description Enrichment** (`enrich_descriptions=True`) | Improved column/table descriptions via LLM | Yes (LLM endpoint) |

## Basic Usage

```python
from genie_world.profiler import profile_schema

# Metadata only — no warehouse needed
profile = profile_schema("my_catalog", "my_schema")

# Full profiling with all tiers
profile = profile_schema(
    "my_catalog", "my_schema",
    deep=True,
    synonyms=True,
    enrich_descriptions=True,
    warehouse_id="your-warehouse-id",
)
```

## Profile Output

The returned `SchemaProfile` contains:

- **tables** — list of `TableProfile` with columns, types, descriptions
- **relationships** — detected foreign key and shared-column relationships
- **synonyms** — business-friendly column name mappings
- **statistics** — cardinality, null rates, sample values per column

## Modules

| Module | Purpose |
|--------|---------|
| `metadata_profiler` | Reads Unity Catalog metadata |
| `data_profiler` | Runs SQL queries for column statistics |
| `usage_profiler` | Mines system tables for query patterns |
| `relationship_detector` | Finds relationships via naming conventions and shared columns |
| `synonym_generator` | Generates business-friendly aliases with LLM |
| `description_enricher` | Improves table/column descriptions with LLM |
