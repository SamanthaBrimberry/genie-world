---
sidebar_position: 2
title: Quick Start
---

# Quick Start

This guide walks you through profiling a schema, building a Genie Space, and benchmarking its accuracy — all in a few lines of code.

## Setup

All key functions are available as top-level imports:

```python
from genie_world import (
    profile_schema, build_space, create_space,
    run_benchmarks, tune_space,
)

WAREHOUSE = "your-warehouse-id"
```

## 1. Profile Your Schema

```python
profile = profile_schema(
    "my_catalog", "my_schema",
    deep=True, synonyms=True, enrich_descriptions=True,
    warehouse_id=WAREHOUSE,
)
```

The profiler scans your Unity Catalog tables and produces a rich schema profile — column types, descriptions, cardinality, relationships between tables, and business-friendly synonyms.

## 2. Build and Deploy a Genie Space

```python
result = build_space(profile, warehouse_id=WAREHOUSE)
space = create_space(result.config, "My Space", WAREHOUSE, "/Workspace/Users/me/")
print(space["space_url"])
```

The builder takes the profile and generates a complete Genie Space config — data sources, SQL instructions, example Q&A pairs, and snippets. It then deploys via the Databricks API.

## 3. Benchmark Accuracy

```python
results = run_benchmarks(space["space_id"], WAREHOUSE)
print(f"Accuracy: {results.accuracy:.0%}")
```

## 4. Improve — Diagnose, Suggest, Update

```python
from genie_world.benchmarks import diagnose_failures, generate_suggestions, update_space

diagnoses = diagnose_failures(results)
suggestions = generate_suggestions(diagnoses, results, WAREHOUSE)
update_space(space["space_id"], suggestions, WAREHOUSE)
```

## Auto-Tune

Or run the full improvement loop in one call:

```python
result = tune_space(
    space["space_id"], WAREHOUSE,
    target_accuracy=0.9, auto_approve=True,
)
print(f"Final: {result.final_accuracy:.0%} in {len(result.iterations)} iterations")
```

## 5. Manage Your Space

After deployment, manage the space lifecycle:

```python
from genie_world.builder import get_space, list_spaces, update_space, delete_space

# List spaces in a directory
spaces = list_spaces("/Workspace/Users/me/")

# Inspect a space and its config
details = get_space(space["space_id"])

# Update config or metadata
update_space(space["space_id"], result.config, display_name="Renamed")

# Clean up
delete_space(space["space_id"])
```

:::warning Name collision: `update_space`
Both `benchmarks` and `builder` export a function called `update_space`, but they do different things:

- **`genie_world.benchmarks.update_space()`** — applies benchmark-generated suggestions (instruction edits, new snippets) to improve accuracy.
- **`genie_world.builder.update_space()`** — updates config or metadata (display name, data sources) via the Databricks API.

Always import from the specific module to avoid confusion.
:::
