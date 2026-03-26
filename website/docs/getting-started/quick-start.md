---
sidebar_position: 2
title: Quick Start
---

# Quick Start

This guide walks you through profiling a schema, building a Genie Space, and benchmarking its accuracy — all in a few lines of code.

## 1. Profile Your Schema

```python
from genie_world.profiler import profile_schema

WAREHOUSE = "your-warehouse-id"

profile = profile_schema(
    "my_catalog", "my_schema",
    deep=True, synonyms=True, enrich_descriptions=True,
    warehouse_id=WAREHOUSE,
)
```

The profiler scans your Unity Catalog tables and produces a rich schema profile — column types, descriptions, cardinality, relationships between tables, and business-friendly synonyms.

## 2. Build and Deploy a Genie Space

```python
from genie_world.builder import build_space, create_space

result = build_space(profile, warehouse_id=WAREHOUSE)
space = create_space(result.config, "My Space", WAREHOUSE, "/Workspace/Users/me/")
print(space["space_url"])
```

The builder takes the profile and generates a complete Genie Space config — data sources, SQL instructions, example Q&A pairs, and snippets. It then deploys via the Databricks API.

## 3. Benchmark Accuracy

```python
from genie_world.benchmarks import run_benchmarks

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
from genie_world.benchmarks import tune_space

result = tune_space(
    space["space_id"], WAREHOUSE,
    target_accuracy=0.9, auto_approve=True,
)
print(f"Final: {result.final_accuracy:.0%} in {len(result.iterations)} iterations")
```
