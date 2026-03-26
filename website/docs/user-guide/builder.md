---
sidebar_position: 2
title: Builder
---

# Builder

The Builder takes a profiler output and generates a complete, deployable Genie Space configuration.

## Overview

`build_space()` orchestrates the full pipeline:

1. **Data sources** — selects tables, detects entity matching heuristics
2. **Join specs** — generates join conditions between related tables
3. **Snippets** — creates filter, expression, and measure snippets
4. **Example SQLs** — generates parameterized Q&A pairs, validated against the warehouse
5. **Benchmark questions** — creates evaluation questions for accuracy testing
6. **Instructions** — generates natural language SQL instructions (runs last, informed by all above)
7. **Assembly** — combines everything into a valid API config with IDs, sorting, and constraints
8. **Deployment** — deploys via the Databricks Genie Space API

## Basic Usage

```python
from genie_world.profiler import profile_schema
from genie_world.builder import build_space, create_space

profile = profile_schema("catalog", "schema", deep=True, warehouse_id=WAREHOUSE)

# Build the config
result = build_space(profile, warehouse_id=WAREHOUSE)

# Deploy it
space = create_space(result.config, "My Space", WAREHOUSE, "/Workspace/Users/me/")
print(space["space_url"])
```

## Build Result

`build_space()` returns a `BuildResult` with:

- **config** — the complete Genie Space config dict, ready for the API
- **data_sources** — selected tables with entity matching metadata
- **join_specs** — detected join conditions
- **snippets** — filter, expression, and measure snippets
- **example_sqls** — validated Q&A pairs
- **instructions** — generated SQL instructions
- **benchmark_questions** — questions for accuracy evaluation

## Modules

| Module | Purpose |
|--------|---------|
| `data_sources` | Table selection with entity matching heuristics |
| `join_specs` | Join condition generation |
| `snippets` | Filter, expression, and measure snippet generation |
| `example_sqls` | Parameterized Q&A pair generation with SQL validation |
| `benchmarks` | Benchmark question generation |
| `instructions` | Natural language SQL instruction generation |
| `sql_validator` | SQL validation with 3x retry |
| `assembler` | Config assembly with IDs, sorting, and API constraints |
| `deployer` | Genie Space API deployment (PATCH strips internal fields) |
