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

:::note Join specs
Join specs are generated during the build but are **not deployed** to the Genie Space API
due to a Databricks protobuf serialization bug — `join_specs` is set to `[]` in the
deployed config. The generated joins are stored as `_generated_join_specs` in the config
dict for reference. In practice this is fine because Genie infers joins automatically
from the underlying table metadata.
:::

## Basic Usage

```python
from genie_world import profile_schema, build_space, create_space

profile = profile_schema("catalog", "schema", deep=True, warehouse_id=WAREHOUSE)

# Build the config
result = build_space(profile, warehouse_id=WAREHOUSE)

# Deploy it
space = create_space(result.config, "My Space", WAREHOUSE, "/Workspace/Users/me/")
print(space["space_url"])
```

## `build_space()` Parameters

```python
build_space(
    profile: SchemaProfile,
    *,
    warehouse_id: str | None = None,
    example_count: int = 10,
    benchmark_count: int = 10,
    include_tables: list[str] | None = None,
    exclude_tables: list[str] | None = None,
    sql_functions: list[dict] | None = None,
    metric_views: list[dict] | None = None,
) -> BuildResult
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `profile` | *required* | `SchemaProfile` from the profiler |
| `warehouse_id` | `None` | If provided, generated SQL is validated and fixed against the warehouse |
| `example_count` | `10` | Number of example Q&A pairs to generate |
| `benchmark_count` | `10` | Number of benchmark questions to generate |
| `include_tables` | `None` | If set, only include these tables (by short name) |
| `exclude_tables` | `None` | Table names to explicitly exclude |
| `sql_functions` | `None` | Pass-through Unity Catalog function references |
| `metric_views` | `None` | Pass-through metric views |

## Build Result

`build_space()` returns a `BuildResult` (Pydantic model) with two fields:

- **`config`** (`dict`) — the complete Genie Space config dict, ready for the API
- **`warnings`** (`list[BuilderWarning]`) — list of warnings generated during the build

Each `BuilderWarning` has:

| Field | Type | Description |
|-------|------|-------------|
| `section` | `str` | Pipeline stage that produced the warning (e.g. `"data_sources"`, `"example_sqls"`) |
| `message` | `str` | Human-readable summary |
| `detail` | `str \| None` | Optional additional context |

## Table Exclusion Suggestions

`suggest_table_exclusions()` analyzes a profile and returns a list of tables that may not be useful in a Genie Space (e.g. tables with no columns or very few rows):

```python
from genie_world.builder.data_sources import suggest_table_exclusions

suggestions = suggest_table_exclusions(profile)
for s in suggestions:
    print(f"Consider excluding '{s['table']}': {s['reason']}")
```

Each suggestion is a dict with `table` (name) and `reason` (why). These are recommendations only — nothing is auto-excluded. `build_space()` also surfaces these as warnings when the suggested tables are not already excluded.

## Config Validation

Before deploying, you can validate a config to catch issues early:

```python
from genie_world.builder import validate_config

errors = validate_config(result.config)
if errors:
    for e in errors:
        print(f"  - {e}")
else:
    print("Config is valid")
```

`validate_config()` checks:
- Config is not empty
- Required keys (`data_sources`) are present
- At least one table is included
- Serialized size is within the 3.5 MB API limit
- At most 1 text instruction (API constraint)
- No internal `_`-prefixed fields that would be stripped on deploy

## Space Lifecycle Management

After building a config, you can create and manage spaces programmatically:

```python
from genie_world.builder import create_space, get_space, list_spaces, update_space, delete_space

# Create a new space
space = create_space(
    result.config, "My Space", WAREHOUSE, "/Workspace/Users/me/",
    description="Sales analytics powered by genie-world",
)

# List all spaces in a workspace directory
spaces = list_spaces("/Workspace/Users/me/")
for s in spaces:
    print(f"{s['space_id']}: {s.get('title', 'untitled')}")

# Get a space with its full parsed config
details = get_space("your-space-id")
config = details["serialized_space"]  # parsed dict, not JSON string

# Update a space's config, title, or description
update_space("your-space-id", new_config, display_name="Updated Name", description="New desc")

# Delete a space
delete_space("your-space-id")
```

| Function | Signature | Description |
|----------|-----------|-------------|
| `create_space()` | `(config, display_name, warehouse_id, parent_path, description=None)` | Deploy a new Genie Space from a config dict |
| `get_space()` | `(space_id)` | Fetch a space by ID with its parsed config |
| `list_spaces()` | `(parent_path)` | List all spaces under a workspace path |
| `update_space()` | `(space_id, config, display_name=None, description=None)` | Replace a space's config and optionally update title/description |
| `delete_space()` | `(space_id)` | Delete a space by ID |
| `validate_config()` | `(config)` | Check a config for errors before deploying |

## Modules

| Module | Purpose |
|--------|---------|
| `data_sources` | Table selection with entity matching heuristics |
| `join_specs` | Join condition generation |
| `snippets` | Filter, expression, and measure snippet generation |
| `example_sqls` | Parameterized Q&A pair generation with SQL validation |
| `benchmarks` | Benchmark question generation |
| `instructions` | Natural language SQL instruction generation |
| `sql_validator` | SQL validation with up to 4 total attempts (1 initial + 3 LLM-fix retries) |
| `assembler` | Config assembly with IDs, sorting, and API constraints |
| `deployer` | Space lifecycle: create, get, list, update, delete, validate |
