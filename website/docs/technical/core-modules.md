---
sidebar_position: 2
title: Core Modules
---

# Core Modules

The `genie_world.core` package provides shared infrastructure used by all blocks.

## Modules

### `auth`

Handles Databricks authentication via the SDK. Works with CLI profiles, environment variables, and notebook context.

### `config`

Global configuration via `GenieWorldConfig`:

```python
from genie_world.core.config import GenieWorldConfig, set_config, get_config

set_config(GenieWorldConfig(
    warehouse_id="your-warehouse-id",
    llm_model="databricks-claude-sonnet-4-6",
))

config = get_config()
```

Supports environment variable overrides with the `GENIE_WORLD_` prefix.

### `sql`

SQL execution helpers that run queries against a Databricks SQL warehouse and return structured results.

### `llm`

LLM call utilities using the Databricks serving endpoint API. Uses raw `api_client.do("POST", ...)` to avoid SDK serialization issues.

### `genie_client`

Wrapper around the Databricks Genie Conversation API with full state visibility. Used by the benchmarks runner to send questions and retrieve results.

### `tracing`

MLflow tracing integration. Adds per-state spans to `GenieClient` operations for observability.

### `storage`

Utilities for reading/writing intermediate results (profiles, configs, benchmark results).

### `models`

Shared Pydantic models used across all blocks.
