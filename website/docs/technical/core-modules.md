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

Wrapper around the Databricks Genie Conversation API with full state visibility. Used by the benchmarks runner to send questions and retrieve results. Includes automatic retry with exponential backoff for transient errors (429 rate limits, 503 service unavailable).

### `retry`

Retry utilities for transient API errors. Provides:

- **`is_retryable(error)`** — checks if an exception matches transient patterns (429, 503, timeout)
- **`@retry_with_backoff`** — decorator with configurable max retries, base delay, max delay cap, and custom retryable check

Used internally by `GenieClient` for resilient polling. Can also be applied to your own functions:

```python
from genie_world.core.retry import retry_with_backoff

@retry_with_backoff(max_retries=3, base_delay=2.0)
def call_external_api():
    ...
```

### `tracing`

MLflow tracing integration. Adds per-state spans to `GenieClient` operations for observability.

### `storage`

Utilities for reading/writing intermediate results (profiles, configs, benchmark results). Requires the `storage_path` config field (or `GENIE_WORLD_STORAGE_PATH` env var) to be set for `LocalStorage`. Also provides standalone `save_artifact()` and `load_artifact()` helpers that accept an explicit path.

### `models`

Shared Pydantic models used across all blocks. Note that individual blocks also have their own `models.py` (e.g. `profiler/models.py`, `benchmarks/models.py`) for block-specific types.
