---
sidebar_position: 3
title: Configuration
---

# Configuration

## Python Config

```python
from genie_world.core.config import GenieWorldConfig, set_config

set_config(GenieWorldConfig(
    warehouse_id="your-warehouse-id",
    llm_model="databricks-claude-sonnet-4-6",
))
```

## Environment Variables

```bash
export GENIE_WORLD_WAREHOUSE_ID=your-warehouse-id
export GENIE_WORLD_LLM_MODEL=databricks-claude-sonnet-4-6
```

## Authentication

Genie World uses the Databricks SDK for authentication. It works with any standard auth method:

- **Databricks CLI profile** — `databricks configure`
- **Environment variables** — `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
- **On Databricks** — automatic via notebook context

See the [Databricks SDK auth docs](https://docs.databricks.com/dev-tools/auth.html) for all supported methods.
