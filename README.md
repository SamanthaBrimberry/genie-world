# Genie World

Modular Python library for profiling data, building Databricks Genie Spaces programmatically, benchmarking their accuracy, and optimizing their configuration over time.

## Installation

```bash
pip install -e "."

# With optional extras
pip install -e ".[profiler]"   # Data profiling dependencies
pip install -e ".[tracing]"    # MLflow tracing support
pip install -e ".[dev]"        # Development/testing tools
pip install -e ".[all]"        # Everything
```

Requires Python 3.10+ and a Databricks workspace.

## Quick Start

```python
from genie_world.profiler import profile_schema

# Metadata only — no warehouse or LLM needed
profile = profile_schema("my_catalog", "my_schema")

# Full profiling — statistics, usage signals, synonyms
profile = profile_schema(
    "my_catalog", "my_schema",
    deep=True,              # Tier 2: cardinality, null %, min/max via SQL
    usage=True,             # Tier 3: query frequency, PK/FK from system tables
    synonyms=True,          # LLM-generated business-friendly column synonyms
    warehouse_id="abc123",
)

# Profile specific tables
profile = profile_tables(
    tables=["my_catalog.my_schema.orders", "my_catalog.my_schema.customers"],
    deep=True,
    warehouse_id="abc123",
)
```

## Building Blocks

Genie World is designed as composable building blocks. Each block is independently importable and useful on its own.

| Block | Status | Description |
|-------|--------|-------------|
| **Core** | Done | Auth, config, SQL execution, LLM (FMAPI), storage, tracing |
| **Profiler** | Done | Table/column metadata, statistics, PK/FK detection, synonyms |
| **Builder** | Planned | Generate Genie Space configs from profiles |
| **Benchmarks** | Planned | Auto-generate benchmark questions, evaluate accuracy |
| **Observability** | Planned | Full state-transition tracing, multi-space monitoring |
| **Analyzer** | Planned | Best-practices checklist evaluation, optimization suggestions |
| **Feedback** | Planned | Config versioning, conflict detection, query pattern analysis |

## Data Profiler

The profiler uses a tiered approach — each tier is opt-in and degrades gracefully:

**Tier 1 — Metadata** (always runs, no warehouse needed)
- Column names, types, descriptions, tags via Unity Catalog APIs

**Tier 2 — Data Statistics** (opt-in, needs `warehouse_id`)
- Cardinality, null percentage, min/max, top-N values via SQL

**Tier 3 — Usage Signals** (opt-in, needs system table access)
- Query frequency from `system.query.history`
- PK/FK constraints from `system.information_schema`
- Table lineage from `system.lineage`

**Relationship Detection** (automatic)
- UC declared constraints (highest confidence)
- Column naming patterns (`_id`, `_key`, `_fk` suffixes)
- Deduplication with confidence-based ranking

**Synonym Generation** (opt-in, needs LLM endpoint)
- Business-friendly column synonyms via Databricks Foundation Model API

## Configuration

```python
from genie_world.core.config import GenieWorldConfig, set_config

set_config(GenieWorldConfig(
    warehouse_id="your-warehouse-id",
    llm_model="databricks-claude-sonnet-4-6",
))
```

Or via environment variables:

```bash
export GENIE_WORLD_WAREHOUSE_ID=your-warehouse-id
export GENIE_WORLD_LLM_MODEL=databricks-claude-sonnet-4-6
export GENIE_WORLD_STORAGE_PATH=/Volumes/my/artifacts
```

## Architecture

```
genie_world/
├── core/               # Shared infrastructure
│   ├── auth.py         # WorkspaceClient factory (OBO + PAT/CLI)
│   ├── config.py       # GenieWorldConfig with env var support
│   ├── llm.py          # FMAPI via serving_endpoints.query()
│   ├── sql.py          # Statement Execution API with read-only validation
│   ├── storage.py      # Artifact persistence (local filesystem)
│   ├── tracing.py      # MLflow @trace decorator (no-op fallback)
│   └── models.py       # Shared types (SpaceConfig stub)
├── profiler/           # Data Profiler block
│   ├── __init__.py     # Public API: profile_schema(), profile_tables()
│   ├── models.py       # SchemaProfile, TableProfile, ColumnProfile, etc.
│   ├── metadata_profiler.py    # Tier 1: UC API metadata
│   ├── data_profiler.py        # Tier 2: SQL-based statistics
│   ├── usage_profiler.py       # Tier 3: System table mining
│   ├── relationship_detector.py # PK/FK inference
│   └── synonym_generator.py    # LLM-powered synonyms
├── builder/            # (planned) Space Builder
├── benchmarks/         # (planned) Benchmark Engine
├── observability/      # (planned) Tracing & Monitoring
├── analyzer/           # (planned) Analyzer & Optimizer
└── feedback/           # (planned) Versioning & Patterns
```

## Development

```bash
# Setup
git clone <repo-url>
cd genie-world
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Run specific test file
pytest tests/unit/profiler/test_public_api.py -v
```

## Baselines

This project extends patterns from:

- [databricks-ai-bridge](https://github.com/databricks/databricks-ai-bridge) — Genie API wrapper, MLflow tracing
- [dbx-genie-rx](https://github.com/hiydavid/dbx-genie-rx) — Genie Space analyzer and optimizer
