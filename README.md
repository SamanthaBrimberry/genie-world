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

On Databricks notebooks, install from GitHub:
```python
%pip install git+https://github.com/SamanthaBrimberry/genie-world.git
dbutils.library.restartPython()
```

Requires Python 3.10+ and a Databricks workspace.

## Quick Start

### Profile a schema

```python
from genie_world.profiler import profile_schema

# Metadata only — no warehouse or LLM needed
profile = profile_schema("my_catalog", "my_schema")

# Full profiling — statistics, usage signals, synonyms, description enrichment
profile = profile_schema(
    "my_catalog", "my_schema",
    deep=True,                  # Tier 2: cardinality, null %, min/max via SQL
    usage=True,                 # Tier 3: query frequency, PK/FK from system tables
    synonyms=True,              # LLM-generated business-friendly column synonyms
    enrich_descriptions=True,   # LLM fills missing table/column descriptions
    warehouse_id="abc123",
)

for table in profile.tables:
    print(f"{table.table} — {len(table.columns)} columns")

for rel in profile.relationships:
    print(f"  {rel.source_table}.{rel.source_column} -> {rel.target_table}.{rel.target_column}")
```

### Build a Genie Space

```python
from genie_world.builder import build_space, create_space

# Generate complete config (instructions, examples, benchmarks, join specs)
result = build_space(profile, warehouse_id="abc123")

# Check for warnings
for w in result.warnings:
    print(f"[{w.section}] {w.message}")

# Inspect what was generated
config = result.config
print(f"Example SQLs: {len(config['instructions']['example_question_sqls'])}")
print(f"Join specs: {len(config['instructions']['join_specs'])}")
print(f"Benchmarks: {len(config.get('benchmarks', {}).get('questions', []))}")

# Deploy to Databricks
space = create_space(
    result.config,
    display_name="Sales Analytics",
    warehouse_id="abc123",
    parent_path="/Workspace/Users/me/",
)
print(f"Space URL: {space['space_url']}")
```

### Fine-grained control

```python
from genie_world.builder import (
    generate_data_sources, generate_join_specs, generate_snippets,
    generate_example_sqls, generate_benchmarks, generate_instructions,
    assemble_space, create_space,
)

# Generate sections individually
data_sources = generate_data_sources(profile)
join_specs = generate_join_specs(profile)
snippets = generate_snippets(profile)
examples, _ = generate_example_sqls(profile, join_specs, snippets, warehouse_id="abc123")
benchmarks, _ = generate_benchmarks(profile, join_specs, snippets, examples, warehouse_id="abc123")
instructions = generate_instructions(profile, join_specs, snippets, examples)

# Customize before assembling
instructions[0]["content"].append("Always use fiscal quarters, not calendar quarters.")

config = assemble_space(data_sources, join_specs, instructions, snippets, examples, benchmarks)
```

## Building Blocks

Genie World is designed as composable building blocks. Each block is independently importable and useful on its own.

| Block | Status | Description |
|-------|--------|-------------|
| **Core** | Done | Auth, config, SQL execution, LLM (FMAPI), storage, tracing |
| **Profiler** | Done | Table/column metadata, statistics, PK/FK detection, synonyms, description enrichment |
| **Builder** | Done | Generate Genie Space configs from profiles, validate SQL, deploy spaces |
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

**Relationship Detection** (automatic)
- UC declared constraints (highest confidence)
- Shared column names across tables (e.g., `campaign_id` on multiple tables)
- Column naming patterns (`_id`, `_key`, `_fk` suffixes)
- Deduplication with confidence-based ranking

**Synonym Generation** (opt-in, needs LLM endpoint)
- Business-friendly column synonyms via Databricks Foundation Model API

**Description Enrichment** (opt-in, needs LLM endpoint)
- LLM fills missing table and column descriptions using column names, types, and sample values as context

## Space Builder

The builder generates complete Genie Space configurations from profiler output:

**Deterministic generators** (no LLM needed):
- `data_sources` — tables with column configs, entity matching, format assistance
- `join_specs` — join SQL from detected relationships

**LLM generators**:
- `snippets` — SQL filters, expressions, and measures
- `example_sqls` — example Q&A pairs with validated SQL
- `benchmarks` — benchmark questions (different from examples)
- `instructions` — text instructions (generated last to avoid conflicts)

**SQL validation**: Generated SQL is executed against the warehouse. Failures are sent back to the LLM for fixes, up to 3 retries.

**Assembler**: Combines all sections, generates 32-char hex IDs, enforces Genie Space schema constraints (string arrays, sorting, size limits).

**Deployer**: Creates the space via Databricks API with pre-flight size checks.

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
├── core/                       # Shared infrastructure
│   ├── auth.py                 # WorkspaceClient factory (OBO + PAT/CLI)
│   ├── config.py               # GenieWorldConfig with env var support
│   ├── llm.py                  # FMAPI via workspace client raw API
│   ├── sql.py                  # Statement Execution API with read-only validation
│   ├── storage.py              # Artifact persistence (local filesystem)
│   ├── tracing.py              # MLflow @trace decorator (no-op fallback)
│   └── models.py               # Shared types (SpaceConfig stub)
├── profiler/                   # Data Profiler block
│   ├── __init__.py             # Public API: profile_schema(), profile_tables()
│   ├── models.py               # SchemaProfile, TableProfile, ColumnProfile, etc.
│   ├── metadata_profiler.py    # Tier 1: UC API metadata
│   ├── data_profiler.py        # Tier 2: SQL-based statistics
│   ├── usage_profiler.py       # Tier 3: System table mining
│   ├── relationship_detector.py # PK/FK inference (naming + shared columns)
│   ├── synonym_generator.py    # LLM-powered synonyms
│   └── description_enricher.py # LLM-powered missing description generation
├── builder/                    # Space Builder block
│   ├── __init__.py             # Public API: build_space(), BuildResult
│   ├── data_sources.py         # Deterministic: profile -> tables + column_configs
│   ├── join_specs.py           # Deterministic: relationships -> join SQL
│   ├── snippets.py             # LLM: filters, expressions, measures
│   ├── example_sqls.py         # LLM + validate: example Q&A pairs
│   ├── benchmarks.py           # LLM + validate: benchmark questions
│   ├── instructions.py         # LLM: text instructions (generated last)
│   ├── sql_validator.py        # Execute SQL, LLM fix, retry up to 3x
│   ├── assembler.py            # Combine sections, IDs, constraints, sorting
│   └── deployer.py             # Create space via Databricks API
├── benchmarks/                 # (planned) Benchmark Engine
├── observability/              # (planned) Tracing & Monitoring
├── analyzer/                   # (planned) Analyzer & Optimizer
└── feedback/                   # (planned) Versioning & Patterns
```

## Development

```bash
# Setup
git clone https://github.com/SamanthaBrimberry/genie-world.git
cd genie-world
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Run tests (142 tests)
pytest tests/ -v

# Run specific block tests
pytest tests/unit/profiler/ -v
pytest tests/unit/builder/ -v
pytest tests/unit/core/ -v
```

## Baselines

This project extends patterns from:

- [databricks-ai-bridge](https://github.com/databricks/databricks-ai-bridge) — Genie API wrapper, MLflow tracing
- [dbx-genie-rx](https://github.com/hiydavid/dbx-genie-rx) — Genie Space analyzer and optimizer
