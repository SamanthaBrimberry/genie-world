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

### 1. Profile your data

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
```

### 2. Build and deploy a Genie Space

```python
from genie_world.builder import build_space, create_space

# Generate complete config (instructions, examples, benchmarks, snippets)
result = build_space(profile, warehouse_id="abc123")

# Check for warnings
for w in result.warnings:
    print(f"[{w.section}] {w.message}")

# Deploy to Databricks
space = create_space(
    result.config,
    display_name="Sales Analytics",
    warehouse_id="abc123",
    parent_path="/Workspace/Users/me/",
)
print(f"Space URL: {space['space_url']}")
```

### 3. Benchmark and improve accuracy

```python
from genie_world.benchmarks import run_benchmarks, diagnose_failures, generate_suggestions, update_space

# Run benchmarks against the live space
results = run_benchmarks(space["space_id"], "abc123")
print(f"Accuracy: {results.accuracy:.0%} ({results.correct}/{results.total})")

# Diagnose failures and generate improvements
diagnoses = diagnose_failures(results)
suggestions = generate_suggestions(diagnoses, results, "abc123")

# Review and apply
for s in suggestions:
    print(f"[{s.section}] {s.action}: {s.rationale}")
update_space(space["space_id"], suggestions, "abc123")
```

### 4. Auto-tune (one-liner)

```python
from genie_world.benchmarks import tune_space

result = tune_space(
    space["space_id"], "abc123",
    target_accuracy=0.9,
    max_iterations=3,
    auto_approve=True,
)
print(f"Final accuracy: {result.final_accuracy:.0%} in {len(result.iterations)} iterations")
```

## The Full Pipeline

```
Profile → Build → Deploy → Benchmark → Diagnose → Suggest → Update → Re-benchmark
   │         │        │         │           │          │          │          │
   ▼         ▼        ▼         ▼           ▼          ▼          ▼          ▼
 Schema   Config   Live     70% acc    Failure    Config     PATCH     90-100%
 Profile  + SQL    Space              types      changes    API       accuracy
```

Tested end-to-end: a Genie Space went from **70% to 100% accuracy** in 2 automated tuning cycles.

## Building Blocks

Each block is independently importable and useful on its own.

| Block | Status | Description |
|-------|--------|-------------|
| **Core** | Done | Auth, config, SQL execution, LLM (FMAPI), Genie client, storage, tracing |
| **Profiler** | Done | Table/column metadata, statistics, PK/FK detection, synonyms, description enrichment |
| **Builder** | Done | Generate Genie Space configs, validate SQL, deploy spaces |
| **Benchmarks** | Done | Run questions, evaluate accuracy, diagnose failures, suggest improvements, update spaces |
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
- `example_sqls` — example Q&A pairs with validated SQL (including parameterized queries)
- `benchmarks` — benchmark questions (different from examples)
- `instructions` — text instructions (generated last to avoid conflicts)

**SQL validation**: Generated SQL is executed against the warehouse. Failures are sent back to the LLM for fixes, up to 3 retries.

**Assembler**: Combines all sections, generates 32-char hex IDs, enforces Genie Space schema constraints (string arrays, sorting, size limits).

**Deployer**: Creates the space via Databricks API with pre-flight size checks.

**Table suggestions**: `suggest_table_exclusions(profile)` recommends tables to exclude (non-queryable types, etc.) — user decides, nothing is silently filtered.

## Benchmarks

The benchmarks block evaluates and improves Genie Space accuracy:

**Runner**: Queries the Genie Conversation API per question in parallel. Extracts questions from the space config and/or accepts custom questions.

**Evaluator**: Hybrid comparison (programmatic + LLM fallback):
- Positional column matching (tolerates alias differences)
- NULL-aware, numeric tolerance (0.1% relative)
- Order-sensitive when ORDER BY detected, LLM judgment for ambiguous ordering
- Performance capture (execution time, row count)

**Diagnoser**: Classifies failures by type — wrong table, missing join, wrong aggregation, missing filter, entity mismatch, wrong date handling, etc. Flags performance issues (10x slower queries).

**Suggester**: Generates targeted config changes per failure type — adds examples, updates instructions, creates filter snippets, fixes column synonyms. Validates suggested SQL against the warehouse.

**Updater**: Fetches current config, merges suggestions (add/update/remove), preserves existing IDs, re-enforces constraints, PATCHes the space in place.

**Auto-tune**: `tune_space()` wraps the full pipeline in an iterative loop with configurable target accuracy and max iterations. Includes regression detection.

## Fine-Grained Control

Every generator is independently importable:

```python
from genie_world.builder import (
    generate_data_sources, generate_join_specs, generate_snippets,
    generate_example_sqls, generate_benchmarks, generate_instructions,
    assemble_space, create_space, suggest_table_exclusions,
)

from genie_world.benchmarks import (
    run_benchmarks, diagnose_failures, generate_suggestions,
    update_space, tune_space,
)
```

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
├── core/                       # Shared infrastructure (9 modules)
│   ├── auth.py                 # WorkspaceClient factory (OBO + PAT/CLI)
│   ├── config.py               # GenieWorldConfig with env var support
│   ├── genie_client.py         # Genie Conversation API wrapper
│   ├── llm.py                  # FMAPI via workspace client raw API
│   ├── sql.py                  # Statement Execution API with read-only validation
│   ├── storage.py              # Artifact persistence (local filesystem)
│   ├── tracing.py              # MLflow @trace decorator (no-op fallback)
│   └── models.py               # Shared types (SpaceConfig stub)
├── profiler/                   # Data Profiler block (8 modules)
│   ├── __init__.py             # Public API: profile_schema(), profile_tables()
│   ├── models.py               # SchemaProfile, TableProfile, ColumnProfile, etc.
│   ├── metadata_profiler.py    # Tier 1: UC API metadata
│   ├── data_profiler.py        # Tier 2: SQL-based statistics
│   ├── usage_profiler.py       # Tier 3: System table mining
│   ├── relationship_detector.py # PK/FK inference (naming + shared columns)
│   ├── synonym_generator.py    # LLM-powered synonyms
│   └── description_enricher.py # LLM-powered missing description generation
├── builder/                    # Space Builder block (10 modules)
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
├── benchmarks/                 # Benchmarks block (8 modules)
│   ├── __init__.py             # Public API: run_benchmarks(), tune_space()
│   ├── models.py               # BenchmarkResult, QuestionResult, Diagnosis, etc.
│   ├── runner.py               # Parallel question execution via GenieClient
│   ├── evaluator.py            # Hybrid comparison + performance capture
│   ├── diagnoser.py            # Failure classification + performance flagging
│   ├── suggester.py            # Targeted config change suggestions
│   └── updater.py              # Config merge + PATCH API updates
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

# Run tests (260 tests)
pytest tests/ -v

# Run specific block tests
pytest tests/unit/core/ -v
pytest tests/unit/profiler/ -v
pytest tests/unit/builder/ -v
pytest tests/unit/benchmarks/ -v
```

## Baselines

This project extends patterns from:

- [databricks-ai-bridge](https://github.com/databricks/databricks-ai-bridge) — Genie API wrapper, MLflow tracing
- [dbx-genie-rx](https://github.com/hiydavid/dbx-genie-rx) — Genie Space analyzer and optimizer
