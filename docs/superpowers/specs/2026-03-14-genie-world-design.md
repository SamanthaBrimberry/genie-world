# Genie World: Modular Genie Space Optimization Toolkit

## Overview

Genie World is a pip-installable Python library that provides modular building blocks for profiling data, building Databricks Genie Spaces programmatically, benchmarking their accuracy, and optimizing their configuration over time. It extends the patterns established in [dbx-genie-rx](https://github.com/hiydavid/dbx-genie-rx) and [databricks-ai-bridge](https://github.com/databricks/databricks-ai-bridge), making them composable, portable, and usable by both Solution Architects and customers.

## Target Users

- **Solution Architects / Solution Engineers**: Setting up high-quality Genie Spaces for customer demos, POCs, and production deployments
- **Customers / Data Analysts**: Diagnosing and improving accuracy of existing Genie Spaces they own

## Design Principles

- **Library-first**: Works in Databricks notebooks, local scripts, Databricks jobs — anywhere Python runs
- **Building blocks**: Each block is independently importable and useful on its own
- **MCP-ready interfaces**: Stateless, self-contained function signatures that map cleanly to future MCP tool definitions
- **Tiered dependencies**: Core requires only `databricks-sdk` and `pydantic`. Each block's extras (Spark, MLflow, LLM endpoints) are opt-in via `pip install genie-world[profiler]`
- **Graceful degradation**: Missing MLflow? Tracing no-ops. No warehouse? Metadata-only profiling. No LLM? Skip synonym generation.

## Architecture

### Package Structure

```
genie-world/
├── pyproject.toml
├── genie_world/
│   ├── __init__.py
│   ├── core/                  # Shared infrastructure
│   │   ├── __init__.py
│   │   ├── models.py          # Shared Pydantic types
│   │   ├── auth.py            # WorkspaceClient factory, OBO support
│   │   ├── genie_client.py    # Genie REST API wrapper with full payload capture
│   │   ├── llm.py             # LLM serving endpoint wrapper (retry, JSON repair)
│   │   ├── sql.py             # Statement Execution API wrapper
│   │   ├── storage.py         # Artifact persistence (UC Volumes / local filesystem)
│   │   ├── tracing.py         # MLflow trace decorator (optional, no-op fallback)
│   │   └── config.py          # Project-level configuration
│   ├── profiler/              # Block 1: Data Profiler
│   ├── builder/               # Block 2: Space Builder
│   ├── benchmarks/            # Block 3: Benchmark Engine
│   ├── observability/         # Block 4: Tracing & Monitoring
│   ├── analyzer/              # Block 5: Analyzer & Optimizer
│   └── feedback/              # Block 6: Versioning, Conflicts, Patterns
├── tests/
│   ├── unit/
│   └── integration/
└── docs/
```

### Core Module Details

**`core/models.py`** — Shared Pydantic types that flow between blocks. A `TableProfile` from the profiler feeds into the builder. A `SpaceConfig` flows from builder to analyzer to optimizer. This is the contract between building blocks.

**`core/auth.py`** — Databricks authentication borrowed from dbx-genie-rx's contextvars pattern. Supports OBO (On-Behalf-Of) for Databricks Apps, PAT, OAuth, and CLI auth. Every block uses this so no block handles auth itself.

**`core/genie_client.py`** — Wraps the Genie REST API using the pattern from the tracing demo (full payload visibility at every state transition) combined with databricks-ai-bridge's `Genie` class. Provides every block access to the API with full observability built in.

**`core/llm.py`** — Wraps Databricks serving endpoints with retry logic, JSON response parsing/repair, and rate limit handling. Ported from dbx-genie-rx's `llm_utils.py`. All blocks that need LLM access use this shared utility.

**`core/sql.py`** — Statement Execution API wrapper with read-only SQL validation, column/row/data normalization. Ported from dbx-genie-rx's `sql_executor.py`. Used by profiler (data queries), benchmarks (result comparison), and observability (query execution).

**`core/storage.py`** — Artifact persistence with a simple interface. Default: Unity Catalog Volumes (JSON/Parquet files in `/Volumes/...`). Fallback: local filesystem. Each block produces serializable artifacts. Users choose where they land.

**`core/tracing.py`** — MLflow trace decorator that no-ops when MLflow is absent. Adopts databricks-ai-bridge's `_compat.py` pattern. Every block uses this for automatic tracing without hard-depending on MLflow.

**`core/config.py`** — Project-level configuration: LLM endpoint name, default warehouse ID, storage location, MLflow experiment ID. Loadable from environment variables, `.env` files, or passed programmatically.

### Block Interconnections

```
                    ┌──────────────┐
                    │   Profiler   │
                    └──────┬───────┘
                           │ SchemaProfile
                    ┌──────▼───────┐
                    │   Builder    │
                    └──────┬───────┘
                           │ SpaceConfig
              ┌────────────┼────────────┐
              ▼            ▼            ▼
       ┌────────────┐ ┌─────────┐ ┌──────────────┐
       │ Benchmarks │ │Analyzer │ │Observability │
       └─────┬──────┘ └────┬────┘ └──────┬───────┘
             │              │             │
             └──────┬───────┘             │
                    ▼                     ▼
             ┌────────────┐       ┌────────────┐
             │ Optimizer  │       │  Feedback   │
             └─────┬──────┘       └──────┬──────┘
                   │                     │
                   └─────────┬───────────┘
                             ▼
                      Back to Builder
                      (iterate & improve)
```

---

## Block 1: Data Profiler (First Deliverable)

### Purpose

Takes a catalog/schema (or list of tables) and produces rich metadata that feeds into every downstream block. Profiles column types, descriptions, cardinality, value distributions, PK/FK relationships, usage patterns, and business synonyms.

### Module Structure

```
profiler/
├── __init__.py              # Public API: profile_tables(), profile_schema()
├── metadata_profiler.py     # Tier 1: UC API-based (schema, descriptions, tags, stats)
├── data_profiler.py         # Tier 2: SQL-based (distributions, cardinality, samples)
├── usage_profiler.py        # Tier 3: System tables (query frequency, lineage, co-occurrence)
├── relationship_detector.py # PK/FK inference (UC constraints, naming, lineage, value overlap)
├── synonym_generator.py     # LLM-powered column synonyms + business terminology
└── models.py                # Profiler-specific models
```

### Profiling Tiers

**Tier 1 — Metadata Profiler** (always runs, no warehouse or LLM needed):
- Uses Databricks SDK / Unity Catalog APIs
- Extracts: column names, data types, descriptions, tags, table comments, row counts
- Fast — completes in seconds for most schemas

**Tier 2 — Data Profiler** (opt-in, needs warehouse ID):
- Runs SQL queries via Statement Execution API
- Computes: cardinality, null percentage, top-N frequent values, min/max, sample rows
- Works in notebooks AND external environments (no Spark session required)

**Tier 3 — Usage Profiler** (opt-in, needs system table access):
- Mines Unity Catalog system tables for real-world usage signals
- Sources:
  - `system.information_schema.table_constraints` + `key_column_usage` — declared PK/FK constraints
  - `system.lineage.table_lineage` — upstream/downstream table relationships
  - `system.lineage.column_lineage` — column-level data flow
  - `system.query.history` — query frequency, column co-occurrence patterns
  - `system.access.audit` — table/column access frequency, staleness detection
- Graceful degradation: if system tables aren't accessible, logs a warning and continues

**Synonym Generation** (opt-in, needs LLM endpoint):
- Sends column profiles to LLM in batches per table
- Includes column names, descriptions, data types, and sample values for context
- Produces business-friendly synonyms (e.g., `sales_territory` → `["region", "territory", "sales area"]`)

**Relationship Detection** (runs on profiles, multi-source):
1. UC constraints (highest confidence) — from `information_schema.table_constraints`
2. Lineage (high confidence) — from `system.lineage.table_lineage` and `column_lineage`
3. Naming patterns (medium confidence) — scans for `_id`, `_key`, `_fk` suffixes and matches across tables
4. Query co-occurrence (medium confidence) — columns commonly joined in historical queries
5. Value overlap (lower confidence, expensive) — validates candidate relationships by comparing actual values

### Data Models

```python
class ColumnProfile(BaseModel):
    name: str
    data_type: str
    description: str | None
    nullable: bool
    cardinality: int | None
    null_percent: float | None
    top_values: list[str] | None
    min_value: str | None
    max_value: str | None
    sample_values: list[str] | None
    synonyms: list[str] | None
    tags: dict[str, str] | None
    query_frequency: int | None
    co_queried_columns: list[str] | None

class TableProfile(BaseModel):
    catalog: str
    schema_name: str
    table: str
    description: str | None
    row_count: int | None
    columns: list[ColumnProfile]
    query_frequency: int | None
    upstream_tables: list[str] | None
    downstream_tables: list[str] | None

class Relationship(BaseModel):
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    confidence: float  # 0-1
    detection_method: str  # "uc_constraint" | "lineage" | "query_cooccurrence" | "naming_pattern" | "value_overlap"

class SchemaProfile(BaseModel):
    catalog: str
    schema_name: str
    tables: list[TableProfile]
    relationships: list[Relationship]
    profiled_at: datetime
```

### Public API

```python
from genie_world.profiler import profile_tables, profile_schema

# Full profiling — all tiers
profile = profile_schema(
    catalog="my_catalog",
    schema="my_schema",
    deep=True,              # Tier 2: data profiling via SQL
    usage=True,             # Tier 3: system table mining
    synonyms=True,          # LLM synonym generation
    warehouse_id="abc123",
)

# Profile specific tables
profile = profile_tables(
    tables=["my_catalog.my_schema.orders", "my_catalog.my_schema.customers"],
    deep=True,
    synonyms=True,
    warehouse_id="abc123",
)

# Minimal — metadata only, no warehouse or LLM needed
profile = profile_schema("my_catalog", "my_schema")
```

### Data Flow

```
Input: catalog.schema (or table list)
         │
         ▼
┌─────────────────────┐
│  metadata_profiler   │  UC API: column names, types, descriptions,
│  (always runs)       │  tags, table comments, row counts
└────────┬────────────┘
         │ TableProfile (partial)
         ▼
┌─────────────────────┐
│  data_profiler       │  SQL via Statement Execution API:
│  (opt-in via deep)   │  cardinality, null %, top-N values,
└────────┬────────────┘  min/max, sample rows
         │ TableProfile (enriched)
         ▼
┌─────────────────────┐
│  usage_profiler      │  System tables: query frequency,
│  (opt-in via usage)  │  lineage, co-occurrence, access patterns
└────────┬────────────┘
         │ TableProfile (enriched + usage signals)
         ▼
┌─────────────────────┐
│ relationship_detector│  UC constraints + lineage + naming patterns +
│ (runs on profiles)   │  query co-occurrence + value overlap
└────────┬────────────┘
         │ list[Relationship]
         ▼
┌─────────────────────┐
│  synonym_generator   │  LLM: column names + descriptions + sample
│  (opt-in via synon.) │  values → business-friendly synonyms
└────────┬────────────┘
         │ TableProfile (final with synonyms)
         ▼
Output: SchemaProfile (all tables + relationships + synonyms)
        → stored as JSON artifact via core/storage.py
```

---

## Block 2: Space Builder (Future)

### Purpose

Takes a `SchemaProfile` and programmatically generates a complete Genie Space configuration — instructions, example Q&A pairs, SQL snippets, join specs, and column configs.

### Public API

```python
from genie_world.builder import build_space, create_space

config = build_space(
    profile=schema_profile,
    instructions=True,
    examples=True,
    snippets=True,
)

space = create_space(
    config=config,
    display_name="Sales Analytics",
    target_directory="/Workspace/Users/me/genie-spaces",
)
```

### Key Modules

- `instruction_generator.py` — LLM generates text instructions from profile context, following best practices from dbx-genie-rx's checklist
- `example_generator.py` — LLM generates example Q&A pairs with validated SQL
- `snippet_generator.py` — generates filters, expressions, measures from column profiles
- `join_spec_generator.py` — generates join specifications from detected relationships
- `column_config_generator.py` — generates column configs with descriptions, synonyms, entity matching flags
- `space_assembler.py` — combines all generated components into a valid `SpaceConfig`
- `space_deployer.py` — creates the space via Databricks API (ported from dbx-genie-rx's `genie_creator.py`)

### Consumes / Produces

- **Input:** `SchemaProfile` (from profiler)
- **Output:** `SpaceConfig` (Pydantic model matching Genie Space JSON schema)

---

## Block 3: Benchmark Engine (Future)

### Purpose

Auto-generates benchmark questions from data profiles and evaluates Genie's answers with hybrid labeling.

### Public API

```python
from genie_world.benchmarks import generate_benchmarks, run_benchmarks

questions = generate_benchmarks(
    profile=schema_profile,
    count=20,
    difficulty_mix={"simple": 0.3, "moderate": 0.5, "complex": 0.2},
)

results = run_benchmarks(
    space_id="abc123",
    benchmarks=questions,
    warehouse_id="wh456",
)
```

### Key Modules

- `question_generator.py` — LLM generates questions from profile (column types, relationships, value distributions inform difficulty)
- `benchmark_runner.py` — queries Genie per question, executes expected + actual SQL, collects results
- `result_evaluator.py` — hybrid labeling ported from dbx-genie-rx's `error_analysis.py` (programmatic comparison + LLM fallback)
- `failure_diagnoser.py` — classifies failures by type (wrong table, missing join, wrong aggregation, etc.)

### Consumes / Produces

- **Input:** `SchemaProfile`, `SpaceConfig`
- **Output:** `BenchmarkResults` (questions + generated SQL + labels + failure diagnoses)

---

## Block 4: Observability (Future)

### Purpose

Full state-transition tracing with complete API payload capture (what the SDK hides), plus multi-space monitoring and regression detection.

### Public API

```python
from genie_world.observability import trace_query, get_space_metrics

trace = trace_query(
    space_id="abc123",
    question="Top 10 products by revenue",
)

metrics = get_space_metrics(
    space_ids=["abc123", "def456"],
    since="7d",
)
```

### Key Modules

- `tracer.py` — full state-transition tracing using REST API approach from `genie_tracing_demo.py`, captures every state (FETCHING_METADATA, FILTERING_CONTEXT, ASKING_AI, EXECUTING_QUERY, COMPLETED) with complete payloads
- `monitor.py` — aggregates traces across spaces, computes latency percentiles, failure rates, throughput
- `regression_detector.py` — compares current performance against historical baselines, alerts on degradation

### Consumes / Produces

- **Input:** Space IDs, MLflow experiment
- **Output:** `QueryTrace`, `SpaceMetrics`, `RegressionAlert`

---

## Block 5: Analyzer & Optimizer (Future)

### Purpose

Ported from dbx-genie-rx — evaluates space config against best practices checklist, generates field-level optimization suggestions informed by benchmark results and data profiles.

### Public API

```python
from genie_world.analyzer import analyze_space, optimize_space

analysis = analyze_space(space_id="abc123")

suggestions = optimize_space(
    space_id="abc123",
    benchmark_results=results,
    profile=schema_profile,
)
```

### Key Modules

- `checklist_evaluator.py` — markdown-driven checklist evaluation (ported from dbx-genie-rx's `agent.py` + `checklist_parser.py`)
- `optimizer.py` — field-level suggestion generation (ported from dbx-genie-rx's `optimizer.py`)
- `synthesizer.py` — cross-sectional synthesis (ported from dbx-genie-rx's `synthesizer.py`)
- `conflict_detector.py` — NEW: cross-references text instructions, SQL examples, and snippets to surface contradictions

### Consumes / Produces

- **Input:** `SpaceConfig`, `BenchmarkResults`, `SchemaProfile`
- **Output:** `AnalysisResult`, `list[OptimizationSuggestion]`

---

## Block 6: Feedback Loop (Future)

### Purpose

Config versioning, instruction conflict detection, and query pattern analysis. Closes the loop between observability and optimization.

### Public API

```python
from genie_world.feedback import snapshot_config, detect_conflicts, analyze_patterns

snapshot_config(space_id="abc123", label="post-optimization-v2")

conflicts = detect_conflicts(space_id="abc123")

patterns = analyze_patterns(space_id="abc123", since="30d")
```

### Key Modules

- `versioner.py` — snapshots space configs with labels and timestamps, diffs between versions, tracks score changes
- `conflict_detector.py` — cross-references all instruction types for contradictions
- `pattern_analyzer.py` — mines query traces for failing question patterns, misselected columns, SQL anti-patterns

### Consumes / Produces

- **Input:** `SpaceConfig`, `QueryTrace` (from observability), `BenchmarkResults`
- **Output:** `ConfigSnapshot`, `list[Conflict]`, `PatternReport`

---

## Baseline Sources

This project extends and builds on:

1. **[databricks-ai-bridge](https://github.com/databricks/databricks-ai-bridge)** — Core `Genie` class (REST API wrapper), `GenieAgent` (LangChain-compatible), MLflow tracing with state-change spans, result parsing/truncation. Patterns adopted for `core/genie_client.py` and `core/tracing.py`.

2. **[dbx-genie-rx](https://github.com/hiydavid/dbx-genie-rx)** — Full-stack Genie Space analyzer. Config ingestion, 10-section best-practices evaluation, benchmark labeling with hybrid error analysis, AI-driven optimization suggestions, new space creation. Patterns ported into analyzer, benchmarks, and builder blocks.

3. **genie_tracing_demo.py** — Raw REST API approach for full visibility into every Genie state transition. Captures complete API payloads that the SDK hides. Pattern adopted for `observability/tracer.py`.

## Implementation Sequence

1. **Core** — shared infrastructure (auth, models, genie client, LLM, SQL, storage, tracing, config)
2. **Profiler** — data profiling (metadata, deep, usage, relationships, synonyms)
3. **Builder** — space generation from profiles
4. **Benchmarks** — question generation and evaluation
5. **Observability** — tracing and monitoring
6. **Analyzer** — checklist evaluation and optimization (port from dbx-genie-rx)
7. **Feedback** — versioning, conflicts, patterns

Each block is independently useful after core is complete. The sequence above reflects dependency order but blocks can be developed in parallel once core stabilizes.

## Testing Strategy

- **Unit tests**: Per-module, mocked Databricks APIs, no workspace needed
- **Integration tests**: Require a Databricks workspace, gated behind environment variable (`RUN_INTEGRATION_TESTS=1`)
- **Test data**: Synthetic schemas for unit tests, real catalog/schema for integration tests
- **CI**: Unit tests run on every PR, integration tests run nightly or on-demand

## Future Layers (Not In Scope)

- **MCP Server**: Thin wrapper exposing library functions as MCP tools for Claude Code/Cursor
- **Databricks App**: Web UI extending dbx-genie-rx's React frontend, consuming this library
- **CLI**: Command-line interface for common workflows
