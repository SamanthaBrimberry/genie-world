# Genie World

Modular Python library for building, benchmarking, and optimizing Databricks Genie Spaces. Profile your data, generate a complete Genie Space config, deploy it, and iteratively improve accuracy — all from a notebook or script.

## Installation

```bash
pip install -e ".[all]"
```

On Databricks:
```python
%pip install git+https://github.com/SamanthaBrimberry/genie-world.git
dbutils.library.restartPython()
```

Requires Python 3.10+ and a Databricks workspace with a SQL warehouse.

## Quick Start

```python
from genie_world.profiler import profile_schema
from genie_world.builder import build_space, create_space
from genie_world.benchmarks import run_benchmarks, diagnose_failures, generate_suggestions, update_space

WAREHOUSE = "your-warehouse-id"

# 1. Profile your schema
profile = profile_schema(
    "my_catalog", "my_schema",
    deep=True, synonyms=True, enrich_descriptions=True,
    warehouse_id=WAREHOUSE,
)

# 2. Build and deploy a Genie Space
result = build_space(profile, warehouse_id=WAREHOUSE)
space = create_space(result.config, "My Space", WAREHOUSE, "/Workspace/Users/me/")
print(space["space_url"])

# 3. Benchmark accuracy
results = run_benchmarks(space["space_id"], WAREHOUSE)
print(f"Accuracy: {results.accuracy:.0%}")

# 4. Improve — diagnose failures, generate fixes, apply
diagnoses = diagnose_failures(results)
suggestions = generate_suggestions(diagnoses, results, WAREHOUSE)
update_space(space["space_id"], suggestions, WAREHOUSE)
```

Or auto-tune in one call:

```python
from genie_world.benchmarks import tune_space

result = tune_space(space["space_id"], WAREHOUSE, target_accuracy=0.9, auto_approve=True)
print(f"Final: {result.final_accuracy:.0%} in {len(result.iterations)} iterations")
```

## How It Works

**Profiler** scans your Unity Catalog tables and produces a rich schema profile — column types, descriptions, cardinality, relationships between tables, and business-friendly synonyms. Works in tiers: metadata-only (no warehouse needed), SQL-based statistics, and system table mining.

**Builder** takes the profile and generates a complete Genie Space config — data sources with entity matching, SQL instructions, example Q&A pairs (validated against the warehouse), filter/expression/measure snippets, and benchmark questions. Deploys via the Databricks API.

**Benchmarks** runs questions against the live Genie Space, compares results with hybrid evaluation (programmatic matching + LLM fallback for ambiguous cases), diagnoses failure types (wrong table, missing join, wrong aggregation, etc.), generates targeted config improvements, and applies them via the PATCH API. The auto-tune loop repeats until your target accuracy is reached.

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
```

## Baselines

Extends patterns from [databricks-ai-bridge](https://github.com/databricks/databricks-ai-bridge) and [dbx-genie-rx](https://github.com/hiydavid/dbx-genie-rx).
