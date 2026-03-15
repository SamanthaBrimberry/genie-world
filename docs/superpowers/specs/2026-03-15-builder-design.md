# Genie World: Space Builder Block Design

## Overview

The Space Builder takes a `SchemaProfile` (from the profiler) and programmatically generates a complete Databricks Genie Space configuration. It produces valid, deployable configs with data sources, column configs, join specs, instructions, example SQL queries, snippets, and benchmark questions — all from profiled metadata.

## Prerequisites

**Profiler enhancement (before Builder implementation):** The profiler should generate missing column/table descriptions via LLM when metadata is sparse. This ensures the Builder receives complete metadata and doesn't need to fill gaps itself. Add an `enrich_descriptions=True` flag to `profile_schema()` that calls the LLM for tables/columns with no description.

## Design Principles

- **Section generators + assembler** — Each config section has its own generator. An assembler combines them into a valid config. `build_space()` calls all generators, but users can call them individually for fine-grained control.
- **Deterministic where possible** — `data_sources` and `join_specs` are derived directly from the profile with no LLM calls.
- **Validate + retry SQL** — Generated SQL is executed against the warehouse. Failures are sent back to the LLM for fixes, up to 3 retries.
- **Instructions generated last** — Avoids conflicts with examples and snippets by generating text instructions after everything else exists.

## Module Structure

```
builder/
├── __init__.py           # Public API: build_space(), create_space()
├── data_sources.py       # Deterministic: profile → tables + column_configs
├── join_specs.py         # Deterministic: relationships → join SQL
├── instructions.py       # LLM: text instructions (generated last)
├── snippets.py           # LLM: filters, expressions, measures
├── example_sqls.py       # LLM + validate: example Q&A pairs with SQL
├── benchmarks.py         # LLM + validate: benchmark questions with SQL
├── sql_validator.py      # Execute SQL, send errors to LLM, retry up to 3x
├── assembler.py          # Combine sections, generate IDs, enforce constraints
└── deployer.py           # Create space via Databricks API
```

## Generation Order

```
data_sources ─┐
              ├─ (parallel, deterministic, no LLM)
join_specs   ─┘
      │
snippets (LLM)
      │
examples (LLM + validate)
      │
benchmarks (LLM + validate, receives examples to avoid overlap)
      │
instructions (LLM, receives join_specs + snippets + examples)
      │
assembler → valid SpaceConfig dict
```

Instructions are generated last so the LLM can see all existing examples and snippets, avoiding contradictions (per dbx-genie-rx best practices).

## Module Details

### `data_sources.py` (Deterministic)

```python
def generate_data_sources(profile: SchemaProfile) -> dict:
    """Transform a SchemaProfile into the data_sources config section."""
```

For each table in the profile:
- `identifier` → `"{catalog}.{schema_name}.{table}"`
- `description` → from `TableProfile.description` (wrapped in list per schema)
- `column_configs` → for each column:
  - `column_name` → `ColumnProfile.name`
  - `description` → `ColumnProfile.description` (if present, wrapped in list)
  - `synonyms` → `ColumnProfile.synonyms` (if present)
  - `enable_entity_matching` → `True` for string columns with low cardinality (< 100) and synonyms
  - `enable_format_assistance` → `True` for date/timestamp columns and low-cardinality string columns
  - `exclude` → `True` for columns matching internal patterns (`_metadata`, `_rescued_data`, etc.)

Returns `{"tables": [...]}` sorted by `identifier`, each table's `column_configs` sorted by `column_name`.

### `join_specs.py` (Deterministic)

```python
def generate_join_specs(profile: SchemaProfile) -> list[dict]:
    """Transform relationships into join_specs config section."""
```

For each relationship with confidence >= 0.6:
- `left.identifier` → source table full name
- `left.alias` → short table name
- `right.identifier` → target table full name
- `right.alias` → short table name
- `sql` → `["{left_alias}.{source_column} = {right_alias}.{target_column}"]`
- `comment` → auto-generated join description
- `instruction` → when to use this join (stronger language for higher confidence)

### `snippets.py` (LLM)

```python
def generate_snippets(profile: SchemaProfile) -> dict:
    """Generate SQL snippet configs (filters, expressions, measures) via LLM."""
```

Returns `{"filters": [...], "expressions": [...], "measures": [...]}`.

The LLM receives column profiles (names, types, top values, descriptions) and generates:
- **Filters** — common business filters from low-cardinality columns. Each has: `sql`, `display_name`, `synonyms`, `comment`, `instruction`.
- **Expressions** — reusable calculated columns. Each has: `alias`, `sql`, `display_name`, `synonyms`, `comment`, `instruction`.
- **Measures** — standard aggregations. Each has: `alias`, `sql`, `display_name`, `synonyms`, `comment`, `instruction`.

### `example_sqls.py` (LLM + Validate)

```python
def generate_example_sqls(
    profile: SchemaProfile,
    join_specs: list[dict],
    snippets: dict,
    warehouse_id: str | None = None,
    count: int = 10,
) -> list[dict]:
    """Generate example Q&A pairs with SQL, validated against warehouse."""
```

The LLM receives profile context, join specs, and snippets. Generates `count` example Q&A pairs, each with:
- `question` → natural language question
- `sql` → SQL answer
- `parameters` → for parameterized queries: `name`, `type_hint`, `description`, `default_value`
- `usage_guidance` → when to use this example

Asks for a mix of complexity: simple single-table, multi-table joins, aggregations with filters, parameterized queries.

If `warehouse_id` is provided, each SQL is validated via `sql_validator.py`. If None, SQL is generated without validation and a warning is logged.

### `benchmarks.py` (LLM + Validate)

```python
def generate_benchmarks(
    profile: SchemaProfile,
    join_specs: list[dict],
    snippets: dict,
    examples: list[dict],
    warehouse_id: str | None = None,
    count: int = 10,
) -> dict:
    """Generate benchmark questions with SQL, different from examples."""
```

Similar to `example_sqls` but:
- Receives `examples` as context to generate different questions (varied phrasings, edge cases, ambiguous queries)
- Returns `{"questions": [...]}` where each question has `question` and `answer: [{"format": "SQL", "content": [...]}]`
- Same validation + retry logic via `sql_validator.py`

### `sql_validator.py`

```python
def validate_and_fix_sql(
    sql: str,
    question: str,
    profile: SchemaProfile,
    warehouse_id: str,
    max_retries: int = 3,
) -> tuple[str, list[str]]:
    """Execute SQL, retry with LLM fix on failure. Returns (final_sql, warnings)."""
```

1. Execute SQL via `core/sql.py`
2. If succeeds → return as-is
3. If fails → send error message + original question + profile context to LLM asking for a fix
4. Retry up to `max_retries` times
5. If still failing → return last attempt with a warning

### `instructions.py` (LLM, Generated Last)

```python
def generate_instructions(
    profile: SchemaProfile,
    join_specs: list[dict],
    snippets: dict,
    examples: list[dict],
) -> list[dict]:
    """Generate text instructions that complement existing examples and snippets."""
```

Generated LAST so it sees all examples and snippets. Produces a single text instruction covering:
- Business term interpretation
- Default time period handling
- Rounding/formatting conventions
- Clarification triggers (when to ask user for more info)
- References to key join behavior

The prompt includes dbx-genie-rx best practices: focused, minimal, globally-applicable, no conflicts with SQL examples.

Returns a list containing one text instruction dict with `content` as a list of strings.

### `assembler.py`

```python
def assemble_space(
    data_sources: dict,
    join_specs: list[dict],
    instructions: list[dict],
    snippets: dict,
    examples: list[dict],
    benchmarks: dict | None = None,
    *,
    sql_functions: list[dict] | None = None,
    metric_views: list[dict] | None = None,
) -> dict:
    """Combine all sections into a valid Genie Space config."""
```

Handles:
- **ID generation** — 32-char lowercase hex IDs via `uuid4().hex` for all elements
- **`sample_questions` derivation** — extracts 3-5 question texts from `examples`, wraps in `{"id": "...", "question": ["..."]}` format (no SQL)
- **String array enforcement** — wraps bare strings in lists for schema fields (`description`, `content`, `sql`, `synonyms`, etc.)
- **String splitting** — splits strings > 1KB into multiple array elements
- **Sorting** — sorts all arrays by required keys per schema
- **Constraint enforcement** — at most 1 text instruction, removes empty SQL snippets
- **Schema version** — sets `version: 2`
- **Correct nesting** — outputs the proper structure:

```python
{
    "version": 2,
    "config": {"sample_questions": [...]},
    "data_sources": {"tables": [...], "metric_views": [...]},
    "instructions": {
        "text_instructions": [...],
        "example_question_sqls": [...],
        "sql_functions": [...],
        "join_specs": [...],
        "sql_snippets": {
            "filters": [...],
            "expressions": [...],
            "measures": [...]
        }
    },
    "benchmarks": {"questions": [...]}
}
```

- **Pass-through sections** — includes `sql_functions` and `metric_views` if provided

### `deployer.py`

```python
def create_space(
    config: dict,
    display_name: str,
    warehouse_id: str,
    parent_path: str,
    description: str | None = None,
) -> dict:
    """Deploy a config to Databricks as a new Genie Space.

    Returns {"space_id": "...", "display_name": "...", "space_url": "..."}.
    """
```

Ported from dbx-genie-rx's `genie_creator.py`:
- Serializes config to JSON string for `serialized_space`
- Calls `POST /api/2.0/genie/spaces` via workspace client
- Maps common API errors to user-friendly exceptions (403 → PermissionError, 400 → ValueError)

## Public API

### `build_space()` — Full auto

```python
def build_space(
    profile: SchemaProfile,
    *,
    warehouse_id: str | None = None,
    example_count: int = 10,
    benchmark_count: int = 10,
    sql_functions: list[dict] | None = None,
    metric_views: list[dict] | None = None,
) -> dict:
    """Generate a complete Genie Space config from a SchemaProfile.

    If warehouse_id is provided, generated SQL is validated and fixed (up to 3 retries).
    If None, SQL is generated without validation (warning logged).
    """
```

### `create_space()` — Deploy

```python
def create_space(
    config: dict,
    display_name: str,
    warehouse_id: str,
    parent_path: str,
) -> dict:
    """Deploy a config to Databricks as a new Genie Space."""
```

### Individual generators — Fine-grained control

All section generators are importable from `genie_world.builder`:
- `generate_data_sources(profile)`
- `generate_join_specs(profile)`
- `generate_snippets(profile)`
- `generate_example_sqls(profile, join_specs, snippets, warehouse_id, count)`
- `generate_benchmarks(profile, join_specs, snippets, examples, warehouse_id, count)`
- `generate_instructions(profile, join_specs, snippets, examples)`
- `assemble_space(data_sources, join_specs, instructions, snippets, examples, benchmarks, ...)`

## Error Handling

- **No warehouse_id** — `build_space()` generates everything but skips SQL validation. Logs warning: "SQL validation skipped — no warehouse_id provided."
- **SQL validation failure after 3 retries** — includes the failing SQL in the config with a `ProfilingWarning`-style warning. User can fix manually.
- **LLM errors** — each generator catches LLM errors and returns empty/partial results with warnings. The assembler handles missing sections gracefully.
- **Deployment errors** — mapped to user-friendly exceptions (PermissionError, ValueError, TimeoutError).

## Testing Strategy

- **Unit tests**: Mock LLM responses and SQL execution. Verify each generator produces correctly structured output. Test assembler validation and constraint enforcement.
- **Integration tests**: Generate a config from a real profile (e.g., `samples.tpch`), deploy it, verify the space is accessible. Gated behind `RUN_INTEGRATION_TESTS=1`.

## Usage Examples

### Full auto (notebook)

```python
from genie_world.profiler import profile_schema
from genie_world.builder import build_space, create_space

profile = profile_schema("my_catalog", "my_schema", deep=True, synonyms=True, warehouse_id="wh-123")
config = build_space(profile, warehouse_id="wh-123")
result = create_space(config, "Sales Analytics", "wh-123", "/Workspace/Users/me/")
print(f"Space: {result['space_url']}")
```

### Fine-grained control

```python
from genie_world.builder import (
    generate_data_sources, generate_join_specs, generate_snippets,
    generate_example_sqls, generate_benchmarks, generate_instructions,
    assemble_space, create_space,
)

data_sources = generate_data_sources(profile)
join_specs = generate_join_specs(profile)
snippets = generate_snippets(profile)
examples = generate_example_sqls(profile, join_specs, snippets, warehouse_id="wh-123")
benchmarks = generate_benchmarks(profile, join_specs, snippets, examples, warehouse_id="wh-123")
instructions = generate_instructions(profile, join_specs, snippets, examples)

# Customize
instructions[0]["content"].append("Always use fiscal quarters, not calendar quarters.")

config = assemble_space(data_sources, join_specs, instructions, snippets, examples, benchmarks)
result = create_space(config, "Sales Analytics", "wh-123", "/Workspace/Users/me/")
```

## Baseline References

- **dbx-genie-rx `genie_creator.py`** — deployer logic, constraint enforcement, string array handling, sorting rules
- **dbx-genie-rx `docs/genie-space-schema.md`** — complete schema reference with validation rules
- **dbx-genie-rx `docs/checklist-by-schema.md`** — best practices for each config section (informs LLM prompts)
