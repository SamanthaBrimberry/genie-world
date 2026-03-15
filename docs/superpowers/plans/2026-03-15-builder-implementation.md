# Space Builder Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Space Builder block that generates complete Genie Space configurations from profiler output, including a profiler enhancement for missing descriptions.

**Architecture:** Section generators + assembler pattern. Deterministic generators for data_sources and join_specs (no LLM). LLM generators for snippets, example SQLs, benchmarks, and instructions. SQL validator with 3x retry. Assembler enforces Genie Space schema constraints. Deployer creates spaces via API.

**Tech Stack:** Python 3.10+, pydantic 2.x, databricks-sdk, pytest. Existing core modules: auth, config, llm, sql, storage, tracing.

**Spec:** `docs/superpowers/specs/2026-03-15-builder-design.md`

---

## File Structure

### Profiler Enhancement
| File | Purpose |
|------|---------|
| `genie_world/profiler/description_enricher.py` | LLM-powered missing description generation |
| `tests/unit/profiler/test_description_enricher.py` | Tests for description enricher |

### Builder (`genie_world/builder/`)
| File | Purpose |
|------|---------|
| `genie_world/builder/__init__.py` | Public API: build_space(), BuildResult, BuilderWarning |
| `genie_world/builder/data_sources.py` | Deterministic: profile → tables + column_configs |
| `genie_world/builder/join_specs.py` | Deterministic: relationships → join SQL |
| `genie_world/builder/snippets.py` | LLM: filters, expressions, measures |
| `genie_world/builder/example_sqls.py` | LLM + validate: example Q&A pairs |
| `genie_world/builder/benchmarks.py` | LLM + validate: benchmark questions |
| `genie_world/builder/sql_validator.py` | Execute SQL, LLM fix, retry up to 3x |
| `genie_world/builder/instructions.py` | LLM: text instructions (generated last) |
| `genie_world/builder/assembler.py` | Combine sections, IDs, constraints, sorting |
| `genie_world/builder/deployer.py` | Create space via Databricks API |

### Tests
| File | Purpose |
|------|---------|
| `tests/unit/builder/__init__.py` | Builder test package |
| `tests/unit/builder/test_data_sources.py` | Data sources generator tests |
| `tests/unit/builder/test_join_specs.py` | Join specs generator tests |
| `tests/unit/builder/test_snippets.py` | Snippets generator tests |
| `tests/unit/builder/test_example_sqls.py` | Example SQL generator tests |
| `tests/unit/builder/test_benchmarks.py` | Benchmarks generator tests |
| `tests/unit/builder/test_sql_validator.py` | SQL validator tests |
| `tests/unit/builder/test_instructions.py` | Instructions generator tests |
| `tests/unit/builder/test_assembler.py` | Assembler tests |
| `tests/unit/builder/test_deployer.py` | Deployer tests |
| `tests/unit/builder/test_public_api.py` | build_space() orchestration tests |

---

## Chunk 1: Profiler Enhancement + Builder Foundation

### Task 1: Description Enricher (Profiler Enhancement)

**Files:**
- Create: `genie_world/profiler/description_enricher.py`
- Create: `tests/unit/profiler/test_description_enricher.py`
- Modify: `genie_world/profiler/__init__.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/profiler/test_description_enricher.py
import json
import pytest
from unittest.mock import patch
from genie_world.profiler.models import ColumnProfile, TableProfile, ProfilingWarning
from genie_world.profiler.description_enricher import enrich_descriptions_for_table


class TestEnrichDescriptionsForTable:
    @patch("genie_world.profiler.description_enricher.call_llm")
    def test_fills_missing_descriptions(self, mock_llm):
        mock_llm.return_value = json.dumps({
            "table_description": "Customer order records",
            "columns": {
                "id": "Unique order identifier",
                "amount": "Total order value in USD"
            }
        })

        table = TableProfile(
            catalog="main", schema_name="sales", table="orders",
            description=None,
            columns=[
                ColumnProfile(name="id", data_type="INT", nullable=False),
                ColumnProfile(name="amount", data_type="DOUBLE", nullable=True),
            ],
        )

        enriched, warnings = enrich_descriptions_for_table(table)

        assert enriched.description == "Customer order records"
        assert enriched.columns[0].description == "Unique order identifier"
        assert enriched.columns[1].description == "Total order value in USD"
        assert len(warnings) == 0

    @patch("genie_world.profiler.description_enricher.call_llm")
    def test_preserves_existing_descriptions(self, mock_llm):
        mock_llm.return_value = json.dumps({
            "table_description": "LLM generated description",
            "columns": {
                "id": "LLM generated",
                "name": "LLM generated"
            }
        })

        table = TableProfile(
            catalog="main", schema_name="sales", table="customers",
            description="Existing table description",
            columns=[
                ColumnProfile(name="id", data_type="INT", nullable=False, description="Existing col desc"),
                ColumnProfile(name="name", data_type="STRING", nullable=True),
            ],
        )

        enriched, warnings = enrich_descriptions_for_table(table)

        # Existing descriptions should be preserved
        assert enriched.description == "Existing table description"
        assert enriched.columns[0].description == "Existing col desc"
        # Missing description should be filled
        assert enriched.columns[1].description == "LLM generated"

    @patch("genie_world.profiler.description_enricher.call_llm")
    def test_skips_table_with_all_descriptions(self, mock_llm):
        table = TableProfile(
            catalog="main", schema_name="sales", table="orders",
            description="Has description",
            columns=[
                ColumnProfile(name="id", data_type="INT", nullable=False, description="Has desc"),
            ],
        )

        enriched, warnings = enrich_descriptions_for_table(table)

        # Should not call LLM at all
        mock_llm.assert_not_called()
        assert enriched.description == "Has description"

    @patch("genie_world.profiler.description_enricher.call_llm")
    def test_handles_llm_error(self, mock_llm):
        mock_llm.side_effect = RuntimeError("LLM unavailable")

        table = TableProfile(
            catalog="main", schema_name="sales", table="orders",
            description=None,
            columns=[ColumnProfile(name="id", data_type="INT", nullable=False)],
        )

        enriched, warnings = enrich_descriptions_for_table(table)

        assert enriched.description is None  # unchanged
        assert len(warnings) == 1
        assert "LLM unavailable" in warnings[0].message
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source .venv/bin/activate && pytest tests/unit/profiler/test_description_enricher.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement description enricher**

```python
# genie_world/profiler/description_enricher.py
"""LLM-powered description generation for tables and columns with missing metadata."""

from __future__ import annotations

import logging

from genie_world.core.llm import call_llm, parse_json_from_llm_response
from genie_world.core.tracing import trace
from genie_world.profiler.models import ColumnProfile, ProfilingWarning, TableProfile

logger = logging.getLogger(__name__)


def _needs_enrichment(table: TableProfile) -> bool:
    """Check if a table has any missing descriptions."""
    if not table.description:
        return True
    return any(col.description is None for col in table.columns)


def _build_description_prompt(table: TableProfile) -> list[dict]:
    """Build LLM prompt to generate missing descriptions."""
    missing_cols = [col for col in table.columns if col.description is None]
    col_info = "\n".join(
        f"  - {col.name} ({col.data_type})"
        + (f" — samples: {', '.join(col.sample_values[:3])}" if col.sample_values else "")
        for col in missing_cols
    )

    needs_table_desc = "YES — generate a table_description" if not table.description else "NO — table already has a description"

    system_msg = (
        "You are a data catalog expert. Generate clear, concise descriptions for "
        "database tables and columns based on their names, types, and context. "
        "Return ONLY valid JSON — no prose, no markdown."
    )

    user_msg = (
        f"Table: {table.catalog}.{table.schema_name}.{table.table}\n"
        f"Table description needed: {needs_table_desc}\n\n"
        f"Columns needing descriptions:\n{col_info}\n\n"
        "Return a JSON object with:\n"
        '- "table_description": string (or null if not needed)\n'
        '- "columns": {{"column_name": "description", ...}}\n\n'
        "Keep descriptions concise (1-2 sentences). Focus on business meaning."
    )

    return [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": user_msg},
    ]


@trace(name="enrich_descriptions_for_table", span_type="CHAIN")
def enrich_descriptions_for_table(
    table: TableProfile,
    model: str | None = None,
) -> tuple[TableProfile, list[ProfilingWarning]]:
    """Fill in missing table and column descriptions via LLM.

    Preserves existing descriptions. Only calls LLM if there are gaps.

    Returns (enriched_table, warnings).
    """
    full_name = f"{table.catalog}.{table.schema_name}.{table.table}"
    warnings: list[ProfilingWarning] = []

    if not _needs_enrichment(table):
        return table, warnings

    prompt = _build_description_prompt(table)

    try:
        raw = call_llm(prompt, model=model)
        result = parse_json_from_llm_response(raw)
    except Exception as exc:
        logger.warning("Description enrichment failed for %s: %s", full_name, exc)
        warnings.append(
            ProfilingWarning(table=full_name, tier="descriptions", message=str(exc))
        )
        return table, warnings

    # Apply table description if missing
    table_desc = table.description
    if not table_desc and result.get("table_description"):
        table_desc = result["table_description"]

    # Apply column descriptions where missing
    col_descs = result.get("columns", {})
    enriched_columns = []
    for col in table.columns:
        if col.description is None and col.name in col_descs:
            enriched_columns.append(
                col.model_copy(update={"description": col_descs[col.name]})
            )
        else:
            enriched_columns.append(col)

    return table.model_copy(update={
        "description": table_desc,
        "columns": enriched_columns,
    }), warnings
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source .venv/bin/activate && pytest tests/unit/profiler/test_description_enricher.py -v`
Expected: 4 passed.

- [ ] **Step 5: Wire into profile_schema()**

Add `enrich_descriptions` parameter to `profile_schema()` and `profile_tables()` in `genie_world/profiler/__init__.py`. Add it after synonyms in the generation order. Import `enrich_descriptions_for_table` and call it for each table when the flag is True.

- [ ] **Step 6: Run full test suite**

Run: `source .venv/bin/activate && pytest tests/ -v --tb=short`
Expected: All tests pass (101 existing + 4 new).

- [ ] **Step 7: Commit**

```bash
git add genie_world/profiler/description_enricher.py tests/unit/profiler/test_description_enricher.py genie_world/profiler/__init__.py
git commit -m "feat(profiler): add LLM description enrichment for sparse metadata"
```

---

### Task 2: Builder Scaffolding + Models

**Files:**
- Create: `genie_world/builder/__init__.py`
- Create: `tests/unit/builder/__init__.py`

- [ ] **Step 1: Create builder package with BuilderWarning and BuildResult**

```python
# genie_world/builder/__init__.py
"""Space Builder block for genie-world.

Generates complete Genie Space configurations from SchemaProfile data.
"""

from __future__ import annotations

from pydantic import BaseModel


class BuilderWarning(BaseModel):
    """Warning generated during space building."""
    section: str
    message: str
    detail: str | None = None


class BuildResult(BaseModel):
    """Result from build_space() containing config and any warnings."""
    config: dict
    warnings: list[BuilderWarning]
```

Create `tests/unit/builder/__init__.py` as empty file.

- [ ] **Step 2: Write test for models**

```python
# tests/unit/builder/test_models.py
from genie_world.builder import BuilderWarning, BuildResult


class TestBuilderModels:
    def test_builder_warning(self):
        w = BuilderWarning(section="example_sqls", message="SQL validation failed", detail="SELECT bad")
        assert w.section == "example_sqls"

    def test_build_result(self):
        r = BuildResult(config={"version": 2}, warnings=[])
        assert r.config["version"] == 2
        assert len(r.warnings) == 0
```

- [ ] **Step 3: Run tests**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_models.py -v`
Expected: 2 passed.

- [ ] **Step 4: Commit**

```bash
git add genie_world/builder/ tests/unit/builder/
git commit -m "feat(builder): scaffold builder package with BuilderWarning and BuildResult"
```

---

### Task 3: Data Sources Generator (Deterministic)

**Files:**
- Create: `genie_world/builder/data_sources.py`
- Create: `tests/unit/builder/test_data_sources.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/builder/test_data_sources.py
from genie_world.profiler.models import ColumnProfile, SchemaProfile, TableProfile
from genie_world.builder.data_sources import generate_data_sources
from datetime import datetime


def _make_profile():
    return SchemaProfile(
        schema_version="1.0",
        catalog="main",
        schema_name="sales",
        tables=[
            TableProfile(
                catalog="main", schema_name="sales", table="orders",
                description="Customer orders",
                columns=[
                    ColumnProfile(name="order_id", data_type="STRING", nullable=False, description="Unique ID"),
                    ColumnProfile(name="order_date", data_type="TIMESTAMP", nullable=True, description="When placed"),
                    ColumnProfile(name="status", data_type="STRING", nullable=True, cardinality=5, synonyms=["state", "order status"]),
                    ColumnProfile(name="amount", data_type="DOUBLE", nullable=True),
                    ColumnProfile(name="_metadata", data_type="STRING", nullable=True),
                ],
            ),
        ],
        relationships=[],
        profiled_at=datetime(2026, 3, 15),
    )


class TestGenerateDataSources:
    def test_basic_structure(self):
        profile = _make_profile()
        result = generate_data_sources(profile)

        assert "tables" in result
        assert len(result["tables"]) == 1
        table = result["tables"][0]
        assert table["identifier"] == "main.sales.orders"
        assert table["description"] == ["Customer orders"]

    def test_column_configs_sorted(self):
        profile = _make_profile()
        result = generate_data_sources(profile)
        col_names = [c["column_name"] for c in result["tables"][0]["column_configs"]]
        assert col_names == sorted(col_names)

    def test_entity_matching_for_low_cardinality_string(self):
        profile = _make_profile()
        result = generate_data_sources(profile)
        configs = {c["column_name"]: c for c in result["tables"][0]["column_configs"]}
        assert configs["status"].get("enable_entity_matching") is True

    def test_format_assistance_for_timestamp(self):
        profile = _make_profile()
        result = generate_data_sources(profile)
        configs = {c["column_name"]: c for c in result["tables"][0]["column_configs"]}
        assert configs["order_date"].get("enable_format_assistance") is True

    def test_excludes_internal_columns(self):
        profile = _make_profile()
        result = generate_data_sources(profile)
        configs = {c["column_name"]: c for c in result["tables"][0]["column_configs"]}
        assert configs["_metadata"].get("exclude") is True

    def test_synonyms_included(self):
        profile = _make_profile()
        result = generate_data_sources(profile)
        configs = {c["column_name"]: c for c in result["tables"][0]["column_configs"]}
        assert configs["status"]["synonyms"] == ["state", "order status"]

    def test_tables_sorted_by_identifier(self):
        profile = SchemaProfile(
            schema_version="1.0", catalog="main", schema_name="s",
            tables=[
                TableProfile(catalog="main", schema_name="s", table="zebra", columns=[]),
                TableProfile(catalog="main", schema_name="s", table="alpha", columns=[]),
            ],
            relationships=[], profiled_at=datetime(2026, 3, 15),
        )
        result = generate_data_sources(profile)
        ids = [t["identifier"] for t in result["tables"]]
        assert ids == ["main.s.alpha", "main.s.zebra"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_data_sources.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement data sources generator**

```python
# genie_world/builder/data_sources.py
"""Deterministic generator: SchemaProfile → data_sources config section."""

from __future__ import annotations

from genie_world.profiler.models import ColumnProfile, SchemaProfile

_INTERNAL_COLUMN_PATTERNS = {"_metadata", "_rescued_data", "_commit_version", "_commit_timestamp"}
_DATE_TYPES = {"DATE", "TIMESTAMP"}
_STRING_TYPES = {"STRING", "CHAR", "VARCHAR"}
_ENTITY_MATCHING_CARDINALITY_THRESHOLD = 100


def generate_data_sources(profile: SchemaProfile) -> dict:
    """Transform a SchemaProfile into the data_sources config section.

    Produces table entries with column_configs including descriptions,
    synonyms, entity matching, format assistance, and exclusions.
    Tables sorted by identifier, column_configs sorted by column_name.
    """
    tables = []

    for table in profile.tables:
        identifier = f"{table.catalog}.{table.schema_name}.{table.table}"

        column_configs = []
        for col in table.columns:
            config: dict = {"column_name": col.name}

            if col.description:
                config["description"] = [col.description]

            if col.synonyms:
                config["synonyms"] = col.synonyms

            # Entity matching: string columns with low cardinality and synonyms
            is_string = col.data_type.upper() in _STRING_TYPES
            low_card = col.cardinality is not None and col.cardinality < _ENTITY_MATCHING_CARDINALITY_THRESHOLD
            if is_string and (low_card or col.synonyms):
                config["enable_entity_matching"] = True

            # Format assistance: date/timestamp columns and low-cardinality strings
            is_date = col.data_type.upper() in _DATE_TYPES
            if is_date or (is_string and low_card):
                config["enable_format_assistance"] = True

            # Exclude internal columns
            if col.name.lower() in _INTERNAL_COLUMN_PATTERNS or col.name.startswith("_"):
                config["exclude"] = True

            column_configs.append(config)

        # Sort column_configs by column_name
        column_configs.sort(key=lambda c: c["column_name"])

        entry: dict = {"identifier": identifier, "column_configs": column_configs}
        if table.description:
            entry["description"] = [table.description]

        tables.append(entry)

    # Sort tables by identifier
    tables.sort(key=lambda t: t["identifier"])

    return {"tables": tables}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_data_sources.py -v`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/builder/data_sources.py tests/unit/builder/test_data_sources.py
git commit -m "feat(builder): add deterministic data_sources generator"
```

---

### Task 4: Join Specs Generator (Deterministic)

**Files:**
- Create: `genie_world/builder/join_specs.py`
- Create: `tests/unit/builder/test_join_specs.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/builder/test_join_specs.py
from datetime import datetime
from genie_world.profiler.models import DetectionMethod, Relationship, SchemaProfile, TableProfile
from genie_world.builder.join_specs import generate_join_specs


def _make_profile_with_rels():
    return SchemaProfile(
        schema_version="1.0", catalog="main", schema_name="sales",
        tables=[
            TableProfile(catalog="main", schema_name="sales", table="orders", columns=[]),
            TableProfile(catalog="main", schema_name="sales", table="customers", columns=[]),
        ],
        relationships=[
            Relationship(
                source_table="main.sales.orders", source_column="customer_id",
                target_table="main.sales.customers", target_column="id",
                confidence=0.7, detection_method=DetectionMethod.NAMING_PATTERN,
            ),
            Relationship(
                source_table="main.sales.orders", source_column="internal_ref",
                target_table="main.sales.logs", target_column="ref_id",
                confidence=0.3, detection_method=DetectionMethod.VALUE_OVERLAP,
            ),
        ],
        profiled_at=datetime(2026, 3, 15),
    )


class TestGenerateJoinSpecs:
    def test_generates_join_for_high_confidence(self):
        profile = _make_profile_with_rels()
        specs = generate_join_specs(profile)

        assert len(specs) == 1  # Only confidence >= 0.6
        spec = specs[0]
        assert spec["left"]["identifier"] == "main.sales.orders"
        assert spec["right"]["identifier"] == "main.sales.customers"
        assert "orders.customer_id = customers.id" in spec["sql"][0]

    def test_filters_low_confidence(self):
        profile = _make_profile_with_rels()
        specs = generate_join_specs(profile)
        # Low confidence relationship (0.3) should be excluded
        assert all("internal_ref" not in str(s) for s in specs)

    def test_includes_comment_and_instruction(self):
        profile = _make_profile_with_rels()
        specs = generate_join_specs(profile)
        spec = specs[0]
        assert "comment" in spec
        assert "instruction" in spec
        assert isinstance(spec["comment"], list)
        assert isinstance(spec["instruction"], list)

    def test_empty_relationships(self):
        profile = SchemaProfile(
            schema_version="1.0", catalog="main", schema_name="s",
            tables=[], relationships=[],
            profiled_at=datetime(2026, 3, 15),
        )
        specs = generate_join_specs(profile)
        assert specs == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_join_specs.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement join specs generator**

```python
# genie_world/builder/join_specs.py
"""Deterministic generator: relationships → join_specs config section."""

from __future__ import annotations

from genie_world.profiler.models import SchemaProfile

MIN_CONFIDENCE = 0.6


def _short_name(full_table: str) -> str:
    """Extract short table name from fully qualified name."""
    parts = full_table.split(".")
    return parts[-1] if parts else full_table


def generate_join_specs(profile: SchemaProfile) -> list[dict]:
    """Transform high-confidence relationships into join_specs.

    Only includes relationships with confidence >= 0.6.
    Generates SQL join conditions, comments, and instructions.
    """
    specs = []

    for rel in profile.relationships:
        if rel.confidence < MIN_CONFIDENCE:
            continue

        left_alias = _short_name(rel.source_table)
        right_alias = _short_name(rel.target_table)

        confidence_label = "high" if rel.confidence >= 0.9 else "medium"
        instruction_strength = (
            f"Always use this join when combining {left_alias} and {right_alias} data."
            if rel.confidence >= 0.9
            else f"Use this join when {right_alias} attributes are needed for {left_alias} analysis."
        )

        specs.append({
            "left": {"identifier": rel.source_table, "alias": left_alias},
            "right": {"identifier": rel.target_table, "alias": right_alias},
            "sql": [f"{left_alias}.{rel.source_column} = {right_alias}.{rel.target_column}"],
            "comment": [f"Join {left_alias} to {right_alias} on {rel.source_column} ({confidence_label} confidence, detected via {rel.detection_method.value})."],
            "instruction": [instruction_strength],
        })

    return specs
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_join_specs.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/builder/join_specs.py tests/unit/builder/test_join_specs.py
git commit -m "feat(builder): add deterministic join_specs generator"
```

---

## Chunk 2: SQL Validator + LLM Generators

### Task 5: SQL Validator

**Files:**
- Create: `genie_world/builder/sql_validator.py`
- Create: `tests/unit/builder/test_sql_validator.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/builder/test_sql_validator.py
import pytest
from unittest.mock import patch, MagicMock
from genie_world.profiler.models import SchemaProfile, TableProfile, ColumnProfile
from genie_world.builder.sql_validator import validate_and_fix_sql
from datetime import datetime


def _make_simple_profile():
    return SchemaProfile(
        schema_version="1.0", catalog="main", schema_name="s",
        tables=[TableProfile(
            catalog="main", schema_name="s", table="orders",
            columns=[ColumnProfile(name="id", data_type="INT", nullable=False)],
        )],
        relationships=[], profiled_at=datetime(2026, 3, 15),
    )


class TestValidateAndFixSql:
    @patch("genie_world.builder.sql_validator.execute_sql")
    def test_returns_valid_sql_unchanged(self, mock_exec):
        mock_exec.return_value = {"error": None, "row_count": 5, "columns": [], "data": [], "truncated": False}
        profile = _make_simple_profile()

        sql, warnings = validate_and_fix_sql("SELECT * FROM orders", "What are the orders?", profile, "wh-123")

        assert sql == "SELECT * FROM orders"
        assert len(warnings) == 0

    @patch("genie_world.builder.sql_validator.call_llm")
    @patch("genie_world.builder.sql_validator.execute_sql")
    def test_retries_on_failure(self, mock_exec, mock_llm):
        # First call fails, second succeeds
        mock_exec.side_effect = [
            {"error": "Column not found: bad_col", "row_count": 0, "columns": [], "data": [], "truncated": False},
            {"error": None, "row_count": 3, "columns": [], "data": [], "truncated": False},
        ]
        mock_llm.return_value = "SELECT id FROM orders"
        profile = _make_simple_profile()

        sql, warnings = validate_and_fix_sql("SELECT bad_col FROM orders", "Show orders", profile, "wh-123")

        assert sql == "SELECT id FROM orders"
        assert len(warnings) == 0

    @patch("genie_world.builder.sql_validator.call_llm")
    @patch("genie_world.builder.sql_validator.execute_sql")
    def test_gives_up_after_max_retries(self, mock_exec, mock_llm):
        mock_exec.return_value = {"error": "Syntax error", "row_count": 0, "columns": [], "data": [], "truncated": False}
        mock_llm.return_value = "SELECT still_bad FROM orders"
        profile = _make_simple_profile()

        sql, warnings = validate_and_fix_sql("SELECT bad FROM orders", "Show orders", profile, "wh-123", max_retries=3)

        assert len(warnings) == 1
        assert "Syntax error" in warnings[0]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_sql_validator.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement SQL validator**

```python
# genie_world/builder/sql_validator.py
"""SQL validation with LLM-powered fix and retry."""

from __future__ import annotations

import logging

from genie_world.core.llm import call_llm
from genie_world.core.sql import execute_sql
from genie_world.core.tracing import trace
from genie_world.profiler.models import SchemaProfile

logger = logging.getLogger(__name__)


def _build_fix_prompt(
    sql: str, error: str, question: str, profile: SchemaProfile
) -> list[dict]:
    """Build prompt asking LLM to fix a failing SQL query."""
    tables_info = "\n".join(
        f"  {t.catalog}.{t.schema_name}.{t.table}: "
        + ", ".join(f"{c.name} ({c.data_type})" for c in t.columns)
        for t in profile.tables
    )

    return [
        {"role": "system", "content": (
            "You are a SQL expert. Fix the SQL query based on the error. "
            "Return ONLY the corrected SQL — no explanation, no markdown."
        )},
        {"role": "user", "content": (
            f"Question: {question}\n\n"
            f"SQL:\n{sql}\n\n"
            f"Error:\n{error}\n\n"
            f"Available tables and columns:\n{tables_info}\n\n"
            "Return the corrected SQL only."
        )},
    ]


@trace(name="validate_and_fix_sql", span_type="CHAIN")
def validate_and_fix_sql(
    sql: str,
    question: str,
    profile: SchemaProfile,
    warehouse_id: str,
    max_retries: int = 3,
) -> tuple[str, list[str]]:
    """Execute SQL, retry with LLM fix on failure.

    Returns (final_sql, list_of_warning_strings).
    """
    warnings: list[str] = []
    current_sql = sql

    for attempt in range(1 + max_retries):
        result = execute_sql(current_sql, warehouse_id=warehouse_id)

        if result["error"] is None:
            return current_sql, warnings

        error_msg = result["error"]
        logger.info(f"SQL validation attempt {attempt + 1} failed: {error_msg}")

        if attempt < max_retries:
            # Ask LLM to fix
            try:
                prompt = _build_fix_prompt(current_sql, error_msg, question, profile)
                fixed = call_llm(prompt)
                current_sql = fixed.strip()
            except Exception as e:
                logger.warning(f"LLM fix attempt failed: {e}")
                warnings.append(f"LLM fix failed: {e}")
                break

    # All retries exhausted
    warnings.append(f"SQL validation failed after {max_retries} retries: {error_msg}")
    return current_sql, warnings
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_sql_validator.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/builder/sql_validator.py tests/unit/builder/test_sql_validator.py
git commit -m "feat(builder): add SQL validator with LLM fix and 3x retry"
```

---

### Task 6: Snippets Generator (LLM)

**Files:**
- Create: `genie_world/builder/snippets.py`
- Create: `tests/unit/builder/test_snippets.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/builder/test_snippets.py
import json
import pytest
from unittest.mock import patch
from datetime import datetime
from genie_world.profiler.models import ColumnProfile, SchemaProfile, TableProfile
from genie_world.builder.snippets import generate_snippets


def _make_profile():
    return SchemaProfile(
        schema_version="1.0", catalog="main", schema_name="sales",
        tables=[TableProfile(
            catalog="main", schema_name="sales", table="orders",
            columns=[
                ColumnProfile(name="order_date", data_type="TIMESTAMP", nullable=True),
                ColumnProfile(name="amount", data_type="DOUBLE", nullable=True),
                ColumnProfile(name="status", data_type="STRING", nullable=True, cardinality=5,
                              top_values=["active", "completed", "cancelled"]),
            ],
        )],
        relationships=[], profiled_at=datetime(2026, 3, 15),
    )


class TestGenerateSnippets:
    @patch("genie_world.builder.snippets.call_llm")
    def test_returns_all_three_sections(self, mock_llm):
        mock_llm.return_value = json.dumps({
            "filters": [
                {"sql": "status = 'active'", "display_name": "active orders",
                 "synonyms": ["current orders"], "comment": "Active only", "instruction": "Use for active"}
            ],
            "expressions": [
                {"alias": "order_year", "sql": "YEAR(order_date)", "display_name": "year",
                 "synonyms": ["fiscal year"], "comment": "Extract year", "instruction": "Year analysis"}
            ],
            "measures": [
                {"alias": "total_revenue", "sql": "SUM(amount)", "display_name": "total revenue",
                 "synonyms": ["revenue"], "comment": "Sum amounts", "instruction": "Revenue calc"}
            ],
        })

        result = generate_snippets(_make_profile())

        assert "filters" in result
        assert "expressions" in result
        assert "measures" in result
        assert len(result["filters"]) == 1
        assert result["expressions"][0]["alias"] == "order_year"
        assert result["measures"][0]["alias"] == "total_revenue"

    @patch("genie_world.builder.snippets.call_llm")
    def test_handles_llm_error(self, mock_llm):
        mock_llm.side_effect = RuntimeError("LLM unavailable")

        result = generate_snippets(_make_profile())

        assert result == {"filters": [], "expressions": [], "measures": []}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_snippets.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement snippets generator**

```python
# genie_world/builder/snippets.py
"""LLM generator: SQL snippets (filters, expressions, measures)."""

from __future__ import annotations

import logging

from genie_world.core.llm import call_llm, parse_json_from_llm_response
from genie_world.core.tracing import trace
from genie_world.profiler.models import SchemaProfile

logger = logging.getLogger(__name__)

_EMPTY_SNIPPETS = {"filters": [], "expressions": [], "measures": []}


def _build_snippets_prompt(profile: SchemaProfile) -> list[dict]:
    """Build prompt for generating SQL snippets."""
    tables_info = []
    for t in profile.tables:
        cols = "\n".join(
            f"    - {c.name} ({c.data_type})"
            + (f" — top values: {', '.join(c.top_values[:5])}" if c.top_values else "")
            + (f" — {c.description}" if c.description else "")
            for c in t.columns
        )
        tables_info.append(f"  {t.table}:\n{cols}")

    tables_text = "\n".join(tables_info)

    system_msg = (
        "You are a Databricks Genie Space configuration expert. Generate SQL snippets "
        "that help Genie answer common business questions. Return ONLY valid JSON."
    )

    user_msg = (
        f"Tables:\n{tables_text}\n\n"
        "Generate SQL snippets in three categories:\n\n"
        "1. **filters**: Common WHERE clause conditions (e.g., date ranges, status filters)\n"
        "   Each: {sql, display_name, synonyms: [...], comment, instruction}\n\n"
        "2. **expressions**: Reusable calculated columns (e.g., YEAR(date), category buckets)\n"
        "   Each: {alias, sql, display_name, synonyms: [...], comment, instruction}\n\n"
        "3. **measures**: Standard aggregations (e.g., SUM, COUNT DISTINCT)\n"
        "   Each: {alias, sql, display_name, synonyms: [...], comment, instruction}\n\n"
        "Generate 2-4 items per category. Use actual table and column names from above.\n\n"
        'Return: {"filters": [...], "expressions": [...], "measures": [...]}'
    )

    return [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": user_msg},
    ]


@trace(name="generate_snippets", span_type="CHAIN")
def generate_snippets(profile: SchemaProfile) -> dict:
    """Generate SQL snippet configs via LLM.

    Returns {"filters": [...], "expressions": [...], "measures": [...]}.
    On LLM error, returns empty snippets.
    """
    prompt = _build_snippets_prompt(profile)

    try:
        raw = call_llm(prompt)
        result = parse_json_from_llm_response(raw)
    except Exception as exc:
        logger.warning("Snippet generation failed: %s", exc)
        return dict(_EMPTY_SNIPPETS)

    # Validate structure
    return {
        "filters": result.get("filters", []) if isinstance(result.get("filters"), list) else [],
        "expressions": result.get("expressions", []) if isinstance(result.get("expressions"), list) else [],
        "measures": result.get("measures", []) if isinstance(result.get("measures"), list) else [],
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_snippets.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/builder/snippets.py tests/unit/builder/test_snippets.py
git commit -m "feat(builder): add LLM-powered snippets generator"
```

---

### Task 7: Example SQLs Generator (LLM + Validate)

**Files:**
- Create: `genie_world/builder/example_sqls.py`
- Create: `tests/unit/builder/test_example_sqls.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/builder/test_example_sqls.py
import json
import pytest
from unittest.mock import patch
from datetime import datetime
from genie_world.profiler.models import ColumnProfile, SchemaProfile, TableProfile
from genie_world.builder.example_sqls import generate_example_sqls


def _make_profile():
    return SchemaProfile(
        schema_version="1.0", catalog="main", schema_name="sales",
        tables=[TableProfile(
            catalog="main", schema_name="sales", table="orders",
            description="Order records",
            columns=[
                ColumnProfile(name="id", data_type="INT", nullable=False),
                ColumnProfile(name="amount", data_type="DOUBLE", nullable=True),
            ],
        )],
        relationships=[], profiled_at=datetime(2026, 3, 15),
    )


class TestGenerateExampleSqls:
    @patch("genie_world.builder.example_sqls.call_llm")
    def test_generates_examples(self, mock_llm):
        mock_llm.return_value = json.dumps([
            {"question": "Total orders", "sql": "SELECT COUNT(*) FROM main.sales.orders"},
            {"question": "Average amount", "sql": "SELECT AVG(amount) FROM main.sales.orders"},
        ])

        examples, warnings = generate_example_sqls(_make_profile(), [], {}, count=2)

        assert len(examples) == 2
        assert examples[0]["question"] == "Total orders"
        assert examples[0]["sql"] == "SELECT COUNT(*) FROM main.sales.orders"

    @patch("genie_world.builder.example_sqls.validate_and_fix_sql")
    @patch("genie_world.builder.example_sqls.call_llm")
    def test_validates_with_warehouse(self, mock_llm, mock_validate):
        mock_llm.return_value = json.dumps([
            {"question": "Total orders", "sql": "SELECT COUNT(*) FROM orders"},
        ])
        mock_validate.return_value = ("SELECT COUNT(*) FROM main.sales.orders", [])

        examples, warnings = generate_example_sqls(
            _make_profile(), [], {}, warehouse_id="wh-123", count=1
        )

        mock_validate.assert_called_once()
        assert examples[0]["sql"] == "SELECT COUNT(*) FROM main.sales.orders"

    @patch("genie_world.builder.example_sqls.call_llm")
    def test_handles_llm_error(self, mock_llm):
        mock_llm.side_effect = RuntimeError("unavailable")

        examples, warnings = generate_example_sqls(_make_profile(), [], {}, count=5)

        assert examples == []
        assert len(warnings) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_example_sqls.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement example SQLs generator**

```python
# genie_world/builder/example_sqls.py
"""LLM generator: example question-SQL pairs with optional validation."""

from __future__ import annotations

import logging

from genie_world.builder.sql_validator import validate_and_fix_sql
from genie_world.core.llm import call_llm, parse_json_from_llm_response
from genie_world.core.tracing import trace
from genie_world.profiler.models import SchemaProfile

logger = logging.getLogger(__name__)


def _build_examples_prompt(
    profile: SchemaProfile,
    join_specs: list[dict],
    snippets: dict,
    count: int,
) -> list[dict]:
    """Build prompt for generating example Q&A pairs."""
    tables_info = "\n".join(
        f"  {t.catalog}.{t.schema_name}.{t.table}: "
        + ", ".join(f"{c.name} ({c.data_type})" for c in t.columns)
        for t in profile.tables
    )

    joins_info = "\n".join(
        f"  {j['left']['alias']}.{j['sql'][0]}"
        for j in join_specs
    ) if join_specs else "  (none)"

    system_msg = (
        "You are a Databricks SQL expert generating example question-SQL pairs for a Genie Space. "
        "Return ONLY a valid JSON array — no prose, no markdown."
    )

    user_msg = (
        f"Tables:\n{tables_info}\n\n"
        f"Available joins:\n{joins_info}\n\n"
        f"Generate {count} diverse example question-SQL pairs.\n"
        "Mix complexity: simple single-table, multi-table joins, aggregations, filters.\n"
        "Use fully-qualified table names in SQL.\n\n"
        "Return a JSON array of objects:\n"
        '[{"question": "...", "sql": "SELECT ...", "usage_guidance": "..."}]\n\n'
        "Include usage_guidance for complex examples only."
    )

    return [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": user_msg},
    ]


@trace(name="generate_example_sqls", span_type="CHAIN")
def generate_example_sqls(
    profile: SchemaProfile,
    join_specs: list[dict],
    snippets: dict,
    warehouse_id: str | None = None,
    count: int = 10,
) -> tuple[list[dict], list[str]]:
    """Generate example Q&A pairs with SQL, optionally validated.

    Returns (examples, warnings).
    """
    warnings: list[str] = []

    prompt = _build_examples_prompt(profile, join_specs, snippets, count)

    try:
        raw = call_llm(prompt)
        examples = parse_json_from_llm_response(raw)
        if isinstance(examples, dict):
            examples = examples.get("examples", [examples])
        if not isinstance(examples, list):
            examples = [examples]
    except Exception as exc:
        logger.warning("Example SQL generation failed: %s", exc)
        return [], [f"Example generation failed: {exc}"]

    # Validate SQL if warehouse available
    if warehouse_id:
        validated = []
        for ex in examples:
            sql = ex.get("sql", "")
            question = ex.get("question", "")
            fixed_sql, sql_warnings = validate_and_fix_sql(
                sql, question, profile, warehouse_id
            )
            ex["sql"] = fixed_sql
            warnings.extend(sql_warnings)
            validated.append(ex)
        examples = validated

    return examples, warnings
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_example_sqls.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/builder/example_sqls.py tests/unit/builder/test_example_sqls.py
git commit -m "feat(builder): add example SQLs generator with validation"
```

---

### Task 8: Benchmarks Generator (LLM + Validate)

**Files:**
- Create: `genie_world/builder/benchmarks.py`
- Create: `tests/unit/builder/test_benchmarks.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/builder/test_benchmarks.py
import json
import pytest
from unittest.mock import patch
from datetime import datetime
from genie_world.profiler.models import ColumnProfile, SchemaProfile, TableProfile
from genie_world.builder.benchmarks import generate_benchmarks


def _make_profile():
    return SchemaProfile(
        schema_version="1.0", catalog="main", schema_name="sales",
        tables=[TableProfile(
            catalog="main", schema_name="sales", table="orders",
            columns=[ColumnProfile(name="id", data_type="INT", nullable=False)],
        )],
        relationships=[], profiled_at=datetime(2026, 3, 15),
    )


class TestGenerateBenchmarks:
    @patch("genie_world.builder.benchmarks.call_llm")
    def test_generates_benchmarks(self, mock_llm):
        mock_llm.return_value = json.dumps([
            {"question": "How many orders?", "sql": "SELECT COUNT(*) FROM main.sales.orders"},
        ])

        existing_examples = [{"question": "Total revenue", "sql": "SELECT SUM(amount) FROM orders"}]
        result, warnings = generate_benchmarks(_make_profile(), [], {}, existing_examples, count=1)

        assert "questions" in result
        assert len(result["questions"]) == 1
        q = result["questions"][0]
        assert q["question"] == "How many orders?"
        assert q["answer"] == [{"format": "SQL", "content": ["SELECT COUNT(*) FROM main.sales.orders"]}]

    @patch("genie_world.builder.benchmarks.call_llm")
    def test_handles_llm_error(self, mock_llm):
        mock_llm.side_effect = RuntimeError("unavailable")

        result, warnings = generate_benchmarks(_make_profile(), [], {}, [], count=5)

        assert result == {"questions": []}
        assert len(warnings) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_benchmarks.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement benchmarks generator**

```python
# genie_world/builder/benchmarks.py
"""LLM generator: benchmark questions with SQL answers."""

from __future__ import annotations

import logging

from genie_world.builder.sql_validator import validate_and_fix_sql
from genie_world.core.llm import call_llm, parse_json_from_llm_response
from genie_world.core.tracing import trace
from genie_world.profiler.models import SchemaProfile

logger = logging.getLogger(__name__)


def _build_benchmarks_prompt(
    profile: SchemaProfile,
    join_specs: list[dict],
    snippets: dict,
    existing_examples: list[dict],
    count: int,
) -> list[dict]:
    """Build prompt for generating benchmark questions."""
    tables_info = "\n".join(
        f"  {t.catalog}.{t.schema_name}.{t.table}: "
        + ", ".join(f"{c.name} ({c.data_type})" for c in t.columns)
        for t in profile.tables
    )

    existing_qs = "\n".join(
        f"  - {ex.get('question', '')}"
        for ex in existing_examples
    ) if existing_examples else "  (none)"

    system_msg = (
        "You are a Databricks SQL expert generating benchmark questions for testing a Genie Space. "
        "Return ONLY a valid JSON array — no prose, no markdown."
    )

    user_msg = (
        f"Tables:\n{tables_info}\n\n"
        f"Existing example questions (DO NOT duplicate these):\n{existing_qs}\n\n"
        f"Generate {count} NEW benchmark questions that are DIFFERENT from the examples above.\n"
        "Include varied phrasings, edge cases, and ambiguous queries to test robustness.\n"
        "Use fully-qualified table names in SQL.\n\n"
        'Return: [{"question": "...", "sql": "SELECT ..."}]'
    )

    return [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": user_msg},
    ]


@trace(name="generate_benchmarks", span_type="CHAIN")
def generate_benchmarks(
    profile: SchemaProfile,
    join_specs: list[dict],
    snippets: dict,
    existing_examples: list[dict],
    warehouse_id: str | None = None,
    count: int = 10,
) -> tuple[dict, list[str]]:
    """Generate benchmark questions with SQL answers.

    Returns ({"questions": [...]}, warnings).
    """
    warnings: list[str] = []

    prompt = _build_benchmarks_prompt(profile, join_specs, snippets, existing_examples, count)

    try:
        raw = call_llm(prompt)
        items = parse_json_from_llm_response(raw)
        if isinstance(items, dict):
            items = items.get("questions", items.get("benchmarks", [items]))
        if not isinstance(items, list):
            items = [items]
    except Exception as exc:
        logger.warning("Benchmark generation failed: %s", exc)
        return {"questions": []}, [f"Benchmark generation failed: {exc}"]

    # Validate SQL if warehouse available
    if warehouse_id:
        for item in items:
            sql = item.get("sql", "")
            question = item.get("question", "")
            fixed_sql, sql_warnings = validate_and_fix_sql(
                sql, question, profile, warehouse_id
            )
            item["sql"] = fixed_sql
            warnings.extend(sql_warnings)

    # Convert to benchmark schema format
    questions = []
    for item in items:
        questions.append({
            "question": item.get("question", ""),
            "answer": [{"format": "SQL", "content": [item.get("sql", "")]}],
        })

    return {"questions": questions}, warnings
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_benchmarks.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/builder/benchmarks.py tests/unit/builder/test_benchmarks.py
git commit -m "feat(builder): add benchmarks generator with validation"
```

---

### Task 9: Instructions Generator (LLM, Last)

**Files:**
- Create: `genie_world/builder/instructions.py`
- Create: `tests/unit/builder/test_instructions.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/builder/test_instructions.py
import json
import pytest
from unittest.mock import patch
from datetime import datetime
from genie_world.profiler.models import ColumnProfile, SchemaProfile, TableProfile
from genie_world.builder.instructions import generate_instructions


def _make_profile():
    return SchemaProfile(
        schema_version="1.0", catalog="main", schema_name="sales",
        tables=[TableProfile(
            catalog="main", schema_name="sales", table="orders",
            description="Order records",
            columns=[ColumnProfile(name="amount", data_type="DOUBLE", nullable=True)],
        )],
        relationships=[], profiled_at=datetime(2026, 3, 15),
    )


class TestGenerateInstructions:
    @patch("genie_world.builder.instructions.call_llm")
    def test_generates_single_instruction(self, mock_llm):
        mock_llm.return_value = json.dumps({
            "content": [
                "When calculating revenue, sum the amount column.",
                "Round monetary values to 2 decimal places."
            ]
        })

        result = generate_instructions(_make_profile(), [], {}, [])

        assert len(result) == 1
        assert isinstance(result[0]["content"], list)
        assert len(result[0]["content"]) == 2

    @patch("genie_world.builder.instructions.call_llm")
    def test_handles_llm_error(self, mock_llm):
        mock_llm.side_effect = RuntimeError("unavailable")

        result = generate_instructions(_make_profile(), [], {}, [])

        assert result == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_instructions.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement instructions generator**

```python
# genie_world/builder/instructions.py
"""LLM generator: text instructions (generated last to avoid conflicts)."""

from __future__ import annotations

import logging

from genie_world.core.llm import call_llm, parse_json_from_llm_response
from genie_world.core.tracing import trace
from genie_world.profiler.models import SchemaProfile

logger = logging.getLogger(__name__)


def _build_instructions_prompt(
    profile: SchemaProfile,
    join_specs: list[dict],
    snippets: dict,
    examples: list[dict],
) -> list[dict]:
    """Build prompt for generating text instructions."""
    tables_summary = ", ".join(t.table for t in profile.tables)

    joins_summary = "\n".join(
        f"  - {j['left']['alias']} ↔ {j['right']['alias']}: {j['sql'][0]}"
        for j in join_specs
    ) if join_specs else "  (none)"

    examples_summary = "\n".join(
        f"  - Q: {ex.get('question', '')}"
        for ex in examples[:5]
    ) if examples else "  (none)"

    system_msg = (
        "You are a Databricks Genie Space configuration expert. Generate a single, focused "
        "text instruction that helps Genie answer questions accurately. "
        "Return ONLY valid JSON — no prose, no markdown."
    )

    user_msg = (
        f"Tables: {tables_summary}\n\n"
        f"Configured joins:\n{joins_summary}\n\n"
        f"Example questions already configured:\n{examples_summary}\n\n"
        "Generate ONE text instruction covering:\n"
        "- How to interpret key business terms\n"
        "- Default time period handling (e.g., 'last month' = previous calendar month)\n"
        "- Rounding/formatting conventions\n"
        "- When to ask for clarification\n\n"
        "IMPORTANT: Do NOT repeat or contradict the SQL examples above.\n"
        "Keep it concise and globally applicable.\n\n"
        'Return: {"content": ["instruction line 1", "instruction line 2", ...]}'
    )

    return [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": user_msg},
    ]


@trace(name="generate_instructions", span_type="CHAIN")
def generate_instructions(
    profile: SchemaProfile,
    join_specs: list[dict],
    snippets: dict,
    examples: list[dict],
) -> list[dict]:
    """Generate text instructions that complement existing examples and snippets.

    Returns a list with at most one instruction dict (no ID — assembler assigns it).
    On error, returns empty list.
    """
    prompt = _build_instructions_prompt(profile, join_specs, snippets, examples)

    try:
        raw = call_llm(prompt)
        result = parse_json_from_llm_response(raw)
    except Exception as exc:
        logger.warning("Instruction generation failed: %s", exc)
        return []

    content = result.get("content", [])
    if isinstance(content, str):
        content = [content]
    if not isinstance(content, list) or not content:
        return []

    return [{"content": content}]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_instructions.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/builder/instructions.py tests/unit/builder/test_instructions.py
git commit -m "feat(builder): add instructions generator (runs last)"
```

---

## Chunk 3: Assembler, Deployer, and Public API

### Task 10: Assembler

**Files:**
- Create: `genie_world/builder/assembler.py`
- Create: `tests/unit/builder/test_assembler.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/builder/test_assembler.py
import pytest
from genie_world.builder.assembler import assemble_space


class TestAssembleSpace:
    def test_basic_structure(self):
        config = assemble_space(
            data_sources={"tables": [{"identifier": "main.s.t", "column_configs": []}]},
            join_specs=[],
            instructions=[{"content": ["Do this"]}],
            snippets={"filters": [], "expressions": [], "measures": []},
            examples=[{"question": "How many?", "sql": "SELECT COUNT(*) FROM t"}],
        )

        assert config["version"] == 2
        assert "config" in config
        assert "data_sources" in config
        assert "instructions" in config
        assert config["instructions"]["text_instructions"][0]["content"] == ["Do this"]
        assert len(config["instructions"]["example_question_sqls"]) == 1

    def test_generates_32_char_hex_ids(self):
        config = assemble_space(
            data_sources={"tables": []},
            join_specs=[{"left": {}, "right": {}, "sql": ["a = b"]}],
            instructions=[{"content": ["Test"]}],
            snippets={"filters": [{"sql": "x = 1"}], "expressions": [], "measures": []},
            examples=[],
        )

        # Check join spec has ID
        js = config["instructions"]["join_specs"][0]
        assert "id" in js
        assert len(js["id"]) == 32
        assert js["id"].isalnum()

    def test_derives_sample_questions(self):
        config = assemble_space(
            data_sources={"tables": []},
            join_specs=[],
            instructions=[],
            snippets={"filters": [], "expressions": [], "measures": []},
            examples=[
                {"question": "Q1", "sql": "SELECT 1"},
                {"question": "Q2", "sql": "SELECT 2"},
                {"question": "Q3", "sql": "SELECT 3"},
                {"question": "Q4", "sql": "SELECT 4"},
                {"question": "Q5", "sql": "SELECT 5"},
            ],
        )

        sample_qs = config["config"]["sample_questions"]
        assert 3 <= len(sample_qs) <= 5
        for sq in sample_qs:
            assert "id" in sq
            assert "question" in sq
            assert isinstance(sq["question"], list)
            # No SQL in sample_questions
            assert "sql" not in sq

    def test_wraps_strings_in_lists(self):
        config = assemble_space(
            data_sources={"tables": []},
            join_specs=[],
            instructions=[{"content": "bare string"}],
            snippets={"filters": [], "expressions": [], "measures": []},
            examples=[{"question": "Q", "sql": "SELECT 1"}],
        )

        assert config["instructions"]["text_instructions"][0]["content"] == ["bare string"]

    def test_benchmarks_optional(self):
        config = assemble_space(
            data_sources={"tables": []}, join_specs=[], instructions=[],
            snippets={"filters": [], "expressions": [], "measures": []},
            examples=[],
        )
        assert "benchmarks" not in config or config["benchmarks"] is None

    def test_includes_benchmarks_when_provided(self):
        config = assemble_space(
            data_sources={"tables": []}, join_specs=[], instructions=[],
            snippets={"filters": [], "expressions": [], "measures": []},
            examples=[],
            benchmarks={"questions": [{"question": "Q", "answer": [{"format": "SQL", "content": ["SELECT 1"]}]}]},
        )
        assert len(config["benchmarks"]["questions"]) == 1
        assert "id" in config["benchmarks"]["questions"][0]

    def test_passthrough_sql_functions(self):
        config = assemble_space(
            data_sources={"tables": []}, join_specs=[], instructions=[],
            snippets={"filters": [], "expressions": [], "measures": []},
            examples=[],
            sql_functions=[{"identifier": "main.s.my_func"}],
        )
        funcs = config["instructions"]["sql_functions"]
        assert len(funcs) == 1
        assert "id" in funcs[0]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_assembler.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement assembler**

```python
# genie_world/builder/assembler.py
"""Assembler: combines generated sections into a valid Genie Space config."""

from __future__ import annotations

import logging
import uuid

logger = logging.getLogger(__name__)

_STRING_ARRAY_FIELDS = {
    "description", "content", "question", "sql", "instruction",
    "synonyms", "usage_guidance", "comment",
}

_MAX_STRING_LENGTH = 1024  # 1 KB per string element


def _gen_id() -> str:
    """Generate a 32-character lowercase hex ID."""
    return uuid.uuid4().hex


def _ensure_string_array(value) -> list[str]:
    """Wrap a bare string in a list; split if > 1KB."""
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return value

    result = []
    for item in value:
        if isinstance(item, str) and len(item) > _MAX_STRING_LENGTH:
            # Split at sentence boundaries or at max length
            while len(item) > _MAX_STRING_LENGTH:
                split_at = item.rfind(". ", 0, _MAX_STRING_LENGTH)
                if split_at == -1:
                    split_at = _MAX_STRING_LENGTH
                else:
                    split_at += 1  # include the period
                result.append(item[:split_at])
                item = item[split_at:].lstrip()
            if item:
                result.append(item)
        else:
            result.append(item)
    return result


def _process_dict(obj: dict) -> dict:
    """Recursively enforce string arrays in a dict."""
    result = {}
    for key, value in obj.items():
        if key in _STRING_ARRAY_FIELDS and not isinstance(value, list):
            result[key] = _ensure_string_array(value)
        elif isinstance(value, dict):
            result[key] = _process_dict(value)
        elif isinstance(value, list):
            result[key] = [_process_dict(item) if isinstance(item, dict) else item for item in value]
        else:
            result[key] = value
    return result


def _add_ids(items: list[dict]) -> list[dict]:
    """Add 'id' field to each dict that doesn't already have one."""
    for item in items:
        if isinstance(item, dict) and "id" not in item:
            item["id"] = _gen_id()
    return items


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
    """Combine all sections into a valid Genie Space config.

    Generates IDs, derives sample_questions, enforces string arrays,
    sorting, and schema constraints.
    """
    # Process string arrays in all sections
    data_sources = _process_dict(data_sources)
    join_specs = [_process_dict(js) for js in join_specs]
    instructions = [_process_dict(inst) for inst in instructions]
    examples = [_process_dict(ex) for ex in examples]

    snippets = {
        "filters": [_process_dict(f) for f in snippets.get("filters", [])],
        "expressions": [_process_dict(e) for e in snippets.get("expressions", [])],
        "measures": [_process_dict(m) for m in snippets.get("measures", [])],
    }

    # Generate IDs
    _add_ids(join_specs)
    _add_ids(instructions)
    _add_ids(examples)
    _add_ids(snippets["filters"])
    _add_ids(snippets["expressions"])
    _add_ids(snippets["measures"])

    if sql_functions:
        sql_functions = [_process_dict(f) for f in sql_functions]
        _add_ids(sql_functions)

    # Derive sample_questions from examples (3-5 questions, no SQL)
    sample_count = min(max(3, len(examples)), 5)
    sample_questions = []
    for ex in examples[:sample_count]:
        q = ex.get("question", "")
        if isinstance(q, str):
            q = [q]
        sample_questions.append({"id": _gen_id(), "question": q})

    # Constraint: at most 1 text instruction
    if len(instructions) > 1:
        logger.warning("Truncating text_instructions from %d to 1", len(instructions))
        instructions = instructions[:1]

    # Build config
    config: dict = {
        "version": 2,
        "config": {"sample_questions": sample_questions},
        "data_sources": data_sources,
        "instructions": {
            "text_instructions": instructions,
            "example_question_sqls": examples,
            "sql_functions": sql_functions or [],
            "join_specs": join_specs,
            "sql_snippets": snippets,
        },
    }

    # Add metric_views if provided
    if metric_views:
        config["data_sources"]["metric_views"] = metric_views

    # Add benchmarks if provided
    if benchmarks and benchmarks.get("questions"):
        questions = benchmarks["questions"]
        _add_ids(questions)
        config["benchmarks"] = {"questions": questions}

    return config
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_assembler.py -v`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/builder/assembler.py tests/unit/builder/test_assembler.py
git commit -m "feat(builder): add assembler with ID generation and constraint enforcement"
```

---

### Task 11: Deployer

**Files:**
- Create: `genie_world/builder/deployer.py`
- Create: `tests/unit/builder/test_deployer.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/builder/test_deployer.py
import json
import pytest
from unittest.mock import patch, MagicMock
from genie_world.builder.deployer import create_space


class TestCreateSpace:
    @patch("genie_world.builder.deployer.get_workspace_client")
    def test_creates_space_successfully(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.config.host = "https://test.cloud.databricks.com"
        mock_client.api_client.do.return_value = {"space_id": "abc123"}

        result = create_space(
            config={"version": 2},
            display_name="Test Space",
            warehouse_id="wh-123",
            parent_path="/Workspace/Users/me/",
        )

        assert result["space_id"] == "abc123"
        assert "space_url" in result
        assert "abc123" in result["space_url"]

    @patch("genie_world.builder.deployer.get_workspace_client")
    def test_raises_on_oversized_config(self, mock_get_client):
        huge_config = {"data": "x" * (4 * 1024 * 1024)}  # > 3.5 MB

        with pytest.raises(ValueError, match="3.5 MB"):
            create_space(huge_config, "Test", "wh-123", "/Workspace/")

    def test_raises_on_empty_display_name(self):
        with pytest.raises(ValueError, match="Display name"):
            create_space({"version": 2}, "", "wh-123", "/Workspace/")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_deployer.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement deployer**

```python
# genie_world/builder/deployer.py
"""Deploy a Genie Space config to Databricks."""

from __future__ import annotations

import json
import logging

from genie_world.core.auth import get_workspace_client
from genie_world.core.tracing import trace

logger = logging.getLogger(__name__)

_MAX_SERIALIZED_SIZE = 3_500_000  # 3.5 MB


@trace(name="create_space", span_type="CHAIN")
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
    if not display_name or not display_name.strip():
        raise ValueError("Display name is required")

    display_name = display_name.strip()

    if not parent_path.endswith("/"):
        parent_path += "/"

    serialized = json.dumps(config)

    if len(serialized) > _MAX_SERIALIZED_SIZE:
        size_mb = len(serialized) / (1024 * 1024)
        raise ValueError(
            f"Serialized config is {size_mb:.1f} MB, exceeds 3.5 MB limit. "
            "Reduce the number of tables, examples, or benchmarks."
        )

    client = get_workspace_client()
    host = (client.config.host or "").rstrip("/")

    logger.info("Creating Genie Space '%s' at %s", display_name, parent_path)

    try:
        response = client.api_client.do(
            method="POST",
            path="/api/2.0/genie/spaces",
            body={
                "title": display_name,
                "description": description or f"Genie Space generated by genie-world",
                "parent_path": parent_path,
                "warehouse_id": warehouse_id,
                "serialized_space": serialized,
            },
        )

        space_id = response.get("space_id")
        if not space_id:
            raise ValueError(f"API did not return space_id: {response}")

        space_url = f"{host}/genie/rooms/{space_id}"
        logger.info("Created Genie Space: %s", space_url)

        return {
            "space_id": space_id,
            "display_name": display_name,
            "space_url": space_url,
        }

    except Exception as e:
        error_str = str(e).lower()
        if "403" in error_str or "permission" in error_str:
            raise PermissionError(
                "Cannot create Genie Space. Ensure write permission to the target directory."
            )
        elif "400" in error_str or "invalid" in error_str:
            raise ValueError(f"Invalid configuration: {e}")
        else:
            raise
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_deployer.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/builder/deployer.py tests/unit/builder/test_deployer.py
git commit -m "feat(builder): add deployer with size check and error mapping"
```

---

### Task 12: Public API (build_space orchestrator)

**Files:**
- Modify: `genie_world/builder/__init__.py`
- Create: `tests/unit/builder/test_public_api.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/builder/test_public_api.py
import json
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from genie_world.profiler.models import ColumnProfile, SchemaProfile, TableProfile
from genie_world.builder import build_space, BuildResult


def _make_profile():
    return SchemaProfile(
        schema_version="1.0", catalog="main", schema_name="sales",
        tables=[TableProfile(
            catalog="main", schema_name="sales", table="orders",
            description="Orders",
            columns=[
                ColumnProfile(name="id", data_type="INT", nullable=False, description="PK"),
                ColumnProfile(name="amount", data_type="DOUBLE", nullable=True, description="Total"),
            ],
        )],
        relationships=[], profiled_at=datetime(2026, 3, 15),
    )


class TestBuildSpace:
    @patch("genie_world.builder.generate_instructions")
    @patch("genie_world.builder.generate_benchmarks")
    @patch("genie_world.builder.generate_example_sqls")
    @patch("genie_world.builder.generate_snippets")
    def test_orchestrates_all_generators(self, mock_snippets, mock_examples, mock_benchmarks, mock_instructions):
        mock_snippets.return_value = {"filters": [], "expressions": [], "measures": []}
        mock_examples.return_value = ([{"question": "Q", "sql": "SELECT 1"}], [])
        mock_benchmarks.return_value = ({"questions": []}, [])
        mock_instructions.return_value = [{"content": ["Test"]}]

        result = build_space(_make_profile())

        assert isinstance(result, BuildResult)
        assert result.config["version"] == 2
        assert "data_sources" in result.config
        assert "instructions" in result.config
        mock_snippets.assert_called_once()
        mock_examples.assert_called_once()
        mock_benchmarks.assert_called_once()
        mock_instructions.assert_called_once()

    @patch("genie_world.builder.generate_instructions")
    @patch("genie_world.builder.generate_benchmarks")
    @patch("genie_world.builder.generate_example_sqls")
    @patch("genie_world.builder.generate_snippets")
    def test_warns_when_no_warehouse(self, mock_snippets, mock_examples, mock_benchmarks, mock_instructions):
        mock_snippets.return_value = {"filters": [], "expressions": [], "measures": []}
        mock_examples.return_value = ([], [])
        mock_benchmarks.return_value = ({"questions": []}, [])
        mock_instructions.return_value = []

        result = build_space(_make_profile(), warehouse_id=None)

        assert any("validation skipped" in w.message.lower() for w in result.warnings)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_public_api.py -v`
Expected: FAIL — `ImportError: cannot import name 'build_space'`

- [ ] **Step 3: Implement build_space orchestrator**

Update `genie_world/builder/__init__.py` to add the full orchestrator:

```python
# genie_world/builder/__init__.py
"""Space Builder block for genie-world.

Generates complete Genie Space configurations from SchemaProfile data.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel

from genie_world.builder.assembler import assemble_space
from genie_world.builder.benchmarks import generate_benchmarks
from genie_world.builder.data_sources import generate_data_sources
from genie_world.builder.deployer import create_space
from genie_world.builder.example_sqls import generate_example_sqls
from genie_world.builder.instructions import generate_instructions
from genie_world.builder.join_specs import generate_join_specs
from genie_world.builder.snippets import generate_snippets
from genie_world.core.tracing import trace
from genie_world.profiler.models import SchemaProfile

logger = logging.getLogger(__name__)


class BuilderWarning(BaseModel):
    """Warning generated during space building."""
    section: str
    message: str
    detail: str | None = None


class BuildResult(BaseModel):
    """Result from build_space() containing config and any warnings."""
    config: dict
    warnings: list[BuilderWarning]


@trace(name="build_space", span_type="CHAIN")
def build_space(
    profile: SchemaProfile,
    *,
    warehouse_id: str | None = None,
    example_count: int = 10,
    benchmark_count: int = 10,
    sql_functions: list[dict] | None = None,
    metric_views: list[dict] | None = None,
) -> BuildResult:
    """Generate a complete Genie Space config from a SchemaProfile.

    Returns BuildResult with config dict and list of BuilderWarnings.
    """
    warnings: list[BuilderWarning] = []

    if not warehouse_id:
        warnings.append(BuilderWarning(
            section="general",
            message="SQL validation skipped — no warehouse_id provided.",
        ))

    # 1. Deterministic generators
    data_sources = generate_data_sources(profile)
    join_specs = generate_join_specs(profile)

    # 2. LLM: snippets
    snippets = generate_snippets(profile)

    # 3. LLM + validate: examples
    examples, example_warnings = generate_example_sqls(
        profile, join_specs, snippets, warehouse_id=warehouse_id, count=example_count,
    )
    for w in example_warnings:
        warnings.append(BuilderWarning(section="example_sqls", message=w))

    # 4. LLM + validate: benchmarks (receives examples to avoid overlap)
    benchmarks, bench_warnings = generate_benchmarks(
        profile, join_specs, snippets, examples,
        warehouse_id=warehouse_id, count=benchmark_count,
    )
    for w in bench_warnings:
        warnings.append(BuilderWarning(section="benchmarks", message=w))

    # 5. LLM: instructions LAST
    instructions = generate_instructions(profile, join_specs, snippets, examples)

    # 6. Assemble
    config = assemble_space(
        data_sources=data_sources,
        join_specs=join_specs,
        instructions=instructions,
        snippets=snippets,
        examples=examples,
        benchmarks=benchmarks if benchmarks.get("questions") else None,
        sql_functions=sql_functions,
        metric_views=metric_views,
    )

    return BuildResult(config=config, warnings=warnings)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source .venv/bin/activate && pytest tests/unit/builder/test_public_api.py -v`
Expected: 2 passed.

- [ ] **Step 5: Run full test suite**

Run: `source .venv/bin/activate && pytest tests/ -v --tb=short`
Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add genie_world/builder/__init__.py tests/unit/builder/test_public_api.py
git commit -m "feat(builder): add build_space() orchestrator with BuildResult"
```

---

### Task 13: Final Verification

- [ ] **Step 1: Run full test suite**

Run: `source .venv/bin/activate && pytest tests/ -v --tb=short`
Expected: All tests pass.

- [ ] **Step 2: Verify imports**

Run: `source .venv/bin/activate && python -c "from genie_world.builder import build_space, create_space, BuildResult, BuilderWarning, generate_data_sources, generate_join_specs, generate_snippets, generate_example_sqls, generate_benchmarks, generate_instructions, assemble_space; print('All exports OK')"`
Expected: `All exports OK`

- [ ] **Step 3: Verify profiler enhancement**

Run: `source .venv/bin/activate && python -c "from genie_world.profiler.description_enricher import enrich_descriptions_for_table; print('Description enricher OK')"`
Expected: `Description enricher OK`
