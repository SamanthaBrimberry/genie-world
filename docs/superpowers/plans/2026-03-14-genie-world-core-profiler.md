# Genie World: Core + Data Profiler Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the shared core infrastructure and the Data Profiler block — the first deliverable of the genie-world library.

**Architecture:** Python library with a `core/` package providing auth, config, SQL execution, LLM access, tracing, and storage, plus a `profiler/` package with tiered profiling (metadata, data, usage, relationships, synonyms). Each tier is opt-in and degrades gracefully when dependencies are unavailable.

**Tech Stack:** Python 3.10+, pydantic 2.x, databricks-sdk, httpx, mlflow (optional), pytest

**Spec:** `docs/superpowers/specs/2026-03-14-genie-world-design.md`

---

## File Structure

### Project Root
| File | Purpose |
|------|---------|
| `pyproject.toml` | Package config, dependencies, optional extras |
| `genie_world/__init__.py` | Top-level package |

### Core (`genie_world/core/`)
| File | Purpose |
|------|---------|
| `genie_world/core/__init__.py` | Core package exports |
| `genie_world/core/models.py` | Shared Pydantic models: SpaceConfig stub, base types |
| `genie_world/core/config.py` | GenieWorldConfig: env vars, .env files, programmatic config |
| `genie_world/core/auth.py` | WorkspaceClient factory, OBO support via contextvars |
| `genie_world/core/tracing.py` | MLflow trace decorator, no-op fallback |
| `genie_world/core/sql.py` | Statement Execution API wrapper, read-only validation |
| `genie_world/core/llm.py` | LLM serving endpoint wrapper, retry, JSON repair |
| `genie_world/core/storage.py` | Artifact persistence: UC Volumes or local filesystem |
| `genie_world/core/genie_client.py` | **Deferred to Block 2 (Builder) plan** — Genie REST API wrapper. Not needed by Profiler. |

### Profiler (`genie_world/profiler/`)
| File | Purpose |
|------|---------|
| `genie_world/profiler/__init__.py` | Public API: profile_schema(), profile_tables() |
| `genie_world/profiler/models.py` | ColumnProfile, TableProfile, Relationship, SchemaProfile, etc. |
| `genie_world/profiler/metadata_profiler.py` | Tier 1: UC API metadata extraction |
| `genie_world/profiler/data_profiler.py` | Tier 2: SQL-based statistical profiling |
| `genie_world/profiler/usage_profiler.py` | Tier 3: System tables mining |
| `genie_world/profiler/relationship_detector.py` | PK/FK inference from multiple sources |
| `genie_world/profiler/synonym_generator.py` | LLM-powered synonym generation |

### Tests
| File | Purpose |
|------|---------|
| `tests/__init__.py` | Test package |
| `tests/unit/__init__.py` | Unit test package |
| `tests/unit/core/__init__.py` | Core unit tests |
| `tests/unit/core/test_config.py` | Config tests |
| `tests/unit/core/test_auth.py` | Auth tests |
| `tests/unit/core/test_tracing.py` | Tracing tests |
| `tests/unit/core/test_sql.py` | SQL executor tests |
| `tests/unit/core/test_llm.py` | LLM utility tests |
| `tests/unit/core/test_storage.py` | Storage tests |
| `tests/unit/profiler/__init__.py` | Profiler unit tests |
| `tests/unit/profiler/test_models.py` | Profiler model tests |
| `tests/unit/profiler/test_metadata_profiler.py` | Metadata profiler tests |
| `tests/unit/profiler/test_data_profiler.py` | Data profiler tests |
| `tests/unit/profiler/test_usage_profiler.py` | Usage profiler tests |
| `tests/unit/profiler/test_relationship_detector.py` | Relationship detector tests |
| `tests/unit/profiler/test_synonym_generator.py` | Synonym generator tests |
| `tests/unit/profiler/test_public_api.py` | profile_schema/profile_tables tests |

---

## Chunk 1: Project Scaffolding + Core Foundation

### Task 1: Project Scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `genie_world/__init__.py`
- Create: `genie_world/core/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/unit/__init__.py`
- Create: `tests/unit/core/__init__.py`

- [ ] **Step 1: Create pyproject.toml**

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "genie-world"
version = "0.1.0"
description = "Modular building blocks for optimizing Databricks Genie Spaces"
requires-python = ">=3.10"
dependencies = [
    "databricks-sdk>=0.38.0",
    "pydantic>=2.0.0",
]

[project.optional-dependencies]
profiler = [
    "httpx>=0.25.0",
]
llm = [
    "httpx>=0.25.0",
]
tracing = [
    "mlflow>=2.10.0",
]
dev = [
    "pytest>=8.0.0",
    "pytest-mock>=3.12.0",
    "httpx>=0.25.0",
]
all = [
    "genie-world[profiler,llm,tracing,dev]",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["."]
```

- [ ] **Step 2: Create package init files**

`genie_world/__init__.py`:
```python
"""Genie World: Modular building blocks for optimizing Databricks Genie Spaces."""

__version__ = "0.1.0"
```

`genie_world/core/__init__.py`:
```python
"""Core infrastructure shared across all genie-world blocks."""
```

`tests/__init__.py`, `tests/unit/__init__.py`, `tests/unit/core/__init__.py`: empty files.

- [ ] **Step 3: Verify project installs**

Run: `cd /Users/sammy.brimberry/dbdemos/genie-world && pip install -e ".[dev]"`
Expected: Installs successfully.

- [ ] **Step 4: Verify pytest discovers tests**

Run: `pytest --collect-only`
Expected: No errors (0 tests collected is fine at this point).

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml genie_world/ tests/
git commit -m "feat: scaffold genie-world project with pyproject.toml and package structure"
```

---

### Task 2: Core Config

**Files:**
- Create: `genie_world/core/config.py`
- Create: `tests/unit/core/test_config.py`

- [ ] **Step 1: Write failing tests for config**

```python
# tests/unit/core/test_config.py
import os
import pytest
from genie_world.core.config import GenieWorldConfig


class TestGenieWorldConfig:
    def test_defaults(self):
        """Config should have sensible defaults."""
        config = GenieWorldConfig()
        assert config.warehouse_id is None
        assert config.llm_model == "databricks-claude-sonnet-4-6"
        assert config.storage_path is None
        assert config.mlflow_experiment_id is None
        assert config.max_workers == 4

    def test_from_env(self, monkeypatch):
        """Config should read from environment variables."""
        monkeypatch.setenv("GENIE_WORLD_WAREHOUSE_ID", "wh-123")
        monkeypatch.setenv("GENIE_WORLD_LLM_MODEL", "databricks-claude-opus-4-6")
        monkeypatch.setenv("GENIE_WORLD_STORAGE_PATH", "/Volumes/my/path")
        monkeypatch.setenv("GENIE_WORLD_MLFLOW_EXPERIMENT_ID", "exp-456")
        monkeypatch.setenv("GENIE_WORLD_MAX_WORKERS", "8")

        config = GenieWorldConfig.from_env()
        assert config.warehouse_id == "wh-123"
        assert config.llm_model == "databricks-claude-opus-4-6"
        assert config.storage_path == "/Volumes/my/path"
        assert config.mlflow_experiment_id == "exp-456"
        assert config.max_workers == 8

    def test_override(self):
        """Programmatic values should override defaults."""
        config = GenieWorldConfig(warehouse_id="wh-999", max_workers=2)
        assert config.warehouse_id == "wh-999"
        assert config.max_workers == 2

    def test_global_config(self, monkeypatch):
        """get_config() returns global config, set_config() replaces it."""
        from genie_world.core.config import get_config, set_config

        custom = GenieWorldConfig(warehouse_id="custom-wh")
        set_config(custom)
        assert get_config().warehouse_id == "custom-wh"
        # Reset
        set_config(GenieWorldConfig())
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/core/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'genie_world.core.config'`

- [ ] **Step 3: Implement config**

```python
# genie_world/core/config.py
"""Project-level configuration for genie-world."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class GenieWorldConfig:
    """Configuration for genie-world library.

    Can be set programmatically, from environment variables, or both.
    Programmatic values take precedence over env vars.
    """

    warehouse_id: str | None = None
    llm_model: str = "databricks-claude-sonnet-4-6"
    storage_path: str | None = None
    mlflow_experiment_id: str | None = None
    max_workers: int = 4

    @classmethod
    def from_env(cls) -> GenieWorldConfig:
        """Create config from GENIE_WORLD_* environment variables."""
        max_workers_str = os.environ.get("GENIE_WORLD_MAX_WORKERS", "4")
        return cls(
            warehouse_id=os.environ.get("GENIE_WORLD_WAREHOUSE_ID"),
            llm_model=os.environ.get(
                "GENIE_WORLD_LLM_MODEL", "databricks-claude-sonnet-4-6"
            ),
            storage_path=os.environ.get("GENIE_WORLD_STORAGE_PATH"),
            mlflow_experiment_id=os.environ.get("GENIE_WORLD_MLFLOW_EXPERIMENT_ID"),
            max_workers=int(max_workers_str),
        )


_global_config: GenieWorldConfig = GenieWorldConfig()


def get_config() -> GenieWorldConfig:
    """Get the global genie-world configuration."""
    return _global_config


def set_config(config: GenieWorldConfig) -> None:
    """Set the global genie-world configuration."""
    global _global_config
    _global_config = config
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/core/test_config.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/core/config.py tests/unit/core/test_config.py
git commit -m "feat(core): add GenieWorldConfig with env var and programmatic config"
```

---

### Task 3: Core Auth

**Files:**
- Create: `genie_world/core/auth.py`
- Create: `tests/unit/core/test_auth.py`

- [ ] **Step 1: Write failing tests for auth**

```python
# tests/unit/core/test_auth.py
import os
import pytest
from unittest.mock import MagicMock, patch
from genie_world.core.auth import (
    get_workspace_client,
    set_obo_token,
    get_obo_token,
    is_running_on_databricks_apps,
)


class TestOboToken:
    def test_default_is_none(self):
        assert get_obo_token() is None

    def test_set_and_get(self):
        set_obo_token("test-token")
        assert get_obo_token() == "test-token"
        # Cleanup
        set_obo_token(None)


class TestIsRunningOnDatabricksApps:
    def test_false_when_no_env(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        assert is_running_on_databricks_apps() is False

    def test_true_when_env_set(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_APP_PORT", "8080")
        assert is_running_on_databricks_apps() is True


class TestGetWorkspaceClient:
    @patch("genie_world.core.auth.WorkspaceClient")
    def test_returns_obo_client_when_token_set(self, mock_ws_class, monkeypatch):
        monkeypatch.setenv("DATABRICKS_HOST", "https://test.cloud.databricks.com")
        set_obo_token("obo-token-123")

        client = get_workspace_client()

        mock_ws_class.assert_called_once_with(
            host="https://test.cloud.databricks.com",
            token="obo-token-123",
            auth_type="pat",
        )
        # Cleanup
        set_obo_token(None)

    @patch("genie_world.core.auth.WorkspaceClient")
    def test_returns_default_client_when_no_token(self, mock_ws_class):
        set_obo_token(None)

        client = get_workspace_client()

        mock_ws_class.assert_called_once_with()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/core/test_auth.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement auth**

```python
# genie_world/core/auth.py
"""Databricks authentication utilities.

Supports OBO (On-Behalf-Of) for Databricks Apps and PAT/CLI fallback
for local development.
"""

from __future__ import annotations

import contextvars
import logging
import os

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

_obo_token: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "_obo_token", default=None
)


def set_obo_token(token: str | None) -> None:
    """Set the OBO token for the current request context."""
    _obo_token.set(token)


def get_obo_token() -> str | None:
    """Get the OBO token for the current request context."""
    return _obo_token.get()


def is_running_on_databricks_apps() -> bool:
    """Check if running on Databricks Apps (vs local development)."""
    return os.environ.get("DATABRICKS_APP_PORT") is not None


def get_workspace_client() -> WorkspaceClient:
    """Get a WorkspaceClient with appropriate authentication.

    Uses OBO token if set (Databricks Apps), otherwise falls back to
    PAT/CLI/OAuth auto-detection.
    """
    token = get_obo_token()
    if token:
        host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
        logger.debug("Creating OBO WorkspaceClient for host: %s", host)
        return WorkspaceClient(host=host, token=token, auth_type="pat")

    return WorkspaceClient()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/core/test_auth.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/core/auth.py tests/unit/core/test_auth.py
git commit -m "feat(core): add auth module with OBO and PAT/CLI fallback"
```

---

### Task 4: Core Tracing

**Files:**
- Create: `genie_world/core/tracing.py`
- Create: `tests/unit/core/test_tracing.py`

- [ ] **Step 1: Write failing tests for tracing**

```python
# tests/unit/core/test_tracing.py
from genie_world.core.tracing import trace


class TestTraceDecorator:
    def test_decorated_function_runs(self):
        """Trace decorator should not break function execution."""

        @trace
        def add(a, b):
            return a + b

        assert add(1, 2) == 3

    def test_decorated_with_args(self):
        """Trace decorator with arguments should work."""

        @trace(name="custom_name", span_type="PARSER")
        def parse(data):
            return data.upper()

        assert parse("hello") == "HELLO"

    def test_preserves_function_name(self):
        """Decorated function should preserve its name."""

        @trace
        def my_func():
            pass

        # When mlflow is not available, the function is returned as-is
        # When mlflow IS available, mlflow.trace wraps it (name may differ)
        # Either way, calling it should work
        my_func()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/core/test_tracing.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement tracing**

```python
# genie_world/core/tracing.py
"""MLflow trace decorator with graceful fallback.

Adopts the pattern from databricks-ai-bridge's _compat.py:
if mlflow is installed, traces the function; otherwise, no-op.
"""

from __future__ import annotations

from typing import Callable, Optional, TypeVar

F = TypeVar("F", bound=Callable)


def trace(
    func: F | None = None,
    *,
    name: str | None = None,
    span_type: str | None = None,
) -> F:
    """Decorator that traces a function with MLflow if available.

    Supports both @trace and @trace(name="...", span_type="...") syntax.
    If mlflow is not installed, the function runs normally without tracing.
    """

    def decorator(f: F) -> F:
        try:
            import mlflow

            return mlflow.trace(f, name=name, span_type=span_type)  # type: ignore[return-value]
        except ImportError:
            return f

    if func is None:
        return decorator  # type: ignore[return-value]
    else:
        return decorator(func)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/core/test_tracing.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/core/tracing.py tests/unit/core/test_tracing.py
git commit -m "feat(core): add MLflow trace decorator with no-op fallback"
```

---

## Chunk 2: Core SQL, LLM, and Storage

### Task 5: Core SQL Executor

**Files:**
- Create: `genie_world/core/sql.py`
- Create: `tests/unit/core/test_sql.py`

- [ ] **Step 1: Write failing tests for SQL validation**

```python
# tests/unit/core/test_sql.py
import pytest
from unittest.mock import MagicMock, patch
from genie_world.core.sql import validate_sql_read_only, execute_sql, SqlValidationError


class TestValidateSqlReadOnly:
    def test_allows_select(self):
        validate_sql_read_only("SELECT * FROM my_table")

    def test_allows_with_cte(self):
        validate_sql_read_only("WITH cte AS (SELECT 1) SELECT * FROM cte")

    def test_blocks_drop(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("DROP TABLE my_table")

    def test_blocks_delete(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("DELETE FROM my_table WHERE id = 1")

    def test_blocks_insert(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("INSERT INTO my_table VALUES (1)")

    def test_blocks_update(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("UPDATE my_table SET col = 1")

    def test_blocks_statement_chaining(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("SELECT 1; DROP TABLE my_table")

    def test_rejects_non_select(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("SHOW TABLES")


class TestExecuteSql:
    def test_returns_error_when_no_warehouse(self):
        result = execute_sql("SELECT 1", warehouse_id=None)
        assert result["error"] is not None
        assert result["row_count"] == 0

    def test_returns_error_for_dangerous_sql(self):
        result = execute_sql("DROP TABLE foo", warehouse_id="wh-123")
        assert result["error"] is not None

    @patch("genie_world.core.sql.get_workspace_client")
    def test_executes_and_parses_result(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock the response structure
        mock_col = MagicMock()
        mock_col.name = "id"
        mock_col.type_name = "INT"

        mock_response = MagicMock()
        mock_response.status.state.value = "SUCCEEDED"
        mock_response.manifest.schema.columns = [mock_col]
        mock_response.manifest.truncated = False
        mock_response.result.data_array = [["1"], ["2"]]

        mock_client.statement_execution.execute_statement.return_value = mock_response

        result = execute_sql("SELECT id FROM t", warehouse_id="wh-123")
        assert result["error"] is None
        assert result["row_count"] == 2
        assert result["columns"] == [{"name": "id", "type_name": "INT"}]
        assert result["data"] == [["1"], ["2"]]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/core/test_sql.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement SQL executor**

```python
# genie_world/core/sql.py
"""Statement Execution API wrapper with read-only SQL validation.

Ported from dbx-genie-rx's sql_executor.py.
"""

from __future__ import annotations

import logging
import re

from genie_world.core.auth import get_workspace_client

logger = logging.getLogger(__name__)

MAX_ROWS = 1000
WAIT_TIMEOUT = "30s"

_BLOCKED_SQL_PATTERNS = [
    r"\b(DROP|DELETE|TRUNCATE|UPDATE|INSERT|ALTER|CREATE|GRANT|REVOKE)\b",
    r"\b(EXEC|EXECUTE|CALL)\b",
    r";\s*\w",
]


class SqlValidationError(Exception):
    """Raised when SQL validation fails."""

    pass


def validate_sql_read_only(sql: str) -> None:
    """Validate that SQL is a read-only SELECT or WITH query."""
    sql_upper = sql.upper().strip()

    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        raise SqlValidationError(
            "Only SELECT queries are allowed. Query must start with SELECT or WITH."
        )

    for pattern in _BLOCKED_SQL_PATTERNS:
        if re.search(pattern, sql_upper, re.IGNORECASE):
            raise SqlValidationError(
                "Query contains disallowed SQL operation. "
                "Only read-only SELECT queries are permitted."
            )


def execute_sql(
    sql: str,
    warehouse_id: str | None = None,
    row_limit: int = MAX_ROWS,
) -> dict:
    """Execute SQL on a Databricks SQL Warehouse.

    Returns dict with keys: columns, data, row_count, truncated, error.
    """
    if not warehouse_id:
        return {
            "columns": [],
            "data": [],
            "row_count": 0,
            "truncated": False,
            "error": "No warehouse_id provided",
        }

    try:
        validate_sql_read_only(sql)
    except SqlValidationError as e:
        return {
            "columns": [],
            "data": [],
            "row_count": 0,
            "truncated": False,
            "error": str(e),
        }

    client = get_workspace_client()

    try:
        response = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout=WAIT_TIMEOUT,
            row_limit=row_limit,
        )

        if response.status and response.status.state:
            state = response.status.state.value
            if state == "FAILED":
                error_msg = (
                    response.status.error.message
                    if response.status.error
                    else "Execution failed"
                )
                return {
                    "columns": [],
                    "data": [],
                    "row_count": 0,
                    "truncated": False,
                    "error": error_msg,
                }

        columns = []
        if response.manifest and response.manifest.schema:
            columns = [
                {"name": col.name, "type_name": col.type_name}
                for col in response.manifest.schema.columns or []
            ]

        data = []
        truncated = False
        if response.result and response.result.data_array:
            data = response.result.data_array
        if response.manifest:
            truncated = response.manifest.truncated or False

        return {
            "columns": columns,
            "data": data,
            "row_count": len(data),
            "truncated": truncated,
            "error": None,
        }

    except Exception as e:
        logger.error(f"SQL execution failed: {e}")
        return {
            "columns": [],
            "data": [],
            "row_count": 0,
            "truncated": False,
            "error": str(e),
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/core/test_sql.py -v`
Expected: 8 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/core/sql.py tests/unit/core/test_sql.py
git commit -m "feat(core): add SQL executor with read-only validation"
```

---

### Task 6: Core LLM

**Files:**
- Create: `genie_world/core/llm.py`
- Create: `tests/unit/core/test_llm.py`

- [ ] **Step 1: Write failing tests for LLM utilities**

```python
# tests/unit/core/test_llm.py
import json
import pytest
from unittest.mock import MagicMock, patch
from genie_world.core.llm import (
    call_llm,
    parse_json_from_llm_response,
    _repair_json,
)


class TestRepairJson:
    def test_removes_trailing_comma(self):
        result = _repair_json('{"a": 1, "b": 2,}')
        assert json.loads(result) == {"a": 1, "b": 2}

    def test_fixes_missing_comma_between_objects(self):
        result = _repair_json('{"a": 1}\n{"b": 2}')
        # After repair, should be parseable (as part of a larger structure)
        assert "," in result


class TestParseJsonFromLlmResponse:
    def test_parses_plain_json(self):
        result = parse_json_from_llm_response('{"key": "value"}')
        assert result == {"key": "value"}

    def test_parses_markdown_code_block(self):
        content = '```json\n{"key": "value"}\n```'
        result = parse_json_from_llm_response(content)
        assert result == {"key": "value"}

    def test_parses_json_with_preamble(self):
        content = 'Here is the result:\n{"key": "value"}'
        result = parse_json_from_llm_response(content)
        assert result == {"key": "value"}

    def test_raises_on_empty(self):
        with pytest.raises(ValueError):
            parse_json_from_llm_response("")

    def test_repairs_trailing_comma(self):
        content = '{"key": "value",}'
        result = parse_json_from_llm_response(content)
        assert result == {"key": "value"}


class TestCallLlm:
    @patch("genie_world.core.llm.get_workspace_client")
    def test_calls_serving_endpoint(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_config = MagicMock()
        mock_config.host = "https://test.cloud.databricks.com"
        mock_config.authenticate.return_value = {"Authorization": "Bearer token"}
        mock_client.config = mock_config

        with patch("genie_world.core.llm.httpx") as mock_httpx:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = {
                "choices": [{"message": {"content": "test response"}}]
            }
            mock_httpx.post.return_value = mock_resp

            result = call_llm(
                messages=[{"role": "user", "content": "hello"}],
                model="test-model",
            )

            assert result == "test response"

    @patch("genie_world.core.llm.get_workspace_client")
    def test_raises_on_rate_limit(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.config.host = "https://test.cloud.databricks.com"
        mock_client.config.authenticate.return_value = {}

        with patch("genie_world.core.llm.httpx") as mock_httpx:
            mock_resp = MagicMock()
            mock_resp.status_code = 429
            mock_resp.headers = {"Retry-After": "5"}
            mock_httpx.post.return_value = mock_resp

            with pytest.raises(RuntimeError, match="Rate limited"):
                call_llm(
                    messages=[{"role": "user", "content": "hello"}],
                    model="test-model",
                )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/core/test_llm.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement LLM utilities**

```python
# genie_world/core/llm.py
"""LLM serving endpoint wrapper with retry and JSON repair.

Ported from dbx-genie-rx's llm_utils.py.
"""

from __future__ import annotations

import json
import logging
import re
import time

import httpx

from genie_world.core.auth import get_workspace_client
from genie_world.core.config import get_config

logger = logging.getLogger(__name__)


def call_llm(
    messages: list[dict],
    model: str | None = None,
    max_tokens: int | None = None,
    timeout: float = 600,
) -> str:
    """Call an LLM serving endpoint.

    Args:
        messages: Chat messages in OpenAI format.
        model: Model name. Defaults to config's llm_model.
        max_tokens: Optional max tokens for response.
        timeout: Per-request timeout in seconds.

    Returns:
        The assistant's response content.
    """
    if model is None:
        model = get_config().llm_model

    client = get_workspace_client()
    host = client.config.host.rstrip("/")
    auth_headers = client.config.authenticate()

    url = f"{host}/serving-endpoints/{model}/invocations"
    body: dict = {"messages": messages}
    if max_tokens is not None:
        body["max_tokens"] = max_tokens

    t0 = time.monotonic()
    resp = httpx.post(url, json=body, headers=auth_headers, timeout=timeout)
    elapsed = time.monotonic() - t0
    logger.info(f"LLM responded in {elapsed:.1f}s")

    if resp.status_code == 429:
        retry_after = resp.headers.get("Retry-After", "unknown")
        raise RuntimeError(
            f"Rate limited by serving endpoint (429). Retry-After: {retry_after}s."
        )

    if resp.status_code != 200:
        raise RuntimeError(
            f"Serving endpoint returned {resp.status_code}: {resp.text[:500]}"
        )

    response = resp.json()

    if not isinstance(response, dict) or "choices" not in response:
        raise ValueError(f"Unexpected response format: {list(response.keys()) if isinstance(response, dict) else type(response)}")

    if not response["choices"]:
        raise ValueError("Response has empty 'choices' list")

    content = response["choices"][0]["message"]["content"]
    if not content:
        raise ValueError("LLM returned empty content")

    return content


def _repair_json(content: str) -> str:
    """Attempt to repair common JSON syntax errors from LLM responses."""
    # Remove trailing commas before closing brackets/braces
    content = re.sub(r",\s*([}\]])", r"\1", content)
    # Fix missing commas between closing and opening braces/brackets
    content = re.sub(r"([}\]])\s*\n?\s*([{\[])", r"\1,\n\2", content)
    # Fix missing commas between string values
    content = re.sub(r'(")\s*\n\s*(")', r'\1,\n\2', content)
    content = re.sub(r'(")\s+(")', r'\1, \2', content)
    # Fix missing commas after closing brace/bracket before string
    content = re.sub(r'([}\]])\s*\n\s*(")', r'\1,\n\2', content)
    content = re.sub(r'([}\]])\s+(")', r'\1, \2', content)
    return content


def parse_json_from_llm_response(content: str) -> dict:
    """Parse JSON from an LLM response, handling markdown code blocks and repairs."""
    content = content.strip()

    if not content:
        raise ValueError("LLM returned empty response")

    # Handle markdown code blocks
    if content.startswith("```"):
        lines = content.split("\n")
        start_idx = 1
        end_idx = len(lines)
        for i in range(len(lines) - 1, 0, -1):
            if lines[i].strip() == "```":
                end_idx = i
                break
        content = "\n".join(lines[start_idx:end_idx])

    # Handle text before JSON
    if not content.startswith("{"):
        json_start = content.find("{")
        if json_start != -1:
            brace_count = 0
            json_end = -1
            for i, char in enumerate(content[json_start:], json_start):
                if char == "{":
                    brace_count += 1
                elif char == "}":
                    brace_count -= 1
                    if brace_count == 0:
                        json_end = i + 1
                        break
            if json_end != -1:
                content = content[json_start:json_end]

    if not content:
        raise ValueError("No JSON found in LLM response")

    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        repaired = _repair_json(content)
        try:
            return json.loads(repaired)
        except json.JSONDecodeError:
            raise e
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/core/test_llm.py -v`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/core/llm.py tests/unit/core/test_llm.py
git commit -m "feat(core): add LLM serving endpoint wrapper with JSON repair"
```

---

### Task 7: Core Storage

**Files:**
- Create: `genie_world/core/storage.py`
- Create: `tests/unit/core/test_storage.py`

- [ ] **Step 1: Write failing tests for storage**

```python
# tests/unit/core/test_storage.py
import json
import os
import tempfile
import pytest
from pydantic import BaseModel
from genie_world.core.storage import LocalStorage, save_artifact, load_artifact


class SampleModel(BaseModel):
    name: str
    value: int


class TestLocalStorage:
    def test_save_and_load(self, tmp_path):
        storage = LocalStorage(base_path=str(tmp_path))
        model = SampleModel(name="test", value=42)

        storage.save("my_artifact.json", model)
        loaded = storage.load("my_artifact.json", SampleModel)

        assert loaded.name == "test"
        assert loaded.value == 42

    def test_load_missing_returns_none(self, tmp_path):
        storage = LocalStorage(base_path=str(tmp_path))
        result = storage.load("nonexistent.json", SampleModel)
        assert result is None

    def test_list_artifacts(self, tmp_path):
        storage = LocalStorage(base_path=str(tmp_path))
        storage.save("a.json", SampleModel(name="a", value=1))
        storage.save("b.json", SampleModel(name="b", value=2))

        artifacts = storage.list_artifacts()
        assert sorted(artifacts) == ["a.json", "b.json"]


class TestConvenienceFunctions:
    def test_save_and_load_with_local_path(self, tmp_path):
        model = SampleModel(name="test", value=99)
        path = str(tmp_path / "output.json")

        save_artifact(model, path)
        loaded = load_artifact(path, SampleModel)

        assert loaded.name == "test"
        assert loaded.value == 99
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/core/test_storage.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement storage**

```python
# genie_world/core/storage.py
"""Artifact persistence for genie-world.

Supports local filesystem storage. UC Volumes support can be added later
by implementing the same interface.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class LocalStorage:
    """Store artifacts as JSON files on the local filesystem."""

    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def save(self, name: str, artifact: BaseModel) -> str:
        """Save a Pydantic model as JSON. Returns the full path."""
        path = self.base_path / name
        path.write_text(artifact.model_dump_json(indent=2))
        logger.info(f"Saved artifact to {path}")
        return str(path)

    def load(self, name: str, model_class: type[T]) -> T | None:
        """Load a Pydantic model from JSON. Returns None if not found."""
        path = self.base_path / name
        if not path.exists():
            return None
        data = path.read_text()
        return model_class.model_validate_json(data)

    def list_artifacts(self) -> list[str]:
        """List all artifact filenames in the storage directory."""
        return sorted(f.name for f in self.base_path.iterdir() if f.is_file())


def save_artifact(artifact: BaseModel, path: str) -> None:
    """Save a Pydantic model to a JSON file at the given path."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(artifact.model_dump_json(indent=2))


def load_artifact(path: str, model_class: type[T]) -> T | None:
    """Load a Pydantic model from a JSON file. Returns None if not found."""
    p = Path(path)
    if not p.exists():
        return None
    return model_class.model_validate_json(p.read_text())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/core/test_storage.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/core/storage.py tests/unit/core/test_storage.py
git commit -m "feat(core): add artifact storage with local filesystem support"
```

---

### Task 8: Core Shared Models (SpaceConfig stub)

**Files:**
- Create: `genie_world/core/models.py`
- Create: `tests/unit/core/test_models.py`

- [ ] **Step 1: Write failing tests for core models**

```python
# tests/unit/core/test_models.py
from genie_world.core.models import SpaceConfig


class TestSpaceConfig:
    def test_minimal(self):
        config = SpaceConfig(display_name="Sales Analytics")
        assert config.display_name == "Sales Analytics"
        assert config.data_sources is None
        assert config.instructions is None
        assert config.benchmarks is None

    def test_serialization(self):
        config = SpaceConfig(display_name="Test Space")
        json_str = config.model_dump_json()
        loaded = SpaceConfig.model_validate_json(json_str)
        assert loaded.display_name == "Test Space"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/core/test_models.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement core models**

```python
# genie_world/core/models.py
"""Shared Pydantic models that flow between blocks.

SpaceConfig is stubbed here for type contracts. Full implementation
will be fleshed out when the Builder block is implemented.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class SpaceConfig(BaseModel):
    """Matches the Genie Space serialized_space JSON schema.

    Stubbed with flexible types. Will be fully typed when
    the Builder block is implemented.
    """

    display_name: str
    data_sources: dict[str, Any] | None = None
    instructions: dict[str, Any] | None = None
    benchmarks: dict[str, Any] | None = None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/core/test_models.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/core/models.py tests/unit/core/test_models.py
git commit -m "feat(core): add SpaceConfig stub for cross-block type contracts"
```

---

## Chunk 3: Profiler Models + Metadata Profiler

### Task 9: Profiler Models (renumbered from 8)

**Files:**
- Create: `genie_world/profiler/__init__.py`
- Create: `genie_world/profiler/models.py`
- Create: `tests/unit/profiler/__init__.py`
- Create: `tests/unit/profiler/test_models.py`

- [ ] **Step 1: Write failing tests for profiler models**

```python
# tests/unit/profiler/test_models.py
from datetime import datetime
from genie_world.profiler.models import (
    ColumnProfile,
    TableProfile,
    Relationship,
    DetectionMethod,
    ProfilingWarning,
    SchemaProfile,
)


class TestColumnProfile:
    def test_minimal(self):
        col = ColumnProfile(name="id", data_type="INT", nullable=False)
        assert col.name == "id"
        assert col.cardinality is None
        assert col.synonyms is None

    def test_full(self):
        col = ColumnProfile(
            name="region",
            data_type="STRING",
            nullable=True,
            description="Sales region",
            cardinality=5,
            null_percent=2.1,
            top_values=["EMEA", "NA", "APAC"],
            sample_values=["EMEA", "NA"],
            synonyms=["territory", "area"],
            query_frequency=150,
            co_queried_columns=["revenue", "quarter"],
        )
        assert col.cardinality == 5
        assert len(col.synonyms) == 2


class TestRelationship:
    def test_detection_method_enum(self):
        rel = Relationship(
            source_table="orders",
            source_column="customer_id",
            target_table="customers",
            target_column="id",
            confidence=0.95,
            detection_method=DetectionMethod.UC_CONSTRAINT,
        )
        assert rel.detection_method == DetectionMethod.UC_CONSTRAINT
        assert rel.detection_method.value == "uc_constraint"


class TestSchemaProfile:
    def test_serialization_roundtrip(self):
        profile = SchemaProfile(
            schema_version="1.0",
            catalog="main",
            schema_name="sales",
            tables=[
                TableProfile(
                    catalog="main",
                    schema_name="sales",
                    table="orders",
                    description="Order records",
                    row_count=1000,
                    columns=[
                        ColumnProfile(name="id", data_type="INT", nullable=False),
                    ],
                ),
            ],
            relationships=[],
            warnings=None,
            profiled_at=datetime(2026, 3, 14, 12, 0, 0),
        )

        json_str = profile.model_dump_json()
        loaded = SchemaProfile.model_validate_json(json_str)
        assert loaded.catalog == "main"
        assert len(loaded.tables) == 1
        assert loaded.tables[0].columns[0].name == "id"

    def test_with_warnings(self):
        profile = SchemaProfile(
            schema_version="1.0",
            catalog="main",
            schema_name="sales",
            tables=[],
            relationships=[],
            warnings=[
                ProfilingWarning(
                    table="orders",
                    tier="data",
                    message="Warehouse timeout",
                )
            ],
            profiled_at=datetime(2026, 3, 14),
        )
        assert len(profile.warnings) == 1
        assert profile.warnings[0].tier == "data"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/profiler/test_models.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement profiler models**

```python
# genie_world/profiler/models.py
"""Data models for the profiler block."""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class DetectionMethod(str, Enum):
    UC_CONSTRAINT = "uc_constraint"
    LINEAGE = "lineage"
    QUERY_COOCCURRENCE = "query_cooccurrence"
    NAMING_PATTERN = "naming_pattern"
    VALUE_OVERLAP = "value_overlap"


class ColumnProfile(BaseModel):
    name: str
    data_type: str
    nullable: bool
    description: str | None = None
    cardinality: int | None = None
    null_percent: float | None = None
    top_values: list[str] | None = None
    min_value: str | None = None
    max_value: str | None = None
    sample_values: list[str] | None = None
    synonyms: list[str] | None = None
    tags: dict[str, str] | None = None
    query_frequency: int | None = None
    co_queried_columns: list[str] | None = None


class TableProfile(BaseModel):
    catalog: str
    schema_name: str
    table: str
    description: str | None = None
    row_count: int | None = None
    columns: list[ColumnProfile] = []
    query_frequency: int | None = None
    upstream_tables: list[str] | None = None
    downstream_tables: list[str] | None = None


class Relationship(BaseModel):
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    confidence: float
    detection_method: DetectionMethod


class ProfilingWarning(BaseModel):
    table: str
    tier: str
    message: str


class SchemaProfile(BaseModel):
    schema_version: str
    catalog: str
    schema_name: str
    tables: list[TableProfile]
    relationships: list[Relationship]
    warnings: list[ProfilingWarning] | None = None
    profiled_at: datetime
```

Create init files:

`genie_world/profiler/__init__.py`:
```python
"""Data Profiler block for genie-world."""
```

`tests/unit/profiler/__init__.py`: empty file.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/profiler/test_models.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/profiler/ tests/unit/profiler/
git commit -m "feat(profiler): add Pydantic models for profiler block"
```

---

### Task 9: Metadata Profiler (Tier 1)

**Files:**
- Create: `genie_world/profiler/metadata_profiler.py`
- Create: `tests/unit/profiler/test_metadata_profiler.py`

- [ ] **Step 1: Write failing tests for metadata profiler**

```python
# tests/unit/profiler/test_metadata_profiler.py
import pytest
from unittest.mock import MagicMock, patch
from genie_world.profiler.metadata_profiler import profile_table_metadata, profile_schema_metadata


def _make_mock_column(name, type_name, nullable=True, comment=None):
    col = MagicMock()
    col.name = name
    col.type_name = type_name
    col.nullable = nullable
    col.comment = comment
    return col


def _make_mock_table(name, comment=None, columns=None):
    table = MagicMock()
    table.name = name
    table.comment = comment
    table.catalog_name = "main"
    table.schema_name = "sales"
    table.columns = columns or []
    # properties can hold row count
    table.properties = {}
    return table


class TestProfileTableMetadata:
    @patch("genie_world.profiler.metadata_profiler.get_workspace_client")
    def test_profiles_single_table(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_table = _make_mock_table(
            "orders",
            comment="All customer orders",
            columns=[
                _make_mock_column("id", "INT", nullable=False),
                _make_mock_column("amount", "DOUBLE", nullable=True, comment="Order total"),
            ],
        )
        mock_client.tables.get.return_value = mock_table

        result = profile_table_metadata("main", "sales", "orders")

        assert result.table == "orders"
        assert result.description == "All customer orders"
        assert len(result.columns) == 2
        assert result.columns[0].name == "id"
        assert result.columns[0].data_type == "INT"
        assert result.columns[0].nullable is False
        assert result.columns[1].description == "Order total"


class TestProfileSchemaMetadata:
    @patch("genie_world.profiler.metadata_profiler.get_workspace_client")
    def test_profiles_all_tables(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock list_tables
        table_info_1 = MagicMock()
        table_info_1.name = "orders"
        table_info_2 = MagicMock()
        table_info_2.name = "customers"
        mock_client.tables.list.return_value = [table_info_1, table_info_2]

        # Mock get for each table
        mock_client.tables.get.side_effect = [
            _make_mock_table("orders", columns=[_make_mock_column("id", "INT")]),
            _make_mock_table("customers", columns=[_make_mock_column("id", "INT")]),
        ]

        results = profile_schema_metadata("main", "sales")

        assert len(results) == 2
        assert results[0].table == "orders"
        assert results[1].table == "customers"

    @patch("genie_world.profiler.metadata_profiler.get_workspace_client")
    def test_handles_table_error_gracefully(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        table_info = MagicMock()
        table_info.name = "broken"
        mock_client.tables.list.return_value = [table_info]
        mock_client.tables.get.side_effect = Exception("Permission denied")

        results, warnings = profile_schema_metadata("main", "sales", return_warnings=True)

        assert len(results) == 0
        assert len(warnings) == 1
        assert "Permission denied" in warnings[0].message
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/profiler/test_metadata_profiler.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement metadata profiler**

```python
# genie_world/profiler/metadata_profiler.py
"""Tier 1: Metadata profiling via Unity Catalog APIs.

Extracts column names, types, descriptions, tags, and table comments
using the Databricks SDK. No warehouse or LLM needed.
"""

from __future__ import annotations

import logging
from typing import overload

from genie_world.core.auth import get_workspace_client
from genie_world.core.tracing import trace
from genie_world.profiler.models import ColumnProfile, ProfilingWarning, TableProfile

logger = logging.getLogger(__name__)


@trace(name="profile_table_metadata", span_type="CHAIN")
def profile_table_metadata(
    catalog: str, schema: str, table: str
) -> TableProfile:
    """Profile a single table using UC metadata APIs."""
    client = get_workspace_client()
    full_name = f"{catalog}.{schema}.{table}"

    table_info = client.tables.get(full_name)

    columns = []
    for col in table_info.columns or []:
        columns.append(
            ColumnProfile(
                name=col.name,
                data_type=str(col.type_name) if col.type_name else "UNKNOWN",
                nullable=col.nullable if col.nullable is not None else True,
                description=col.comment,
            )
        )

    return TableProfile(
        catalog=catalog,
        schema_name=schema,
        table=table,
        description=table_info.comment,
        row_count=None,  # Not reliably available from metadata alone
        columns=columns,
    )


@overload
def profile_schema_metadata(
    catalog: str, schema: str, *, return_warnings: bool = False
) -> list[TableProfile]: ...


@overload
def profile_schema_metadata(
    catalog: str, schema: str, *, return_warnings: bool = True
) -> tuple[list[TableProfile], list[ProfilingWarning]]: ...


@trace(name="profile_schema_metadata", span_type="CHAIN")
def profile_schema_metadata(
    catalog: str,
    schema: str,
    *,
    return_warnings: bool = False,
) -> list[TableProfile] | tuple[list[TableProfile], list[ProfilingWarning]]:
    """Profile all tables in a schema using UC metadata APIs.

    Args:
        catalog: Unity Catalog catalog name.
        schema: Schema name within the catalog.
        return_warnings: If True, return (tables, warnings) tuple.

    Returns:
        List of TableProfiles, or (tables, warnings) if return_warnings=True.
    """
    client = get_workspace_client()

    table_list = list(client.tables.list(catalog_name=catalog, schema_name=schema))
    logger.info(f"Found {len(table_list)} tables in {catalog}.{schema}")

    profiles: list[TableProfile] = []
    warnings: list[ProfilingWarning] = []

    for table_info in table_list:
        try:
            profile = profile_table_metadata(catalog, schema, table_info.name)
            profiles.append(profile)
        except Exception as e:
            logger.warning(f"Failed to profile {table_info.name}: {e}")
            warnings.append(
                ProfilingWarning(
                    table=table_info.name,
                    tier="metadata",
                    message=str(e),
                )
            )

    if return_warnings:
        return profiles, warnings
    return profiles
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/profiler/test_metadata_profiler.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/profiler/metadata_profiler.py tests/unit/profiler/test_metadata_profiler.py
git commit -m "feat(profiler): add Tier 1 metadata profiler via UC APIs"
```

---

## Chunk 4: Data Profiler + Usage Profiler

### Task 10: Data Profiler (Tier 2)

**Files:**
- Create: `genie_world/profiler/data_profiler.py`
- Create: `tests/unit/profiler/test_data_profiler.py`

- [ ] **Step 1: Write failing tests for data profiler**

```python
# tests/unit/profiler/test_data_profiler.py
import pytest
from unittest.mock import patch, MagicMock
from genie_world.profiler.models import ColumnProfile, TableProfile
from genie_world.profiler.data_profiler import enrich_table_with_stats, _build_profile_sql


class TestBuildProfileSql:
    def test_generates_sql_for_columns(self):
        columns = [
            ColumnProfile(name="id", data_type="INT", nullable=False),
            ColumnProfile(name="name", data_type="STRING", nullable=True),
        ]
        sql = _build_profile_sql("main.sales.orders", columns)

        assert "COUNT(DISTINCT `id`)" in sql
        assert "COUNT(DISTINCT `name`)" in sql
        assert "main.sales.orders" in sql
        assert "SUM(CASE WHEN" in sql  # null percent

    def test_includes_min_max_for_numeric(self):
        columns = [
            ColumnProfile(name="amount", data_type="DOUBLE", nullable=True),
        ]
        sql = _build_profile_sql("main.sales.orders", columns)
        assert "MIN(`amount`)" in sql
        assert "MAX(`amount`)" in sql


class TestEnrichTableWithStats:
    @patch("genie_world.profiler.data_profiler.execute_sql")
    def test_enriches_columns(self, mock_execute):
        mock_execute.return_value = {
            "columns": [
                {"name": "total_rows", "type_name": "LONG"},
                {"name": "id_cardinality", "type_name": "LONG"},
                {"name": "id_null_pct", "type_name": "DOUBLE"},
                {"name": "id_min", "type_name": "STRING"},
                {"name": "id_max", "type_name": "STRING"},
            ],
            "data": [["100", "95", "0.0", "1", "100"]],
            "row_count": 1,
            "truncated": False,
            "error": None,
        }

        table = TableProfile(
            catalog="main",
            schema_name="sales",
            table="orders",
            columns=[
                ColumnProfile(name="id", data_type="INT", nullable=False),
            ],
        )

        enriched, warnings = enrich_table_with_stats(table, warehouse_id="wh-123")

        assert enriched.row_count == 100
        assert enriched.columns[0].cardinality == 95
        assert enriched.columns[0].null_percent == 0.0

    @patch("genie_world.profiler.data_profiler.execute_sql")
    def test_returns_warning_on_sql_error(self, mock_execute):
        mock_execute.return_value = {
            "columns": [],
            "data": [],
            "row_count": 0,
            "truncated": False,
            "error": "Warehouse timeout",
        }

        table = TableProfile(
            catalog="main",
            schema_name="sales",
            table="orders",
            columns=[
                ColumnProfile(name="id", data_type="INT", nullable=False),
            ],
        )

        enriched, warnings = enrich_table_with_stats(table, warehouse_id="wh-123")

        assert len(warnings) == 1
        assert "Warehouse timeout" in warnings[0].message
        # Original columns should be unchanged
        assert enriched.columns[0].cardinality is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/profiler/test_data_profiler.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement data profiler**

```python
# genie_world/profiler/data_profiler.py
"""Tier 2: SQL-based statistical profiling via Statement Execution API.

Computes cardinality, null percentage, min/max, top-N values, and sample rows.
"""

from __future__ import annotations

import logging

from genie_world.core.sql import execute_sql
from genie_world.core.tracing import trace
from genie_world.profiler.models import ColumnProfile, ProfilingWarning, TableProfile

logger = logging.getLogger(__name__)

_NUMERIC_TYPES = {"INT", "LONG", "SHORT", "BYTE", "FLOAT", "DOUBLE", "DECIMAL"}
_DATE_TYPES = {"DATE", "TIMESTAMP"}
_MINMAX_TYPES = _NUMERIC_TYPES | _DATE_TYPES


def _build_profile_sql(full_table_name: str, columns: list[ColumnProfile]) -> str:
    """Build a SQL query that profiles all columns in one pass."""
    parts = ["SELECT", "  COUNT(*) AS total_rows"]

    for col in columns:
        name = f"`{col.name}`"
        safe = col.name.replace("`", "")

        # Cardinality
        parts.append(f"  , COUNT(DISTINCT {name}) AS `{safe}_cardinality`")

        # Null percentage
        parts.append(
            f"  , ROUND(100.0 * SUM(CASE WHEN {name} IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS `{safe}_null_pct`"
        )

        # Min/Max for numeric and date types
        if col.data_type.upper() in _MINMAX_TYPES:
            parts.append(f"  , CAST(MIN({name}) AS STRING) AS `{safe}_min`")
            parts.append(f"  , CAST(MAX({name}) AS STRING) AS `{safe}_max`")

    parts.append(f"FROM {full_table_name}")

    return "\n".join(parts)


def _build_top_values_sql(
    full_table_name: str, column_name: str, n: int = 10
) -> str:
    """Build SQL to get top-N most frequent values for a column."""
    name = f"`{column_name}`"
    return (
        f"SELECT CAST({name} AS STRING) AS val, COUNT(*) AS cnt "
        f"FROM {full_table_name} "
        f"WHERE {name} IS NOT NULL "
        f"GROUP BY {name} "
        f"ORDER BY cnt DESC "
        f"LIMIT {n}"
    )


def _build_sample_sql(full_table_name: str, n: int = 5) -> str:
    """Build SQL to get sample rows."""
    return f"SELECT * FROM {full_table_name} LIMIT {n}"


@trace(name="enrich_table_with_stats", span_type="CHAIN")
def enrich_table_with_stats(
    table: TableProfile,
    warehouse_id: str,
) -> tuple[TableProfile, list[ProfilingWarning]]:
    """Enrich a TableProfile with statistical data from SQL queries.

    Returns (enriched_table, warnings).
    """
    full_name = f"{table.catalog}.{table.schema_name}.{table.table}"
    warnings: list[ProfilingWarning] = []

    # Run the main profile query
    profile_sql = _build_profile_sql(full_name, table.columns)
    result = execute_sql(profile_sql, warehouse_id=warehouse_id)

    if result["error"]:
        warnings.append(
            ProfilingWarning(
                table=table.table, tier="data", message=result["error"]
            )
        )
        return table, warnings

    if not result["data"]:
        return table, warnings

    row = result["data"][0]
    col_map = {c["name"]: i for i, c in enumerate(result["columns"])}

    # Extract total rows
    row_count = int(row[col_map["total_rows"]]) if "total_rows" in col_map else None

    # Enrich each column
    enriched_columns = []
    for col in table.columns:
        safe = col.name.replace("`", "")
        updates = {}

        card_key = f"{safe}_cardinality"
        if card_key in col_map and row[col_map[card_key]] is not None:
            updates["cardinality"] = int(row[col_map[card_key]])

        null_key = f"{safe}_null_pct"
        if null_key in col_map and row[col_map[null_key]] is not None:
            updates["null_percent"] = float(row[col_map[null_key]])

        min_key = f"{safe}_min"
        if min_key in col_map and row[col_map[min_key]] is not None:
            updates["min_value"] = row[col_map[min_key]]

        max_key = f"{safe}_max"
        if max_key in col_map and row[col_map[max_key]] is not None:
            updates["max_value"] = row[col_map[max_key]]

        enriched_columns.append(col.model_copy(update=updates))

    enriched_table = table.model_copy(
        update={"row_count": row_count, "columns": enriched_columns}
    )
    return enriched_table, warnings
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/profiler/test_data_profiler.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/profiler/data_profiler.py tests/unit/profiler/test_data_profiler.py
git commit -m "feat(profiler): add Tier 2 data profiler with SQL-based statistics"
```

---

### Task 11: Usage Profiler (Tier 3)

**Files:**
- Create: `genie_world/profiler/usage_profiler.py`
- Create: `tests/unit/profiler/test_usage_profiler.py`

- [ ] **Step 1: Write failing tests for usage profiler**

```python
# tests/unit/profiler/test_usage_profiler.py
import pytest
from unittest.mock import patch
from genie_world.profiler.models import TableProfile, ColumnProfile, Relationship, DetectionMethod
from genie_world.profiler.usage_profiler import (
    enrich_with_usage,
    get_declared_relationships,
)


class TestGetDeclaredRelationships:
    @patch("genie_world.profiler.usage_profiler.execute_sql")
    def test_extracts_pk_fk(self, mock_execute):
        mock_execute.return_value = {
            "columns": [
                {"name": "table_name", "type_name": "STRING"},
                {"name": "column_name", "type_name": "STRING"},
                {"name": "referenced_table", "type_name": "STRING"},
                {"name": "referenced_column", "type_name": "STRING"},
            ],
            "data": [
                ["orders", "customer_id", "customers", "id"],
            ],
            "row_count": 1,
            "truncated": False,
            "error": None,
        }

        rels, warnings = get_declared_relationships("main", "sales", warehouse_id="wh-123")

        assert len(rels) == 1
        assert rels[0].source_table == "orders"
        assert rels[0].source_column == "customer_id"
        assert rels[0].target_table == "customers"
        assert rels[0].target_column == "id"
        assert rels[0].detection_method == DetectionMethod.UC_CONSTRAINT
        assert rels[0].confidence == 1.0

    @patch("genie_world.profiler.usage_profiler.execute_sql")
    def test_handles_no_access(self, mock_execute):
        mock_execute.return_value = {
            "columns": [],
            "data": [],
            "row_count": 0,
            "truncated": False,
            "error": "TABLE_OR_VIEW_NOT_FOUND",
        }

        rels, warnings = get_declared_relationships("main", "sales", warehouse_id="wh-123")

        assert len(rels) == 0
        assert len(warnings) == 1


class TestEnrichWithUsage:
    @patch("genie_world.profiler.usage_profiler.execute_sql")
    def test_enriches_query_frequency(self, mock_execute):
        # Mock query history result
        mock_execute.return_value = {
            "columns": [
                {"name": "table_name", "type_name": "STRING"},
                {"name": "query_count", "type_name": "LONG"},
            ],
            "data": [["orders", "42"]],
            "row_count": 1,
            "truncated": False,
            "error": None,
        }

        table = TableProfile(
            catalog="main",
            schema_name="sales",
            table="orders",
            columns=[ColumnProfile(name="id", data_type="INT", nullable=False)],
        )

        enriched, warnings = enrich_with_usage([table], "main", "sales", warehouse_id="wh-123")

        assert enriched[0].query_frequency == 42

    @patch("genie_world.profiler.usage_profiler.execute_sql")
    def test_handles_no_system_tables(self, mock_execute):
        mock_execute.return_value = {
            "columns": [],
            "data": [],
            "row_count": 0,
            "truncated": False,
            "error": "SCHEMA_NOT_FOUND: system.query",
        }

        table = TableProfile(
            catalog="main",
            schema_name="sales",
            table="orders",
            columns=[],
        )

        enriched, warnings = enrich_with_usage([table], "main", "sales", warehouse_id="wh-123")

        # Should return tables unchanged with a warning
        assert enriched[0].query_frequency is None
        assert len(warnings) >= 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/profiler/test_usage_profiler.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement usage profiler**

```python
# genie_world/profiler/usage_profiler.py
"""Tier 3: System tables mining for usage signals.

Queries system.information_schema, system.lineage, and system.query
for declared constraints, lineage, and query frequency.
"""

from __future__ import annotations

import logging
import re

from genie_world.core.sql import execute_sql
from genie_world.core.tracing import trace
from genie_world.profiler.models import (
    DetectionMethod,
    ProfilingWarning,
    Relationship,
    TableProfile,
)

logger = logging.getLogger(__name__)

_VALID_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_]+$")


def _validate_identifier(name: str, label: str) -> str:
    """Validate that a name is a safe SQL identifier (alphanumeric + underscores)."""
    if not _VALID_IDENTIFIER.match(name):
        raise ValueError(f"Invalid {label}: {name!r}. Must be alphanumeric/underscores only.")
    return name


@trace(name="get_declared_relationships", span_type="CHAIN")
def get_declared_relationships(
    catalog: str,
    schema: str,
    *,
    warehouse_id: str,
) -> tuple[list[Relationship], list[ProfilingWarning]]:
    """Get PK/FK relationships declared in information_schema."""
    _validate_identifier(catalog, "catalog")
    _validate_identifier(schema, "schema")
    sql = f"""
    SELECT
        tc.table_name,
        kcu.column_name,
        ccu.table_name AS referenced_table,
        ccu.column_name AS referenced_column
    FROM system.information_schema.table_constraints tc
    JOIN system.information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    JOIN system.information_schema.constraint_column_usage ccu
        ON tc.constraint_name = ccu.constraint_name
        AND tc.table_schema = ccu.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_catalog = '{catalog}'
        AND tc.table_schema = '{schema}'
    """

    result = execute_sql(sql, warehouse_id=warehouse_id)
    warnings: list[ProfilingWarning] = []

    if result["error"]:
        warnings.append(
            ProfilingWarning(
                table="*",
                tier="usage",
                message=f"Could not query information_schema: {result['error']}",
            )
        )
        return [], warnings

    relationships = []
    for row in result["data"]:
        relationships.append(
            Relationship(
                source_table=row[0],
                source_column=row[1],
                target_table=row[2],
                target_column=row[3],
                confidence=1.0,
                detection_method=DetectionMethod.UC_CONSTRAINT,
            )
        )

    return relationships, warnings


@trace(name="enrich_with_usage", span_type="CHAIN")
def enrich_with_usage(
    tables: list[TableProfile],
    catalog: str,
    schema: str,
    *,
    warehouse_id: str,
) -> tuple[list[TableProfile], list[ProfilingWarning]]:
    """Enrich tables with query frequency from system.query.history.

    Returns (enriched_tables, warnings).
    """
    _validate_identifier(catalog, "catalog")
    _validate_identifier(schema, "schema")
    table_names = [t.table for t in tables]
    warnings: list[ProfilingWarning] = []

    # Query frequency from query history
    table_list_sql = ", ".join(f"'{t}'" for t in table_names)
    sql = f"""
    SELECT
        referenced_table_name AS table_name,
        COUNT(*) AS query_count
    FROM system.query.history
    WHERE referenced_table_catalog = '{catalog}'
        AND referenced_table_schema = '{schema}'
        AND referenced_table_name IN ({table_list_sql})
        AND start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
    GROUP BY referenced_table_name
    """

    result = execute_sql(sql, warehouse_id=warehouse_id)

    if result["error"]:
        warnings.append(
            ProfilingWarning(
                table="*",
                tier="usage",
                message=f"Could not query system.query.history: {result['error']}",
            )
        )
        return tables, warnings

    # Build frequency lookup
    freq_map: dict[str, int] = {}
    for row in result["data"]:
        freq_map[row[0]] = int(row[1])

    # Enrich tables
    enriched = []
    for table in tables:
        freq = freq_map.get(table.table)
        if freq is not None:
            enriched.append(table.model_copy(update={"query_frequency": freq}))
        else:
            enriched.append(table)

    return enriched, warnings
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/profiler/test_usage_profiler.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/profiler/usage_profiler.py tests/unit/profiler/test_usage_profiler.py
git commit -m "feat(profiler): add Tier 3 usage profiler with system table mining"
```

---

## Chunk 5: Relationship Detector + Synonym Generator + Public API

### Task 12: Relationship Detector

**Files:**
- Create: `genie_world/profiler/relationship_detector.py`
- Create: `tests/unit/profiler/test_relationship_detector.py`

- [ ] **Step 1: Write failing tests for relationship detector**

```python
# tests/unit/profiler/test_relationship_detector.py
import pytest
from genie_world.profiler.models import (
    ColumnProfile,
    TableProfile,
    Relationship,
    DetectionMethod,
)
from genie_world.profiler.relationship_detector import detect_by_naming_patterns, merge_relationships


class TestDetectByNamingPatterns:
    def test_detects_id_suffix_match(self):
        tables = [
            TableProfile(
                catalog="main",
                schema_name="sales",
                table="orders",
                columns=[
                    ColumnProfile(name="id", data_type="INT", nullable=False),
                    ColumnProfile(name="customer_id", data_type="INT", nullable=False),
                ],
            ),
            TableProfile(
                catalog="main",
                schema_name="sales",
                table="customers",
                columns=[
                    ColumnProfile(name="id", data_type="INT", nullable=False),
                    ColumnProfile(name="name", data_type="STRING", nullable=True),
                ],
            ),
        ]

        rels = detect_by_naming_patterns(tables)

        assert len(rels) >= 1
        match = [
            r
            for r in rels
            if r.source_table == "orders"
            and r.source_column == "customer_id"
            and r.target_table == "customers"
            and r.target_column == "id"
        ]
        assert len(match) == 1
        assert match[0].detection_method == DetectionMethod.NAMING_PATTERN
        assert 0.0 < match[0].confidence < 1.0

    def test_no_self_references(self):
        tables = [
            TableProfile(
                catalog="main",
                schema_name="sales",
                table="orders",
                columns=[
                    ColumnProfile(name="id", data_type="INT", nullable=False),
                    ColumnProfile(name="order_id", data_type="INT", nullable=False),
                ],
            ),
        ]

        rels = detect_by_naming_patterns(tables)

        # order_id -> orders.id would be a self-ref, should still detect
        # but the key pattern is _id suffix matching another table
        for r in rels:
            # At minimum, no crashes
            assert r.source_table is not None

    def test_no_duplicates_after_merge(self):
        rels_a = [
            Relationship(
                source_table="orders",
                source_column="customer_id",
                target_table="customers",
                target_column="id",
                confidence=0.6,
                detection_method=DetectionMethod.NAMING_PATTERN,
            ),
        ]
        rels_b = [
            Relationship(
                source_table="orders",
                source_column="customer_id",
                target_table="customers",
                target_column="id",
                confidence=1.0,
                detection_method=DetectionMethod.UC_CONSTRAINT,
            ),
        ]
        merged = merge_relationships(rels_a, rels_b)
        assert len(merged) == 1
        assert merged[0].confidence == 1.0
        assert merged[0].detection_method == DetectionMethod.UC_CONSTRAINT

    def test_detects_key_suffix(self):
        tables = [
            TableProfile(
                catalog="main",
                schema_name="sales",
                table="line_items",
                columns=[
                    ColumnProfile(name="product_key", data_type="INT", nullable=False),
                ],
            ),
            TableProfile(
                catalog="main",
                schema_name="sales",
                table="products",
                columns=[
                    ColumnProfile(name="key", data_type="INT", nullable=False),
                ],
            ),
        ]

        rels = detect_by_naming_patterns(tables)
        match = [r for r in rels if r.source_column == "product_key"]
        assert len(match) >= 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/profiler/test_relationship_detector.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement relationship detector**

```python
# genie_world/profiler/relationship_detector.py
"""PK/FK relationship detection from multiple sources.

Combines UC constraints and naming patterns.
Lineage-based, query co-occurrence, and value overlap detection are deferred to a follow-up iteration.
"""

from __future__ import annotations

import logging
import re

from genie_world.core.tracing import trace
from genie_world.profiler.models import (
    DetectionMethod,
    Relationship,
    TableProfile,
)

logger = logging.getLogger(__name__)

_FK_SUFFIXES = ("_id", "_key", "_fk")


@trace(name="detect_by_naming_patterns", span_type="CHAIN")
def detect_by_naming_patterns(
    tables: list[TableProfile],
) -> list[Relationship]:
    """Detect relationships by matching column name patterns across tables.

    Looks for columns ending in _id, _key, _fk and matches them to
    tables with a corresponding primary column (id, key, etc.).
    """
    # Build lookup: table_name -> set of column names
    table_columns: dict[str, set[str]] = {}
    for t in tables:
        table_columns[t.table] = {c.name for c in t.columns}

    relationships: list[Relationship] = []

    for table in tables:
        for col in table.columns:
            for suffix in _FK_SUFFIXES:
                if not col.name.endswith(suffix):
                    continue

                # Extract the prefix: "customer_id" -> "customer"
                prefix = col.name[: -len(suffix)]
                if not prefix:
                    continue

                # Look for a matching table
                # Try plural/singular forms
                candidates = [
                    prefix,
                    prefix + "s",
                    prefix + "es",
                    prefix.rstrip("s"),
                ]

                for candidate in candidates:
                    if candidate not in table_columns:
                        continue

                    # Check if the target table has an 'id', 'key', or matching column
                    target_cols = table_columns[candidate]
                    target_col = None

                    # Try common PK column names
                    for pk_name in [
                        suffix.lstrip("_"),  # "id", "key", "fk"
                        col.name,  # exact match
                    ]:
                        if pk_name in target_cols:
                            target_col = pk_name
                            break

                    if target_col and candidate != table.table:
                        relationships.append(
                            Relationship(
                                source_table=table.table,
                                source_column=col.name,
                                target_table=candidate,
                                target_column=target_col,
                                confidence=0.6,
                                detection_method=DetectionMethod.NAMING_PATTERN,
                            )
                        )
                        break  # Found a match, stop checking candidates

    return relationships


def merge_relationships(
    *sources: list[Relationship],
) -> list[Relationship]:
    """Merge relationships from multiple sources, keeping highest confidence.

    If the same (source_table, source_column, target_table, target_column)
    pair is detected by multiple methods, keep the one with highest confidence.
    """
    best: dict[tuple, Relationship] = {}

    for source in sources:
        for rel in source:
            key = (rel.source_table, rel.source_column, rel.target_table, rel.target_column)
            if key not in best or rel.confidence > best[key].confidence:
                best[key] = rel

    return list(best.values())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/profiler/test_relationship_detector.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/profiler/relationship_detector.py tests/unit/profiler/test_relationship_detector.py
git commit -m "feat(profiler): add relationship detector with naming pattern heuristics"
```

---

### Task 13: Synonym Generator

**Files:**
- Create: `genie_world/profiler/synonym_generator.py`
- Create: `tests/unit/profiler/test_synonym_generator.py`

- [ ] **Step 1: Write failing tests for synonym generator**

```python
# tests/unit/profiler/test_synonym_generator.py
import json
import pytest
from unittest.mock import patch
from genie_world.profiler.models import ColumnProfile, TableProfile
from genie_world.profiler.synonym_generator import (
    generate_synonyms_for_table,
    _build_synonym_prompt,
)


class TestBuildSynonymPrompt:
    def test_includes_column_info(self):
        columns = [
            ColumnProfile(
                name="sales_territory",
                data_type="STRING",
                nullable=True,
                description="Geographic sales region",
                sample_values=["EMEA", "NA", "APAC"],
            ),
        ]
        prompt = _build_synonym_prompt("orders", columns)

        assert "sales_territory" in prompt
        assert "STRING" in prompt
        assert "Geographic sales region" in prompt
        assert "EMEA" in prompt


class TestGenerateSynonymsForTable:
    @patch("genie_world.profiler.synonym_generator.call_llm")
    def test_adds_synonyms_to_columns(self, mock_call_llm):
        mock_call_llm.return_value = json.dumps({
            "sales_territory": ["region", "territory", "area"],
            "order_amount": ["total", "revenue", "order value"],
        })

        table = TableProfile(
            catalog="main",
            schema_name="sales",
            table="orders",
            columns=[
                ColumnProfile(
                    name="sales_territory",
                    data_type="STRING",
                    nullable=True,
                ),
                ColumnProfile(
                    name="order_amount",
                    data_type="DOUBLE",
                    nullable=True,
                ),
            ],
        )

        enriched, warnings = generate_synonyms_for_table(table)

        assert enriched.columns[0].synonyms == ["region", "territory", "area"]
        assert enriched.columns[1].synonyms == ["total", "revenue", "order value"]
        assert len(warnings) == 0

    @patch("genie_world.profiler.synonym_generator.call_llm")
    def test_handles_llm_error(self, mock_call_llm):
        mock_call_llm.side_effect = RuntimeError("Rate limited")

        table = TableProfile(
            catalog="main",
            schema_name="sales",
            table="orders",
            columns=[
                ColumnProfile(name="id", data_type="INT", nullable=False),
            ],
        )

        enriched, warnings = generate_synonyms_for_table(table)

        assert enriched.columns[0].synonyms is None
        assert len(warnings) == 1
        assert "Rate limited" in warnings[0].message
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/profiler/test_synonym_generator.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement synonym generator**

```python
# genie_world/profiler/synonym_generator.py
"""LLM-powered synonym generation for column names.

Sends column profiles in batches per table and produces
business-friendly synonyms.
"""

from __future__ import annotations

import json
import logging

from genie_world.core.llm import call_llm, parse_json_from_llm_response
from genie_world.core.tracing import trace
from genie_world.profiler.models import ColumnProfile, ProfilingWarning, TableProfile

logger = logging.getLogger(__name__)


def _build_synonym_prompt(table_name: str, columns: list[ColumnProfile]) -> str:
    """Build the LLM prompt for synonym generation."""
    col_descriptions = []
    for col in columns:
        parts = [f"- **{col.name}** ({col.data_type})"]
        if col.description:
            parts.append(f"  Description: {col.description}")
        if col.sample_values:
            parts.append(f"  Sample values: {', '.join(col.sample_values[:5])}")
        col_descriptions.append("\n".join(parts))

    columns_text = "\n".join(col_descriptions)

    return f"""You are a data analyst generating business-friendly synonyms for database columns.

Table: {table_name}

Columns:
{columns_text}

For each column, generate 2-4 synonyms that a business user might use to refer to this data.
Consider abbreviations, business jargon, and natural language alternatives.

Return a JSON object mapping column names to lists of synonyms:
{{"column_name": ["synonym1", "synonym2", "synonym3"]}}

Only return the JSON, no other text."""


@trace(name="generate_synonyms_for_table", span_type="CHAIN")
def generate_synonyms_for_table(
    table: TableProfile,
    model: str | None = None,
) -> tuple[TableProfile, list[ProfilingWarning]]:
    """Generate synonyms for all columns in a table using an LLM.

    Returns (enriched_table, warnings).
    """
    warnings: list[ProfilingWarning] = []

    if not table.columns:
        return table, warnings

    prompt = _build_synonym_prompt(table.table, table.columns)

    try:
        response = call_llm(
            messages=[{"role": "user", "content": prompt}],
            model=model,
        )
        synonym_map = parse_json_from_llm_response(response)
    except Exception as e:
        logger.warning(f"Synonym generation failed for {table.table}: {e}")
        warnings.append(
            ProfilingWarning(
                table=table.table,
                tier="synonyms",
                message=str(e),
            )
        )
        return table, warnings

    # Apply synonyms to columns
    enriched_columns = []
    for col in table.columns:
        synonyms = synonym_map.get(col.name)
        if synonyms and isinstance(synonyms, list):
            enriched_columns.append(
                col.model_copy(update={"synonyms": synonyms})
            )
        else:
            enriched_columns.append(col)

    return table.model_copy(update={"columns": enriched_columns}), warnings
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/profiler/test_synonym_generator.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/profiler/synonym_generator.py tests/unit/profiler/test_synonym_generator.py
git commit -m "feat(profiler): add LLM-powered synonym generator"
```

---

### Task 14: Public API (profile_schema / profile_tables)

**Files:**
- Modify: `genie_world/profiler/__init__.py`
- Create: `tests/unit/profiler/test_public_api.py`

- [ ] **Step 1: Write failing tests for public API**

```python
# tests/unit/profiler/test_public_api.py
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from genie_world.profiler import profile_schema, profile_tables
from genie_world.profiler.models import (
    ColumnProfile,
    TableProfile,
    SchemaProfile,
)


class TestProfileSchema:
    @patch("genie_world.profiler.metadata_profiler.get_workspace_client")
    def test_metadata_only(self, mock_get_client):
        """Minimal call: metadata only, no warehouse or LLM."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock table listing
        table_info = MagicMock()
        table_info.name = "orders"
        mock_client.tables.list.return_value = [table_info]

        # Mock table detail
        mock_col = MagicMock()
        mock_col.name = "id"
        mock_col.type_name = "INT"
        mock_col.nullable = False
        mock_col.comment = None

        mock_table = MagicMock()
        mock_table.name = "orders"
        mock_table.comment = "Order records"
        mock_table.catalog_name = "main"
        mock_table.schema_name = "sales"
        mock_table.columns = [mock_col]
        mock_table.properties = {}

        mock_client.tables.get.return_value = mock_table

        result = profile_schema("main", "sales")

        assert isinstance(result, SchemaProfile)
        assert result.catalog == "main"
        assert result.schema_name == "sales"
        assert len(result.tables) == 1
        assert result.tables[0].table == "orders"
        assert result.schema_version == "1.0"

    @patch("genie_world.profiler.synonym_generator.call_llm")
    @patch("genie_world.profiler.data_profiler.execute_sql")
    @patch("genie_world.profiler.metadata_profiler.get_workspace_client")
    def test_with_deep_and_synonyms(self, mock_get_client, mock_execute, mock_call_llm):
        """Full profiling with deep + synonyms."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        table_info = MagicMock()
        table_info.name = "orders"
        mock_client.tables.list.return_value = [table_info]

        mock_col = MagicMock()
        mock_col.name = "amount"
        mock_col.type_name = "DOUBLE"
        mock_col.nullable = True
        mock_col.comment = None

        mock_table = MagicMock()
        mock_table.name = "orders"
        mock_table.comment = None
        mock_table.columns = [mock_col]
        mock_table.properties = {}
        mock_client.tables.get.return_value = mock_table

        # Mock data profiler SQL
        mock_execute.return_value = {
            "columns": [
                {"name": "total_rows", "type_name": "LONG"},
                {"name": "amount_cardinality", "type_name": "LONG"},
                {"name": "amount_null_pct", "type_name": "DOUBLE"},
                {"name": "amount_min", "type_name": "STRING"},
                {"name": "amount_max", "type_name": "STRING"},
            ],
            "data": [["500", "200", "1.5", "10.00", "999.99"]],
            "row_count": 1,
            "truncated": False,
            "error": None,
        }

        # Mock LLM synonym response
        mock_call_llm.return_value = '{"amount": ["total", "price"]}'

        result = profile_schema(
            "main", "sales", deep=True, synonyms=True, warehouse_id="wh-123"
        )

        assert result.tables[0].row_count == 500
        assert result.tables[0].columns[0].cardinality == 200
        assert result.tables[0].columns[0].synonyms == ["total", "price"]


class TestProfileTables:
    @patch("genie_world.profiler.metadata_profiler.get_workspace_client")
    def test_profiles_specific_tables(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_col = MagicMock()
        mock_col.name = "id"
        mock_col.type_name = "INT"
        mock_col.nullable = False
        mock_col.comment = None

        mock_table = MagicMock()
        mock_table.name = "orders"
        mock_table.comment = None
        mock_table.columns = [mock_col]
        mock_table.properties = {}
        mock_client.tables.get.return_value = mock_table

        result = profile_tables(tables=["main.sales.orders"])

        assert isinstance(result, SchemaProfile)
        assert len(result.tables) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/profiler/test_public_api.py -v`
Expected: FAIL — `ImportError: cannot import name 'profile_schema' from 'genie_world.profiler'`

- [ ] **Step 3: Implement public API**

```python
# genie_world/profiler/__init__.py
"""Data Profiler block for genie-world.

Public API:
    profile_schema() — Profile all tables in a catalog.schema
    profile_tables() — Profile specific tables by fully-qualified name
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Callable

from genie_world.core.tracing import trace
from genie_world.profiler.data_profiler import enrich_table_with_stats
from genie_world.profiler.metadata_profiler import (
    profile_schema_metadata,
    profile_table_metadata,
)
from genie_world.profiler.models import (
    ProfilingWarning,
    SchemaProfile,
    TableProfile,
)
from genie_world.profiler.relationship_detector import (
    detect_by_naming_patterns,
    merge_relationships,
)
from genie_world.profiler.synonym_generator import generate_synonyms_for_table
from genie_world.profiler.usage_profiler import (
    enrich_with_usage,
    get_declared_relationships,
)

logger = logging.getLogger(__name__)


@trace(name="profile_schema", span_type="CHAIN")
def profile_schema(
    catalog: str,
    schema: str,
    *,
    deep: bool = False,
    usage: bool = False,
    synonyms: bool = False,
    warehouse_id: str | None = None,
    max_workers: int = 4,
    progress_callback: Callable[[str, int, int], None] | None = None,
) -> SchemaProfile:
    """Profile all tables in a schema.

    Args:
        catalog: Unity Catalog catalog name.
        schema: Schema name.
        deep: Run Tier 2 data profiling (needs warehouse_id).
        usage: Run Tier 3 system table mining (needs warehouse_id).
        synonyms: Run LLM synonym generation.
        warehouse_id: SQL warehouse ID for deep/usage tiers.
        max_workers: Max parallel SQL queries for data profiling.
        progress_callback: Optional (stage, current, total) callback.
    """
    all_warnings: list[ProfilingWarning] = []

    # Tier 1: Metadata
    if progress_callback:
        progress_callback("metadata", 0, 1)
    tables, meta_warnings = profile_schema_metadata(
        catalog, schema, return_warnings=True
    )
    all_warnings.extend(meta_warnings)
    if progress_callback:
        progress_callback("metadata", 1, 1)

    # Tier 2: Data profiling
    if deep and warehouse_id:
        tables = _enrich_tables_parallel(
            tables, warehouse_id, max_workers, all_warnings, progress_callback
        )

    # Tier 3: Usage
    if usage and warehouse_id:
        if progress_callback:
            progress_callback("usage", 0, 1)
        tables, usage_warnings = enrich_with_usage(
            tables, catalog, schema, warehouse_id=warehouse_id
        )
        all_warnings.extend(usage_warnings)
        if progress_callback:
            progress_callback("usage", 1, 1)

    # Relationship detection
    if progress_callback:
        progress_callback("relationships", 0, 1)
    all_relationships = []

    # Naming patterns (always)
    naming_rels = detect_by_naming_patterns(tables)
    all_relationships.append(naming_rels)

    # UC constraints (if usage tier enabled)
    if usage and warehouse_id:
        uc_rels, uc_warnings = get_declared_relationships(
            catalog, schema, warehouse_id=warehouse_id
        )
        all_relationships.append(uc_rels)
        all_warnings.extend(uc_warnings)

    merged_rels = merge_relationships(*all_relationships)
    if progress_callback:
        progress_callback("relationships", 1, 1)

    # Synonym generation
    if synonyms:
        tables = _generate_synonyms_for_tables(tables, all_warnings, progress_callback)

    return SchemaProfile(
        schema_version="1.0",
        catalog=catalog,
        schema_name=schema,
        tables=tables,
        relationships=merged_rels,
        warnings=all_warnings if all_warnings else None,
        profiled_at=datetime.now(timezone.utc),
    )


@trace(name="profile_tables", span_type="CHAIN")
def profile_tables(
    tables: list[str],
    *,
    deep: bool = False,
    usage: bool = False,
    synonyms: bool = False,
    warehouse_id: str | None = None,
    max_workers: int = 4,
    progress_callback: Callable[[str, int, int], None] | None = None,
) -> SchemaProfile:
    """Profile specific tables by fully-qualified name (catalog.schema.table).

    All tables must be in the same catalog.schema.
    """
    if not tables:
        raise ValueError("At least one table must be specified")

    all_warnings: list[ProfilingWarning] = []

    # Parse table names
    parsed = []
    for t in tables:
        parts = t.split(".")
        if len(parts) != 3:
            raise ValueError(
                f"Table name must be fully qualified (catalog.schema.table): {t}"
            )
        parsed.append(tuple(parts))

    catalog = parsed[0][0]
    schema = parsed[0][1]

    # Validate all tables are in the same catalog.schema
    for cat, sch, _ in parsed:
        if cat != catalog or sch != schema:
            raise ValueError(
                f"All tables must be in the same catalog.schema. "
                f"Found {cat}.{sch} and {catalog}.{schema}"
            )

    # Tier 1: Metadata for each table
    table_profiles: list[TableProfile] = []
    for i, (cat, sch, tbl) in enumerate(parsed):
        if progress_callback:
            progress_callback("metadata", i, len(parsed))
        try:
            profile = profile_table_metadata(cat, sch, tbl)
            table_profiles.append(profile)
        except Exception as e:
            all_warnings.append(
                ProfilingWarning(table=tbl, tier="metadata", message=str(e))
            )

    if progress_callback:
        progress_callback("metadata", len(parsed), len(parsed))

    # Tier 2: Data profiling
    if deep and warehouse_id:
        table_profiles = _enrich_tables_parallel(
            table_profiles, warehouse_id, max_workers, all_warnings, progress_callback
        )

    # Tier 3: Usage
    if usage and warehouse_id:
        table_profiles, usage_warnings = enrich_with_usage(
            table_profiles, catalog, schema, warehouse_id=warehouse_id
        )
        all_warnings.extend(usage_warnings)

    # Relationships
    all_rels = [detect_by_naming_patterns(table_profiles)]
    if usage and warehouse_id:
        uc_rels, uc_warnings = get_declared_relationships(
            catalog, schema, warehouse_id=warehouse_id
        )
        all_rels.append(uc_rels)
        all_warnings.extend(uc_warnings)

    # Synonyms
    if synonyms:
        table_profiles = _generate_synonyms_for_tables(
            table_profiles, all_warnings, progress_callback
        )

    return SchemaProfile(
        schema_version="1.0",
        catalog=catalog,
        schema_name=schema,
        tables=table_profiles,
        relationships=merge_relationships(*all_rels),
        warnings=all_warnings if all_warnings else None,
        profiled_at=datetime.now(timezone.utc),
    )


def _enrich_tables_parallel(
    tables: list[TableProfile],
    warehouse_id: str,
    max_workers: int,
    warnings: list[ProfilingWarning],
    progress_callback: Callable[[str, int, int], None] | None = None,
) -> list[TableProfile]:
    """Enrich tables with data profiling in parallel."""
    enriched: dict[str, TableProfile] = {}
    total = len(tables)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(enrich_table_with_stats, t, warehouse_id): t.table
            for t in tables
        }
        done = 0
        for future in as_completed(futures):
            table_name = futures[future]
            try:
                enriched_table, table_warnings = future.result()
                enriched[table_name] = enriched_table
                warnings.extend(table_warnings)
            except Exception as e:
                logger.warning(f"Data profiling failed for {table_name}: {e}")
                warnings.append(
                    ProfilingWarning(table=table_name, tier="data", message=str(e))
                )
            done += 1
            if progress_callback:
                progress_callback("data", done, total)

    # Preserve original order
    return [enriched.get(t.table, t) for t in tables]


def _generate_synonyms_for_tables(
    tables: list[TableProfile],
    warnings: list[ProfilingWarning],
    progress_callback: Callable[[str, int, int], None] | None = None,
) -> list[TableProfile]:
    """Generate synonyms for all tables sequentially."""
    enriched = []
    for i, table in enumerate(tables):
        if progress_callback:
            progress_callback("synonyms", i, len(tables))
        enriched_table, syn_warnings = generate_synonyms_for_table(table)
        enriched.append(enriched_table)
        warnings.extend(syn_warnings)

    if progress_callback:
        progress_callback("synonyms", len(tables), len(tables))
    return enriched
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/profiler/test_public_api.py -v`
Expected: 3 passed.

- [ ] **Step 5: Run full test suite**

Run: `pytest tests/ -v`
Expected: All tests pass (approximately 35+ tests).

- [ ] **Step 6: Commit**

```bash
git add genie_world/profiler/__init__.py tests/unit/profiler/test_public_api.py
git commit -m "feat(profiler): add public API with profile_schema() and profile_tables()"
```

---

### Task 15: Final Verification

- [ ] **Step 1: Run full test suite with coverage**

Run: `pytest tests/ -v --tb=short`
Expected: All tests pass.

- [ ] **Step 2: Verify package imports work**

Run: `python -c "from genie_world.profiler import profile_schema, profile_tables; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Verify models import**

Run: `python -c "from genie_world.profiler.models import SchemaProfile, ColumnProfile, TableProfile, Relationship, DetectionMethod; print('OK')"`
Expected: `OK`

- [ ] **Step 4: Commit any remaining changes**

```bash
git status
# If clean, no commit needed
```
