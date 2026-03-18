# Benchmarks Block Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Benchmarks block with GenieClient prerequisite — run questions against live Genie Spaces, evaluate accuracy, diagnose failures, generate improvement suggestions, and update spaces in place.

**Architecture:** Composable pipeline: runner → evaluator → diagnoser → suggester → updater. GenieClient wraps the Genie Conversation API with state-transition visibility. Hybrid evaluation (programmatic + LLM). Auto-tune loop wraps the pipeline with guardrails.

**Tech Stack:** Python 3.10+, pydantic 2.x, databricks-sdk, pytest. Existing core modules: auth, config, llm, sql, storage, tracing.

**Spec:** `docs/superpowers/specs/2026-03-18-benchmarks-design.md`

---

## File Structure

### Core Prerequisite
| File | Purpose |
|------|---------|
| `genie_world/core/genie_client.py` | Genie Conversation API wrapper (ask, get_config, update_config) |
| `tests/unit/core/test_genie_client.py` | GenieClient tests |

### Benchmarks (`genie_world/benchmarks/`)
| File | Purpose |
|------|---------|
| `genie_world/benchmarks/__init__.py` | Public API: run_benchmarks(), tune_space(), exports |
| `genie_world/benchmarks/models.py` | Enums, QuestionResult, BenchmarkResult, Diagnosis, Suggestion, etc. |
| `genie_world/benchmarks/runner.py` | Query Genie API per question (parallel) |
| `genie_world/benchmarks/evaluator.py` | Hybrid comparison + performance capture |
| `genie_world/benchmarks/diagnoser.py` | Classify failures via LLM |
| `genie_world/benchmarks/suggester.py` | Generate targeted config change suggestions |
| `genie_world/benchmarks/updater.py` | Fetch config, merge suggestions, PATCH API |

### Tests
| File | Purpose |
|------|---------|
| `tests/unit/benchmarks/__init__.py` | Benchmarks test package |
| `tests/unit/benchmarks/test_models.py` | Model/enum tests |
| `tests/unit/benchmarks/test_runner.py` | Runner tests |
| `tests/unit/benchmarks/test_evaluator.py` | Evaluator comparison logic tests |
| `tests/unit/benchmarks/test_diagnoser.py` | Diagnoser tests |
| `tests/unit/benchmarks/test_suggester.py` | Suggester tests |
| `tests/unit/benchmarks/test_updater.py` | Updater merge/PATCH tests |
| `tests/unit/benchmarks/test_public_api.py` | run_benchmarks/tune_space tests |

---

## Chunk 1: GenieClient + Benchmarks Models

### Task 1: GenieClient — Core Genie Conversation API

**Files:**
- Create: `genie_world/core/genie_client.py`
- Create: `tests/unit/core/test_genie_client.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/core/test_genie_client.py
import json
import pytest
from unittest.mock import MagicMock, patch, call
from genie_world.core.genie_client import GenieClient, GenieResponse


class TestGenieResponse:
    def test_minimal(self):
        r = GenieResponse(question="test", status="COMPLETED", duration_seconds=1.0)
        assert r.generated_sql is None
        assert r.states == []

    def test_full(self):
        r = GenieResponse(
            question="How many?", status="COMPLETED",
            generated_sql="SELECT COUNT(*) FROM t",
            result={"columns": [{"name": "count"}], "data": [["42"]], "row_count": 1},
            duration_seconds=3.5,
            states=["FETCHING_METADATA", "ASKING_AI", "EXECUTING_QUERY", "COMPLETED"],
            conversation_id="conv-123",
        )
        assert r.generated_sql == "SELECT COUNT(*) FROM t"
        assert len(r.states) == 4


class TestGenieClientAsk:
    @patch("genie_world.core.genie_client.get_workspace_client")
    def test_successful_question(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock start-conversation
        mock_client.api_client.do.side_effect = [
            # POST start-conversation
            {"conversation_id": "conv-1", "message_id": "msg-1"},
            # GET poll - ASKING_AI
            {"status": "ASKING_AI", "attachments": []},
            # GET poll - COMPLETED with SQL
            {
                "status": "COMPLETED",
                "attachments": [
                    {
                        "query": {"query": "SELECT COUNT(*) FROM t", "description": "Count rows"},
                        "attachment_id": "att-1",
                    }
                ],
            },
            # GET query-result
            {
                "statement_response": {
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {"schema": {"columns": [{"name": "count", "type_name": "LONG"}]}},
                    "result": {"data_array": [["42"]]},
                }
            },
        ]

        gc = GenieClient("space-123")
        resp = gc.ask("How many rows?")

        assert resp.status == "COMPLETED"
        assert resp.generated_sql == "SELECT COUNT(*) FROM t"
        assert resp.result is not None
        assert resp.result["row_count"] == 1

    @patch("genie_world.core.genie_client.get_workspace_client")
    def test_failed_question(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_client.api_client.do.side_effect = [
            {"conversation_id": "conv-1", "message_id": "msg-1"},
            {"status": "FAILED", "error": {"message": "Could not understand query"}},
        ]

        gc = GenieClient("space-123")
        resp = gc.ask("gibberish query")

        assert resp.status == "FAILED"
        assert resp.generated_sql is None
        assert "Could not understand" in resp.error

    @patch("genie_world.core.genie_client.get_workspace_client")
    def test_text_response_no_sql(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_client.api_client.do.side_effect = [
            {"conversation_id": "conv-1", "message_id": "msg-1"},
            {
                "status": "COMPLETED",
                "attachments": [
                    {"text": {"content": "I don't have enough information to answer that."}}
                ],
            },
        ]

        gc = GenieClient("space-123")
        resp = gc.ask("What is the meaning of life?")

        assert resp.status == "COMPLETED"
        assert resp.generated_sql is None


class TestGenieClientConfig:
    @patch("genie_world.core.genie_client.get_workspace_client")
    def test_get_config(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_client.api_client.do.return_value = {
            "space_id": "space-123",
            "serialized_space": '{"version": 2, "data_sources": {"tables": []}}',
        }

        gc = GenieClient("space-123")
        config = gc.get_config()

        assert config["version"] == 2

    @patch("genie_world.core.genie_client.get_workspace_client")
    def test_update_config(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.api_client.do.return_value = {"space_id": "space-123"}

        gc = GenieClient("space-123")
        result = gc.update_config({"version": 2, "_internal": "strip me"})

        # Verify _internal field was stripped
        call_args = mock_client.api_client.do.call_args
        body = call_args.kwargs.get("body", call_args[1].get("body", {}))
        serialized = json.loads(body["serialized_space"])
        assert "_internal" not in serialized
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source .venv/bin/activate && pytest tests/unit/core/test_genie_client.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement GenieClient**

```python
# genie_world/core/genie_client.py
"""Genie Conversation API wrapper with full state-transition visibility.

Ports the polling pattern from genie_tracing_demo.py and attachment
parsing from databricks-ai-bridge.
"""

from __future__ import annotations

import json
import logging
import time

from pydantic import BaseModel

from genie_world.core.auth import get_workspace_client
from genie_world.core.tracing import trace

logger = logging.getLogger(__name__)

_TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"}
_POLL_INTERVAL = 1.5  # seconds between polls


class GenieResponse(BaseModel):
    """Full response from a Genie conversation question."""
    question: str
    status: str = "UNKNOWN"
    generated_sql: str | None = None
    description: str | None = None
    result: dict | None = None
    duration_seconds: float = 0.0
    states: list[str] = []
    error: str | None = None
    conversation_id: str | None = None


class GenieClient:
    """Wraps the Genie Conversation API."""

    def __init__(self, space_id: str):
        self.space_id = space_id

    @trace(name="genie_ask", span_type="CHAIN")
    def ask(self, question: str, timeout: int = 300) -> GenieResponse:
        """Send a question, poll until complete, return full response."""
        client = get_workspace_client()
        base = f"/api/2.0/genie/spaces/{self.space_id}"
        start = time.time()
        states: list[str] = []

        # 1. Start conversation
        try:
            resp = client.api_client.do("POST", f"{base}/start-conversation", body={"content": question})
        except Exception as e:
            return GenieResponse(
                question=question, status="FAILED",
                error=str(e), duration_seconds=time.time() - start,
            )

        conv_id = resp.get("conversation_id") or resp.get("conversation", {}).get("id")
        msg_id = resp.get("message_id") or resp.get("message", {}).get("id")

        if not conv_id or not msg_id:
            return GenieResponse(
                question=question, status="FAILED",
                error=f"Missing conversation_id or message_id: {resp}",
                duration_seconds=time.time() - start,
            )

        # 2. Poll until terminal state
        last_status = None
        message = None

        while (time.time() - start) < timeout:
            try:
                message = client.api_client.do(
                    "GET", f"{base}/conversations/{conv_id}/messages/{msg_id}"
                )
            except Exception as e:
                error_str = str(e).lower()
                if "429" in error_str or "rate" in error_str:
                    return GenieResponse(
                        question=question, status="FAILED",
                        error="rate_limited", duration_seconds=time.time() - start,
                        states=states, conversation_id=conv_id,
                    )
                return GenieResponse(
                    question=question, status="FAILED",
                    error=str(e), duration_seconds=time.time() - start,
                    states=states, conversation_id=conv_id,
                )

            status = message.get("status", "UNKNOWN")

            if status != last_status:
                states.append(status)
                last_status = status

            if status in _TERMINAL_STATES:
                break

            time.sleep(_POLL_INTERVAL)
        else:
            return GenieResponse(
                question=question, status="TIMEOUT",
                error=f"Timed out after {timeout}s",
                duration_seconds=time.time() - start,
                states=states, conversation_id=conv_id,
            )

        # 3. Parse response
        duration = time.time() - start

        if status == "FAILED":
            error_msg = message.get("error", {})
            if isinstance(error_msg, dict):
                error_msg = error_msg.get("message", str(error_msg))
            return GenieResponse(
                question=question, status="FAILED",
                error=str(error_msg), duration_seconds=duration,
                states=states, conversation_id=conv_id,
            )

        if status != "COMPLETED":
            return GenieResponse(
                question=question, status=status,
                error=f"Terminal state: {status}",
                duration_seconds=duration, states=states, conversation_id=conv_id,
            )

        # 4. Extract SQL and result from attachments
        generated_sql = None
        description = None
        result = None
        attachment_id = None

        for att in message.get("attachments") or []:
            if "query" in att:
                query_obj = att["query"]
                generated_sql = query_obj.get("query")
                description = query_obj.get("description")
                attachment_id = att.get("attachment_id") or att.get("id")

        # 5. Fetch query result if we have an attachment_id
        if attachment_id:
            try:
                qr_resp = client.api_client.do(
                    "GET",
                    f"{base}/conversations/{conv_id}/messages/{msg_id}/attachments/{attachment_id}/query-result",
                )
                stmt = qr_resp.get("statement_response", {})
                columns = []
                if stmt.get("manifest", {}).get("schema", {}).get("columns"):
                    columns = [
                        {"name": c["name"], "type_name": c.get("type_name", "")}
                        for c in stmt["manifest"]["schema"]["columns"]
                    ]
                data = stmt.get("result", {}).get("data_array", [])
                result = {"columns": columns, "data": data, "row_count": len(data)}
            except Exception as e:
                logger.warning("Failed to fetch query result: %s", e)

        return GenieResponse(
            question=question, status="COMPLETED",
            generated_sql=generated_sql, description=description,
            result=result, duration_seconds=duration,
            states=states, conversation_id=conv_id,
        )

    @trace(name="genie_get_config", span_type="CHAIN")
    def get_config(self) -> dict:
        """Fetch the current space config."""
        client = get_workspace_client()
        resp = client.api_client.do(
            "GET",
            f"/api/2.0/genie/spaces/{self.space_id}",
            query={"include_serialized_space": "true"},
        )
        serialized = resp.get("serialized_space", "{}")
        return json.loads(serialized)

    @trace(name="genie_update_config", span_type="CHAIN")
    def update_config(self, config: dict) -> dict:
        """Update the space config via PATCH. Strips _-prefixed fields."""
        client = get_workspace_client()
        clean = {k: v for k, v in config.items() if not k.startswith("_")}
        return client.api_client.do(
            "PATCH",
            f"/api/2.0/genie/spaces/{self.space_id}",
            body={"serialized_space": json.dumps(clean)},
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source .venv/bin/activate && pytest tests/unit/core/test_genie_client.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/core/genie_client.py tests/unit/core/test_genie_client.py
git commit -m "feat(core): add GenieClient — Genie Conversation API wrapper"
```

---

### Task 2: Benchmarks Models

**Files:**
- Create: `genie_world/benchmarks/__init__.py`
- Create: `genie_world/benchmarks/models.py`
- Create: `tests/unit/benchmarks/__init__.py`
- Create: `tests/unit/benchmarks/test_models.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/benchmarks/test_models.py
from datetime import datetime
from genie_world.benchmarks.models import (
    BenchmarkLabel, FailureType, QuestionSource, ExecutionMetrics,
    QuestionInput, QuestionResult, BenchmarkResult, Diagnosis,
    Suggestion, UpdateResult, TuneResult,
)
from genie_world.core.genie_client import GenieResponse


class TestEnums:
    def test_benchmark_label_values(self):
        assert BenchmarkLabel.CORRECT == "correct"
        assert BenchmarkLabel.NO_SQL == "no_sql"

    def test_failure_type_values(self):
        assert FailureType.WRONG_TABLE == "wrong_table"
        assert FailureType.MISSING_JOIN == "missing_join"

    def test_question_source_values(self):
        assert QuestionSource.SPACE_CONFIG == "space_config"
        assert QuestionSource.CUSTOM == "custom"


class TestQuestionResult:
    def test_minimal(self):
        qr = QuestionResult(
            question="test", expected_sql="SELECT 1",
            source=QuestionSource.CUSTOM, label=BenchmarkLabel.CORRECT,
        )
        assert qr.failure_type is None
        assert qr.confidence == 1.0

    def test_with_genie_response(self):
        gr = GenieResponse(question="test", status="COMPLETED", generated_sql="SELECT 1", duration_seconds=2.0)
        qr = QuestionResult(
            question="test", expected_sql="SELECT 1",
            source=QuestionSource.SPACE_CONFIG, label=BenchmarkLabel.CORRECT,
            genie_response=gr,
        )
        assert qr.genie_response.generated_sql == "SELECT 1"


class TestBenchmarkResult:
    def test_accuracy_excludes_expected_sql_errors(self):
        result = BenchmarkResult(
            space_id="s1", questions=[], accuracy=0.8,
            total=10, correct=8, incorrect=1, no_sql=1,
            uncertain=0, expected_sql_errors=2,
            warnings=[], run_at=datetime(2026, 3, 18),
        )
        # accuracy = 8 / (8 + 1 + 1) = 0.8 — excludes the 2 expected_sql_errors
        assert result.accuracy == 0.8

    def test_serialization(self):
        result = BenchmarkResult(
            space_id="s1", questions=[], accuracy=1.0,
            total=5, correct=5, incorrect=0, no_sql=0,
            uncertain=0, expected_sql_errors=0,
            warnings=[], run_at=datetime(2026, 3, 18),
        )
        json_str = result.model_dump_json()
        loaded = BenchmarkResult.model_validate_json(json_str)
        assert loaded.space_id == "s1"


class TestSuggestion:
    def test_add_action(self):
        s = Suggestion(
            section="example_question_sqls", action="add",
            content={"question": "test", "sql": "SELECT 1"},
            rationale="Missing example", addresses_questions=["Q1"],
        )
        assert s.target_id is None

    def test_update_action(self):
        s = Suggestion(
            section="text_instructions", action="update",
            content={"content": ["Updated instruction"]},
            target_id="abc123", rationale="Fix conflict",
            addresses_questions=["Q2"],
        )
        assert s.target_id == "abc123"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source .venv/bin/activate && pytest tests/unit/benchmarks/test_models.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement models**

```python
# genie_world/benchmarks/models.py
"""Data models for the benchmarks block."""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel

from genie_world.core.genie_client import GenieResponse


class BenchmarkLabel(str, Enum):
    CORRECT = "correct"
    INCORRECT = "incorrect"
    NO_SQL = "no_sql"
    EXPECTED_SQL_ERROR = "expected_sql_error"
    UNCERTAIN = "uncertain"


class FailureType(str, Enum):
    WRONG_TABLE = "wrong_table"
    MISSING_JOIN = "missing_join"
    WRONG_AGGREGATION = "wrong_aggregation"
    WRONG_FILTER = "wrong_filter"
    MISSING_FILTER = "missing_filter"
    ENTITY_MISMATCH = "entity_mismatch"
    WRONG_COLUMN = "wrong_column"
    WRONG_DATE_HANDLING = "wrong_date_handling"
    MISSING_EXAMPLE = "missing_example"
    AMBIGUOUS_QUERY = "ambiguous_query"


class QuestionSource(str, Enum):
    SPACE_CONFIG = "space_config"
    CUSTOM = "custom"


class ExecutionMetrics(BaseModel):
    execution_time_ms: float | None = None
    row_count: int = 0


class QuestionInput(BaseModel):
    question: str
    expected_sql: str
    source: QuestionSource


class QuestionResult(BaseModel):
    question: str
    expected_sql: str
    source: QuestionSource
    label: BenchmarkLabel
    confidence: float = 1.0
    expected_result: dict | None = None
    genie_response: GenieResponse | None = None
    expected_metrics: ExecutionMetrics | None = None
    genie_metrics: ExecutionMetrics | None = None
    failure_type: FailureType | None = None
    comparison_detail: str | None = None
    error_detail: str | None = None


class BenchmarkResult(BaseModel):
    space_id: str
    questions: list[QuestionResult]
    accuracy: float
    total: int
    correct: int
    incorrect: int
    no_sql: int
    uncertain: int
    expected_sql_errors: int
    warnings: list[str]
    space_config: dict | None = None
    run_at: datetime


class Diagnosis(BaseModel):
    question: str
    failure_type: FailureType
    detail: str
    affected_config_section: str
    performance_warning: str | None = None


class Suggestion(BaseModel):
    section: str
    action: str
    content: dict | None = None
    target_id: str | None = None
    rationale: str
    addresses_questions: list[str]


class UpdateResult(BaseModel):
    space_id: str
    changes_applied: int
    updated_config: dict


class TuneResult(BaseModel):
    iterations: list[BenchmarkResult]
    suggestions_applied: list[Suggestion]
    final_accuracy: float
    target_reached: bool
```

Create init files:

`genie_world/benchmarks/__init__.py`:
```python
"""Benchmarks block for genie-world."""
```

`tests/unit/benchmarks/__init__.py`: empty file.

- [ ] **Step 4: Run tests to verify they pass**

Run: `source .venv/bin/activate && pytest tests/unit/benchmarks/test_models.py -v`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add genie_world/benchmarks/ tests/unit/benchmarks/
git commit -m "feat(benchmarks): add data models and enums"
```

---

## Chunk 2: Runner + Evaluator

### Task 3: Runner

**Files:**
- Create: `genie_world/benchmarks/runner.py`
- Create: `tests/unit/benchmarks/test_runner.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/benchmarks/test_runner.py
import json
import pytest
from unittest.mock import patch, MagicMock
from genie_world.benchmarks.runner import run_questions, extract_questions_from_config
from genie_world.benchmarks.models import QuestionInput, QuestionSource
from genie_world.core.genie_client import GenieResponse


class TestExtractQuestionsFromConfig:
    def test_extracts_from_benchmarks(self):
        config = {
            "benchmarks": {
                "questions": [
                    {
                        "question": ["What is total revenue?"],
                        "answer": [{"format": "SQL", "content": ["SELECT SUM(amount)", "FROM orders"]}],
                    }
                ]
            }
        }
        questions = extract_questions_from_config(config)
        assert len(questions) == 1
        assert questions[0].question == "What is total revenue?"
        assert questions[0].expected_sql == "SELECT SUM(amount) FROM orders"
        assert questions[0].source == QuestionSource.SPACE_CONFIG

    def test_empty_benchmarks(self):
        config = {"benchmarks": {"questions": []}}
        assert extract_questions_from_config(config) == []

    def test_no_benchmarks_section(self):
        config = {"data_sources": {"tables": []}}
        assert extract_questions_from_config(config) == []

    def test_skips_parameterized_without_defaults(self):
        config = {
            "benchmarks": {
                "questions": [
                    {
                        "question": ["Revenue for :region"],
                        "answer": [{"format": "SQL", "content": ["SELECT SUM(amount) FROM orders WHERE region = :region"]}],
                        "parameters": [{"name": "region", "type_hint": "STRING"}],
                        # No default_value → skipped
                    }
                ]
            }
        }
        questions = extract_questions_from_config(config)
        assert len(questions) == 0


class TestRunQuestions:
    @patch("genie_world.benchmarks.runner.GenieClient")
    def test_runs_questions_in_parallel(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.ask.return_value = GenieResponse(
            question="test", status="COMPLETED",
            generated_sql="SELECT 1", duration_seconds=1.0,
        )

        questions = [
            QuestionInput(question="Q1", expected_sql="SELECT 1", source=QuestionSource.CUSTOM),
            QuestionInput(question="Q2", expected_sql="SELECT 2", source=QuestionSource.CUSTOM),
        ]

        responses = run_questions("space-1", questions, max_workers=2)
        assert len(responses) == 2
        assert mock_client.ask.call_count == 2
```

- [ ] **Step 2: Run tests, verify fail, implement, verify pass, commit**

Implementation: `runner.py` with `extract_questions_from_config()` (parses nested answer format, handles parameterized questions) and `run_questions()` (ThreadPoolExecutor with GenieClient.ask per question).

```bash
git commit -m "feat(benchmarks): add runner with parallel question execution"
```

---

### Task 4: Evaluator

**Files:**
- Create: `genie_world/benchmarks/evaluator.py`
- Create: `tests/unit/benchmarks/test_evaluator.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/benchmarks/test_evaluator.py
import pytest
from unittest.mock import patch
from genie_world.benchmarks.evaluator import evaluate_question, _compare_results, _normalize_columns
from genie_world.benchmarks.models import BenchmarkLabel
from genie_world.core.genie_client import GenieResponse


class TestNormalizeColumns:
    def test_lowercases_and_strips(self):
        assert _normalize_columns(["Name", "`ID`", "  Status "]) == ["name", "id", "status"]


class TestCompareResults:
    def test_exact_match(self):
        expected = {"columns": [{"name": "id"}], "data": [["1"], ["2"]], "row_count": 2}
        genie = {"columns": [{"name": "id"}], "data": [["1"], ["2"]], "row_count": 2}
        label, detail = _compare_results(expected, genie, order_sensitive=False)
        assert label == BenchmarkLabel.CORRECT

    def test_different_columns(self):
        expected = {"columns": [{"name": "id"}], "data": [["1"]], "row_count": 1}
        genie = {"columns": [{"name": "name"}], "data": [["a"]], "row_count": 1}
        label, detail = _compare_results(expected, genie, order_sensitive=False)
        assert label == BenchmarkLabel.INCORRECT

    def test_row_count_differs_2x(self):
        expected = {"columns": [{"name": "id"}], "data": [["1"]], "row_count": 1}
        genie = {"columns": [{"name": "id"}], "data": [["1"], ["2"], ["3"]], "row_count": 3}
        label, detail = _compare_results(expected, genie, order_sensitive=False)
        assert label == BenchmarkLabel.INCORRECT

    def test_order_sensitive_mismatch(self):
        expected = {"columns": [{"name": "id"}], "data": [["1"], ["2"]], "row_count": 2}
        genie = {"columns": [{"name": "id"}], "data": [["2"], ["1"]], "row_count": 2}
        label_ordered, _ = _compare_results(expected, genie, order_sensitive=True)
        label_unordered, _ = _compare_results(expected, genie, order_sensitive=False)
        assert label_ordered == BenchmarkLabel.INCORRECT  # order matters
        assert label_unordered == BenchmarkLabel.CORRECT    # order doesn't matter

    def test_numeric_tolerance(self):
        expected = {"columns": [{"name": "val"}], "data": [["100.001"]], "row_count": 1}
        genie = {"columns": [{"name": "val"}], "data": [["100.002"]], "row_count": 1}
        label, _ = _compare_results(expected, genie, order_sensitive=False)
        assert label == BenchmarkLabel.CORRECT  # within 0.1% tolerance

    def test_null_matching(self):
        expected = {"columns": [{"name": "val"}], "data": [[None]], "row_count": 1}
        genie = {"columns": [{"name": "val"}], "data": [[None]], "row_count": 1}
        label, _ = _compare_results(expected, genie, order_sensitive=False)
        assert label == BenchmarkLabel.CORRECT

    def test_small_difference_is_uncertain(self):
        expected = {"columns": [{"name": "id"}], "data": [["1"], ["2"]], "row_count": 2}
        genie = {"columns": [{"name": "id"}], "data": [["1"], ["3"]], "row_count": 2}
        label, _ = _compare_results(expected, genie, order_sensitive=False)
        assert label == BenchmarkLabel.UNCERTAIN


class TestEvaluateQuestion:
    @patch("genie_world.benchmarks.evaluator.execute_sql")
    def test_no_sql_generated(self, mock_exec):
        resp = GenieResponse(question="Q", status="COMPLETED", duration_seconds=1.0)
        result = evaluate_question("Q", "SELECT 1", resp, "wh-1")
        assert result.label == BenchmarkLabel.NO_SQL

    @patch("genie_world.benchmarks.evaluator.execute_sql")
    def test_expected_sql_error(self, mock_exec):
        mock_exec.return_value = {"error": "Table not found", "columns": [], "data": [], "row_count": 0, "truncated": False}
        resp = GenieResponse(
            question="Q", status="COMPLETED",
            generated_sql="SELECT 1", duration_seconds=1.0,
            result={"columns": [{"name": "x"}], "data": [["1"]], "row_count": 1},
        )
        result = evaluate_question("Q", "SELECT bad", resp, "wh-1")
        assert result.label == BenchmarkLabel.EXPECTED_SQL_ERROR

    @patch("genie_world.benchmarks.evaluator.execute_sql")
    def test_correct_match(self, mock_exec):
        mock_exec.return_value = {
            "error": None, "columns": [{"name": "count", "type_name": "LONG"}],
            "data": [["42"]], "row_count": 1, "truncated": False,
        }
        resp = GenieResponse(
            question="Q", status="COMPLETED",
            generated_sql="SELECT COUNT(*) FROM t", duration_seconds=1.0,
            result={"columns": [{"name": "count"}], "data": [["42"]], "row_count": 1},
        )
        result = evaluate_question("Q", "SELECT COUNT(*) FROM t", resp, "wh-1")
        assert result.label == BenchmarkLabel.CORRECT
```

- [ ] **Step 2: Run tests, verify fail, implement, verify pass, commit**

Implementation: `evaluator.py` with `_normalize_columns()`, `_compare_results()` (programmatic comparison), `_detect_order_by()`, and `evaluate_question()` (orchestrates the flow including LLM fallback for UNCERTAIN).

```bash
git commit -m "feat(benchmarks): add hybrid evaluator with performance capture"
```

---

## Chunk 3: Diagnoser + Suggester + Updater

### Task 5: Diagnoser

**Files:**
- Create: `genie_world/benchmarks/diagnoser.py`
- Create: `tests/unit/benchmarks/test_diagnoser.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/benchmarks/test_diagnoser.py
import json
import pytest
from unittest.mock import patch
from datetime import datetime
from genie_world.benchmarks.diagnoser import diagnose_failures
from genie_world.benchmarks.models import (
    BenchmarkLabel, BenchmarkResult, FailureType, QuestionResult, QuestionSource,
    ExecutionMetrics,
)
from genie_world.core.genie_client import GenieResponse


class TestDiagnoseFailures:
    @patch("genie_world.benchmarks.diagnoser.call_llm")
    def test_diagnoses_incorrect_question(self, mock_llm):
        mock_llm.return_value = json.dumps({
            "failure_type": "wrong_table",
            "detail": "Genie queried products instead of orders",
            "affected_config_section": "example_question_sqls",
        })

        results = BenchmarkResult(
            space_id="s1", accuracy=0.0, total=1, correct=0,
            incorrect=1, no_sql=0, uncertain=0, expected_sql_errors=0,
            warnings=[], run_at=datetime(2026, 3, 18),
            space_config={"data_sources": {"tables": []}},
            questions=[QuestionResult(
                question="Total revenue", expected_sql="SELECT SUM(amount) FROM orders",
                source=QuestionSource.CUSTOM, label=BenchmarkLabel.INCORRECT,
                genie_response=GenieResponse(
                    question="Total revenue", status="COMPLETED",
                    generated_sql="SELECT SUM(price) FROM products",
                    duration_seconds=2.0,
                ),
            )],
        )

        diagnoses = diagnose_failures(results)
        assert len(diagnoses) == 1
        assert diagnoses[0].failure_type == FailureType.WRONG_TABLE
        assert diagnoses[0].affected_config_section == "example_question_sqls"

    def test_flags_performance_issue(self):
        results = BenchmarkResult(
            space_id="s1", accuracy=1.0, total=1, correct=1,
            incorrect=0, no_sql=0, uncertain=0, expected_sql_errors=0,
            warnings=[], run_at=datetime(2026, 3, 18),
            space_config={},
            questions=[QuestionResult(
                question="test", expected_sql="SELECT 1",
                source=QuestionSource.CUSTOM, label=BenchmarkLabel.CORRECT,
                expected_metrics=ExecutionMetrics(execution_time_ms=100),
                genie_metrics=ExecutionMetrics(execution_time_ms=5000),
            )],
        )

        diagnoses = diagnose_failures(results)
        # Performance warnings are added even for correct questions
        perf = [d for d in diagnoses if d.performance_warning]
        assert len(perf) == 1
        assert "slow" in perf[0].performance_warning.lower()

    @patch("genie_world.benchmarks.diagnoser.call_llm")
    def test_skips_correct_questions(self, mock_llm):
        results = BenchmarkResult(
            space_id="s1", accuracy=1.0, total=1, correct=1,
            incorrect=0, no_sql=0, uncertain=0, expected_sql_errors=0,
            warnings=[], run_at=datetime(2026, 3, 18),
            space_config={},
            questions=[QuestionResult(
                question="test", expected_sql="SELECT 1",
                source=QuestionSource.CUSTOM, label=BenchmarkLabel.CORRECT,
            )],
        )

        diagnoses = diagnose_failures(results)
        mock_llm.assert_not_called()
```

- [ ] **Step 2: Run tests, verify fail, implement, verify pass, commit**

```bash
git commit -m "feat(benchmarks): add failure diagnoser with performance flagging"
```

---

### Task 6: Suggester

**Files:**
- Create: `genie_world/benchmarks/suggester.py`
- Create: `tests/unit/benchmarks/test_suggester.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/benchmarks/test_suggester.py
import json
import pytest
from unittest.mock import patch
from datetime import datetime
from genie_world.benchmarks.suggester import generate_suggestions
from genie_world.benchmarks.models import (
    BenchmarkLabel, BenchmarkResult, Diagnosis, FailureType,
    QuestionResult, QuestionSource,
)


class TestGenerateSuggestions:
    @patch("genie_world.benchmarks.suggester.validate_and_fix_sql")
    @patch("genie_world.benchmarks.suggester.call_llm")
    def test_generates_add_example_suggestion(self, mock_llm, mock_validate):
        mock_llm.return_value = json.dumps({
            "question": "Total revenue by month",
            "sql": "SELECT DATE_TRUNC('month', order_date), SUM(amount) FROM orders GROUP BY 1",
        })
        mock_validate.return_value = (
            "SELECT DATE_TRUNC('month', order_date), SUM(amount) FROM orders GROUP BY 1", []
        )

        diagnoses = [Diagnosis(
            question="Monthly revenue", failure_type=FailureType.MISSING_EXAMPLE,
            detail="No example for time-based aggregation",
            affected_config_section="example_question_sqls",
        )]
        results = BenchmarkResult(
            space_id="s1", accuracy=0.5, total=2, correct=1,
            incorrect=1, no_sql=0, uncertain=0, expected_sql_errors=0,
            warnings=[], run_at=datetime(2026, 3, 18),
            space_config={"instructions": {"example_question_sqls": []}},
            questions=[],
        )

        suggestions = generate_suggestions(diagnoses, results, "wh-1")
        assert len(suggestions) >= 1
        assert suggestions[0].section == "example_question_sqls"
        assert suggestions[0].action == "add"

    @patch("genie_world.benchmarks.suggester.call_llm")
    def test_handles_llm_error(self, mock_llm):
        mock_llm.side_effect = RuntimeError("unavailable")

        diagnoses = [Diagnosis(
            question="test", failure_type=FailureType.WRONG_TABLE,
            detail="wrong", affected_config_section="text_instructions",
        )]
        results = BenchmarkResult(
            space_id="s1", accuracy=0.0, total=1, correct=0,
            incorrect=1, no_sql=0, uncertain=0, expected_sql_errors=0,
            warnings=[], run_at=datetime(2026, 3, 18),
            space_config={}, questions=[],
        )

        suggestions = generate_suggestions(diagnoses, results, "wh-1")
        assert suggestions == []  # graceful failure
```

- [ ] **Step 2: Run tests, verify fail, implement, verify pass, commit**

```bash
git commit -m "feat(benchmarks): add suggester with targeted config changes"
```

---

### Task 7: Updater

**Files:**
- Create: `genie_world/benchmarks/updater.py`
- Create: `tests/unit/benchmarks/test_updater.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/benchmarks/test_updater.py
import pytest
from unittest.mock import patch, MagicMock
from genie_world.benchmarks.updater import update_space, _merge_suggestions
from genie_world.benchmarks.models import Suggestion


class TestMergeSuggestions:
    def test_add_example(self):
        config = {"instructions": {"example_question_sqls": [
            {"id": "existing1", "question": ["Q1"], "sql": ["SELECT 1"]},
        ]}}
        suggestions = [Suggestion(
            section="example_question_sqls", action="add",
            content={"question": ["Q2"], "sql": ["SELECT 2"]},
            rationale="test", addresses_questions=["Q2"],
        )]

        merged = _merge_suggestions(config, suggestions)
        examples = merged["instructions"]["example_question_sqls"]
        assert len(examples) == 2
        assert examples[0]["id"] == "existing1"  # preserved
        assert "id" in examples[1]  # new ID generated

    def test_remove_by_target_id(self):
        config = {"instructions": {"example_question_sqls": [
            {"id": "keep-me", "question": ["Q1"]},
            {"id": "remove-me", "question": ["Q2"]},
        ]}}
        suggestions = [Suggestion(
            section="example_question_sqls", action="remove",
            target_id="remove-me",
            rationale="test", addresses_questions=[],
        )]

        merged = _merge_suggestions(config, suggestions)
        examples = merged["instructions"]["example_question_sqls"]
        assert len(examples) == 1
        assert examples[0]["id"] == "keep-me"

    def test_update_instruction_content(self):
        config = {"instructions": {"text_instructions": [
            {"id": "instr-1", "content": ["Old instruction"]},
        ]}}
        suggestions = [Suggestion(
            section="text_instructions", action="update",
            target_id="instr-1",
            content={"content": ["Updated instruction"]},
            rationale="test", addresses_questions=[],
        )]

        merged = _merge_suggestions(config, suggestions)
        instr = merged["instructions"]["text_instructions"]
        assert instr[0]["content"] == ["Updated instruction"]
        assert instr[0]["id"] == "instr-1"  # ID preserved


class TestUpdateSpace:
    @patch("genie_world.benchmarks.updater.GenieClient")
    def test_fetches_merges_patches(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_config.return_value = {
            "version": 2,
            "instructions": {"example_question_sqls": [], "text_instructions": [],
                             "sql_functions": [], "join_specs": [],
                             "sql_snippets": {"filters": [], "expressions": [], "measures": []}},
            "data_sources": {"tables": []},
            "config": {"sample_questions": []},
        }
        mock_client.update_config.return_value = {"space_id": "s1"}

        suggestions = [Suggestion(
            section="example_question_sqls", action="add",
            content={"question": ["test"], "sql": ["SELECT 1"]},
            rationale="test", addresses_questions=["Q1"],
        )]

        result = update_space("s1", suggestions, "wh-1")
        assert result.changes_applied == 1
        mock_client.update_config.assert_called_once()
```

- [ ] **Step 2: Run tests, verify fail, implement, verify pass, commit**

```bash
git commit -m "feat(benchmarks): add updater with config merge and PATCH"
```

---

## Chunk 4: Public API + Final Verification

### Task 8: Public API (run_benchmarks + tune_space)

**Files:**
- Modify: `genie_world/benchmarks/__init__.py`
- Create: `tests/unit/benchmarks/test_public_api.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/benchmarks/test_public_api.py
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from genie_world.benchmarks import run_benchmarks, BenchmarkResult
from genie_world.core.genie_client import GenieResponse


class TestRunBenchmarks:
    @patch("genie_world.benchmarks.runner.GenieClient")
    @patch("genie_world.benchmarks.evaluator.execute_sql")
    def test_runs_space_benchmarks(self, mock_exec, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock get_config to return space with 1 benchmark
        mock_client.get_config.return_value = {
            "benchmarks": {
                "questions": [{
                    "question": ["How many?"],
                    "answer": [{"format": "SQL", "content": ["SELECT COUNT(*) FROM t"]}],
                }]
            },
            "instructions": {"example_question_sqls": []},
        }

        # Mock Genie response
        mock_client.ask.return_value = GenieResponse(
            question="How many?", status="COMPLETED",
            generated_sql="SELECT COUNT(*) FROM t",
            result={"columns": [{"name": "count"}], "data": [["42"]], "row_count": 1},
            duration_seconds=2.0,
        )

        # Mock expected SQL execution
        mock_exec.return_value = {
            "error": None, "columns": [{"name": "count", "type_name": "LONG"}],
            "data": [["42"]], "row_count": 1, "truncated": False,
        }

        result = run_benchmarks("space-1", "wh-1")

        assert isinstance(result, BenchmarkResult)
        assert result.total == 1
        assert result.correct == 1
        assert result.accuracy == 1.0
        assert result.space_config is not None

    def test_raises_on_no_questions(self):
        with patch("genie_world.benchmarks.runner.GenieClient") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.get_config.return_value = {"benchmarks": {"questions": []}}

            with pytest.raises(ValueError, match="No questions"):
                run_benchmarks("space-1", "wh-1")
```

- [ ] **Step 2: Run tests, verify fail, implement, verify pass, commit**

Implementation: `__init__.py` with full `run_benchmarks()` orchestrator (fetch config → extract questions → merge custom → run → evaluate → return BenchmarkResult) and `tune_space()` wrapper.

Re-export all models, enums, and functions per the exports table in the spec.

```bash
git commit -m "feat(benchmarks): add run_benchmarks() and tune_space() public API"
```

---

### Task 9: Final Verification

- [ ] **Step 1: Run full test suite**

Run: `source .venv/bin/activate && pytest tests/ -v --tb=short`
Expected: All tests pass.

- [ ] **Step 2: Verify imports**

Run: `source .venv/bin/activate && python -c "from genie_world.benchmarks import run_benchmarks, diagnose_failures, generate_suggestions, update_space, tune_space, BenchmarkResult, Diagnosis, Suggestion, UpdateResult, TuneResult, BenchmarkLabel, FailureType, QuestionSource; print('All exports OK')"`
Expected: `All exports OK`

- [ ] **Step 3: Verify GenieClient import**

Run: `source .venv/bin/activate && python -c "from genie_world.core.genie_client import GenieClient, GenieResponse; print('GenieClient OK')"`
Expected: `GenieClient OK`
