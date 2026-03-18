# Genie World: Benchmarks Block Design

## Overview

The Benchmarks block runs questions against a live Genie Space, evaluates accuracy with hybrid comparison (programmatic + LLM), diagnoses failures, generates targeted config improvement suggestions, and can update the space in place via PATCH API. Includes an auto-tuning loop for iterative improvement.

## Prerequisites

**`core/genie_client.py`** — Genie Conversation API wrapper (deferred from earlier blocks, required now).

## Design Principles

- **Composable steps** — run_benchmarks, diagnose, suggest, update are independently callable
- **Auto-tune wrapper** — `tune_space()` composes the steps into an iterative loop with guardrails
- **Hybrid evaluation** — programmatic comparison first (fast, deterministic), LLM fallback for ambiguous cases
- **Performance-aware** — captures execution metrics to flag correct-but-slow queries
- **Transparent suggestions** — user reviews and approves config changes before they're applied
- **In-place updates** — PATCH API modifies existing space, no delete/recreate

## Module Structure

```
genie_world/core/
├── genie_client.py          # NEW: Genie Conversation API wrapper

genie_world/benchmarks/
├── __init__.py              # Public API: run_benchmarks(), tune_space(), etc.
├── models.py                # BenchmarkLabel, FailureType, QuestionResult, etc.
├── runner.py                # Query Genie API per question (parallel)
├── evaluator.py             # Hybrid comparison + performance capture
├── diagnoser.py             # Classify failures + flag performance issues
├── suggester.py             # Generate targeted config change suggestions
└── updater.py               # Fetch config, merge suggestions, PATCH API
```

## Data Flow

```
Questions (from space config + custom)
         │
         ▼
    runner.py ──────→ Query Genie API per question (parallel via max_workers)
         │              Uses core/genie_client.py
         │              Returns: GenieResponse per question
         ▼
    evaluator.py ───→ Execute expected SQL + compare results
         │              Programmatic: column/row/value comparison
         │              NULL-aware, order-sensitive for ORDER BY queries
         │              Numeric tolerance (0.1% relative, 0.01 absolute)
         │              Captures ExecutionMetrics on both SQLs
         │              LLM fallback for UNCERTAIN cases
         │              Labels: CORRECT, INCORRECT, NO_SQL, EXPECTED_SQL_ERROR, UNCERTAIN
         ▼
    diagnoser.py ───→ Classify each failure by type via LLM
         │              Uses space_config + question context
         │              Also flags performance issues (10x slower, excess rows)
         │              Maps failures to affected config sections
         ▼
    suggester.py ───→ Generate config change suggestions
         │              Targeted additions/updates per failure type
         │              Validates suggested SQL against warehouse
         │              Uses builder/sql_validator.py for validation
         ▼
    updater.py ─────→ Fetch current config via GET
                        Merge approved suggestions
                        Re-enforce assembler constraints (sorting, IDs, strings)
                        Update space via PATCH /api/2.0/genie/spaces/{space_id}
```

## Prerequisite: Core Genie Client

```python
# genie_world/core/genie_client.py

class GenieClient:
    """Wraps the Genie Conversation API with full state-transition visibility."""

    def __init__(self, space_id: str):
        self.space_id = space_id

    def ask(self, question: str, timeout: int = 300) -> GenieResponse:
        """Send question via POST /start-conversation, poll until complete.

        Extracts generated SQL, result data, and state transitions.
        Uses the pattern from genie_tracing_demo.py:
        POST /start-conversation → poll GET /messages/{id} → extract attachments
        → fetch query-result for result data.
        """

    def get_config(self) -> dict:
        """GET /api/2.0/genie/spaces/{space_id}?include_serialized_space=true
        Returns parsed serialized_space dict.
        """

    def update_config(self, config: dict) -> dict:
        """PATCH /api/2.0/genie/spaces/{space_id}
        Strips _-prefixed internal fields before serializing.
        Returns updated space info.
        """


class GenieResponse(BaseModel):
    """Full response from a Genie question."""
    question: str
    status: str                          # COMPLETED, FAILED, CANCELLED
    generated_sql: str | None = None
    description: str | None = None       # Genie's reasoning
    result: dict | None = None           # {columns: [...], data: [...], row_count: int}
    duration_seconds: float = 0.0
    states: list[str] = []               # state transitions: [FETCHING_METADATA, ASKING_AI, ...]
    error: str | None = None
    conversation_id: str | None = None
```

Implementation ports the polling pattern from `genie_tracing_demo.py` (full payload visibility) and attachment parsing from `databricks-ai-bridge`'s `_parse_attachments()`. All API calls go through `core/auth.get_workspace_client()`.

## Data Models

```python
# benchmarks/models.py

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

class ExecutionMetrics(BaseModel):
    """Performance data from SQL execution."""
    execution_time_ms: float | None = None
    row_count: int = 0

class QuestionInput(BaseModel):
    """A question to benchmark."""
    question: str
    expected_sql: str
    source: str  # "space_config" | "custom"

class QuestionResult(BaseModel):
    """Result of running one question against Genie."""
    question: str
    expected_sql: str
    source: str                              # "space_config" | "custom"
    label: BenchmarkLabel
    confidence: float = 1.0
    expected_result: dict | None = None      # {columns, data, row_count}
    genie_response: GenieResponse | None = None
    expected_metrics: ExecutionMetrics | None = None
    genie_metrics: ExecutionMetrics | None = None
    failure_type: FailureType | None = None  # set by diagnoser
    comparison_detail: str | None = None     # "exact_match", "order_mismatch", etc.
    error_detail: str | None = None

class BenchmarkResult(BaseModel):
    """Full results from a benchmark run."""
    space_id: str
    questions: list[QuestionResult]
    accuracy: float                          # correct / (correct + incorrect + no_sql)
    total: int
    correct: int
    incorrect: int
    no_sql: int
    uncertain: int
    expected_sql_errors: int                 # excluded from accuracy calc
    warnings: list[str]
    space_config: dict | None = None         # fetched during run, reused downstream
    run_at: datetime

class Diagnosis(BaseModel):
    """Classification of a failure."""
    question: str
    failure_type: FailureType
    detail: str
    affected_config_section: str             # example_question_sqls, text_instructions, etc.
    performance_warning: str | None = None   # "Genie SQL 10x slower", etc.

class Suggestion(BaseModel):
    """A specific config change recommendation."""
    section: str                             # example_question_sqls, text_instructions, etc.
    action: str                              # "add" | "update" | "remove"
    content: dict | None = None              # for add/update — actual config entry
    target_id: str | None = None             # for update/remove — existing entry ID
    rationale: str
    addresses_questions: list[str]

class UpdateResult(BaseModel):
    """Result from updating a space."""
    space_id: str
    changes_applied: int
    updated_config: dict

class TuneResult(BaseModel):
    """Result from iterative tuning."""
    iterations: list[BenchmarkResult]
    suggestions_applied: list[Suggestion]
    final_accuracy: float
    target_reached: bool
```

## Module Details

### `runner.py`

```python
def run_questions(
    space_id: str,
    questions: list[QuestionInput],
    max_workers: int = 4,
) -> list[GenieResponse]:
    """Query Genie API for each question in parallel.

    Uses ThreadPoolExecutor with max_workers concurrent conversations.
    Each question starts a fresh conversation (no context carryover).
    """
```

Uses `GenieClient.ask()` via ThreadPoolExecutor. Returns `GenieResponse` per question.

### `evaluator.py`

```python
class EvaluationResult(BaseModel):
    label: BenchmarkLabel
    confidence: float = 1.0
    expected_result: dict | None = None
    genie_result: dict | None = None
    expected_metrics: ExecutionMetrics | None = None
    genie_metrics: ExecutionMetrics | None = None
    comparison_detail: str | None = None

def evaluate_question(
    question: str,
    expected_sql: str,
    genie_response: GenieResponse,
    warehouse_id: str,
) -> EvaluationResult:
    """Compare expected vs Genie results with performance capture."""
```

Evaluation flow:

1. **No SQL check** — If `genie_response.generated_sql` is None → `NO_SQL`
2. **Execute expected SQL** — via `core/sql.py`, capture execution time → if fails, `EXPECTED_SQL_ERROR`
3. **Get Genie result** — from `genie_response.result` (already executed by Genie) or execute if missing
4. **Programmatic comparison:**
   - Normalize column names (lowercase, strip backticks/quotes)
   - NULL-aware value comparison (NULL == NULL for comparison purposes)
   - Detect ORDER BY in expected SQL → order-sensitive comparison if present, sort-then-compare if not
   - Numeric tolerance: 0.1% relative, 0.01 absolute for floating point
   - Same columns + same data → `CORRECT` (1.0)
   - Different columns → `INCORRECT` (1.0)
   - Same columns, row count differs >2x → `INCORRECT` (1.0)
   - Same columns, small differences → `UNCERTAIN`
5. **LLM fallback** (UNCERTAIN only) — send both SQLs + result samples + question → `CORRECT` or `INCORRECT` with confidence

### `diagnoser.py`

```python
def diagnose_failures(
    results: BenchmarkResult,
) -> list[Diagnosis]:
    """Classify each non-correct question by failure type.

    Processes INCORRECT, NO_SQL, and UNCERTAIN questions.
    Uses results.space_config for table/column context.
    Also flags performance issues by comparing execution metrics.
    """
```

For each failing question, sends to LLM:
- The question text
- Expected SQL vs Genie SQL (or "no SQL generated")
- Expected result sample vs Genie result sample
- Available tables and columns from `space_config`

LLM returns:
- `failure_type` from the `FailureType` enum
- `detail` explaining what went wrong
- `affected_config_section` where the fix should go

Performance diagnosis (no LLM needed):
- If `genie_metrics.execution_time_ms > 10 * expected_metrics.execution_time_ms` → flag as slow
- If Genie result has >5x more rows than expected → likely missing filter

### `suggester.py`

```python
def generate_suggestions(
    diagnoses: list[Diagnosis],
    results: BenchmarkResult,
    warehouse_id: str,
) -> list[Suggestion]:
    """Generate config change suggestions from diagnosed failures.

    Uses results.space_config for current state awareness.
    Validates suggested SQL against warehouse via sql_validator.
    """
```

Suggestion generation by failure type:

| Failure Type | Config Section | Action |
|---|---|---|
| `MISSING_EXAMPLE` | `example_question_sqls` | Add new example Q&A for the failing pattern |
| `WRONG_TABLE` / `WRONG_COLUMN` | `text_instructions` | Update instruction to clarify table/column usage |
| `MISSING_JOIN` | `text_instructions` | Add join guidance to instructions |
| `WRONG_AGGREGATION` | `example_question_sqls` | Add example demonstrating correct aggregation |
| `WRONG_FILTER` / `MISSING_FILTER` | `sql_snippets.filters` | Add filter snippet for the missing condition |
| `ENTITY_MISMATCH` | `data_sources.column_configs` | Add/update synonyms or entity matching |
| `WRONG_DATE_HANDLING` | `text_instructions` + `sql_snippets.expressions` | Add date handling guidance + expression |
| `AMBIGUOUS_QUERY` | `text_instructions` | Add clarification trigger |

Each suggestion:
- Generates actual config content via LLM (not just a description)
- Validates SQL in generated content against warehouse (3x retry via `builder/sql_validator.py`)
- Checks for duplicates against current config
- Includes `rationale` and `addresses_questions`

### `updater.py`

```python
def update_space(
    space_id: str,
    suggestions: list[Suggestion],
    warehouse_id: str,
) -> UpdateResult:
    """Apply approved suggestions to a live Genie Space."""
```

Flow:
1. Fetch current config via `GenieClient.get_config()`
2. For each suggestion:
   - `"add"` → append to the appropriate section
   - `"update"` → find entry by `target_id`, replace
   - `"remove"` → find entry by `target_id`, delete
3. Re-derive `sample_questions` if examples changed
4. Re-enforce assembler constraints (sorting, string arrays, ID generation for new entries)
5. Strip `_`-prefixed internal fields
6. PATCH via `GenieClient.update_config()`
7. Return `UpdateResult` with changes count and updated config

Reuses `builder/assembler.py` for constraint enforcement.

## Public API

```python
def run_benchmarks(
    space_id: str,
    warehouse_id: str,
    *,
    custom_questions: list[dict] | None = None,
    max_workers: int = 4,
) -> BenchmarkResult:
    """Run benchmark questions against a live Genie Space.

    Pulls benchmark questions from the space config AND
    accepts custom questions [{"question": "...", "expected_sql": "..."}].
    Evaluates each with hybrid comparison.
    Stores fetched space_config in result for downstream reuse.
    Raises ValueError if no questions to benchmark.
    """

def diagnose_failures(
    results: BenchmarkResult,
) -> list[Diagnosis]:
    """Classify each failing question by failure type.

    Processes INCORRECT, NO_SQL, and UNCERTAIN questions.
    Uses results.space_config for table/column context.
    Flags performance issues via execution metric comparison.
    """

def generate_suggestions(
    diagnoses: list[Diagnosis],
    results: BenchmarkResult,
    warehouse_id: str,
) -> list[Suggestion]:
    """Generate config change suggestions from diagnosed failures.

    Uses results.space_config for current state awareness.
    Validates suggested SQL against warehouse.
    """

def update_space(
    space_id: str,
    suggestions: list[Suggestion],
    warehouse_id: str,
) -> UpdateResult:
    """Apply approved suggestions to a live Genie Space.

    Fetches current config, merges suggestions, re-enforces
    assembler constraints, PATCHes the space.
    """

def tune_space(
    space_id: str,
    warehouse_id: str,
    *,
    custom_questions: list[dict] | None = None,
    target_accuracy: float = 0.9,
    max_iterations: int = 3,
    max_workers: int = 4,
    auto_approve: bool = False,
) -> TuneResult:
    """Iterative benchmark → diagnose → suggest → update loop.

    When auto_approve=False: runs one iteration (benchmark + diagnose +
    suggest) and returns TuneResult with pending suggestions. User reviews,
    calls update_space() manually, then re-runs tune_space().

    When auto_approve=True: full loop up to max_iterations. Each iteration
    applies suggestions, then re-benchmarks. Re-runs failing questions +
    random sample of passing questions (regression detection). Stops when
    target_accuracy reached or max_iterations exhausted.
    """
```

## Usage Examples

### Step-by-step (full control)

```python
from genie_world.benchmarks import (
    run_benchmarks, diagnose_failures, generate_suggestions, update_space,
)

# Run benchmarks
results = run_benchmarks("space-id", "wh-id", custom_questions=[
    {"question": "Total spend by artist", "expected_sql": "SELECT ..."},
])
print(f"Accuracy: {results.accuracy:.0%} ({results.correct}/{results.total})")

for q in results.questions:
    if q.label != "correct":
        print(f"  {q.label}: {q.question}")

# Diagnose failures
diagnoses = diagnose_failures(results)
for d in diagnoses:
    print(f"  [{d.failure_type.value}] {d.detail}")
    if d.performance_warning:
        print(f"    PERF: {d.performance_warning}")

# Generate suggestions
suggestions = generate_suggestions(diagnoses, results, "wh-id")
for s in suggestions:
    print(f"  [{s.section}] {s.action}: {s.rationale}")

# Review and apply
approved = [s for s in suggestions if ...]  # user filters
result = update_space("space-id", approved, "wh-id")
print(f"Applied {result.changes_applied} changes")
```

### Auto-tune

```python
from genie_world.benchmarks import tune_space

result = tune_space(
    "space-id", "wh-id",
    target_accuracy=0.9,
    max_iterations=3,
    auto_approve=True,
)
print(f"Final: {result.final_accuracy:.0%} in {len(result.iterations)} iterations")
print(f"Target reached: {result.target_reached}")
```

## Error Handling

- **Genie API timeout** — individual questions that timeout get `NO_SQL` label with error_detail
- **Expected SQL fails** — labeled `EXPECTED_SQL_ERROR`, excluded from accuracy calculation
- **LLM errors in evaluator** — question stays `UNCERTAIN`, counted separately
- **LLM errors in diagnoser/suggester** — returns partial results with warnings
- **PATCH API failure** — `update_space()` raises with the API error, suggestions are not lost
- **Regression in tune_space()** — if a passing question fails after update, flagged in warnings and loop stops if net accuracy decreased

## Testing Strategy

- **Unit tests**: Mock GenieClient, LLM, and SQL execution. Verify evaluator comparison logic, diagnoser classification, suggester output format, updater merge logic.
- **Integration tests**: Run benchmarks against a live space, verify end-to-end flow. Gated behind `RUN_INTEGRATION_TESTS=1`.

## Baseline References

- **dbx-genie-rx `error_analysis.py`** — hybrid auto-labeling pattern (programmatic + LLM fallback)
- **databricks-ai-bridge `genie.py`** — Genie API polling, attachment parsing, state tracking
- **genie_tracing_demo.py** — REST API approach for full state visibility
- **Databricks API** — `PATCH /api/2.0/genie/spaces/{space_id}` for in-place updates
