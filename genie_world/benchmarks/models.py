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
