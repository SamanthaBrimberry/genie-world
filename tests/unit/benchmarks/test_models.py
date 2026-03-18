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
