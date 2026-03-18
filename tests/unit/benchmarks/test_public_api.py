"""Tests for the benchmarks public API (run_benchmarks, tune_space)."""

import pytest
from unittest.mock import patch, MagicMock, call
from datetime import datetime

from genie_world.benchmarks import run_benchmarks, tune_space, BenchmarkResult, TuneResult
from genie_world.benchmarks.models import (
    BenchmarkLabel,
    FailureType,
    QuestionSource,
    QuestionResult,
    Diagnosis,
    Suggestion,
    UpdateResult,
)
from genie_world.core.genie_client import GenieResponse


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_benchmark_result(
    space_id="space-1",
    correct=1,
    incorrect=0,
    no_sql=0,
    uncertain=0,
    expected_sql_errors=0,
    accuracy=1.0,
    space_config=None,
) -> BenchmarkResult:
    """Build a minimal BenchmarkResult for test assertions."""
    total = correct + incorrect + no_sql + uncertain + expected_sql_errors
    return BenchmarkResult(
        space_id=space_id,
        questions=[],
        accuracy=accuracy,
        total=total,
        correct=correct,
        incorrect=incorrect,
        no_sql=no_sql,
        uncertain=uncertain,
        expected_sql_errors=expected_sql_errors,
        warnings=[],
        space_config=space_config or {},
        run_at=datetime.utcnow(),
    )


# ---------------------------------------------------------------------------
# TestRunBenchmarks
# ---------------------------------------------------------------------------

class TestRunBenchmarks:
    @patch("genie_world.benchmarks.GenieClient")
    @patch("genie_world.benchmarks.runner.GenieClient")
    @patch("genie_world.benchmarks.evaluator.execute_sql")
    def test_runs_space_benchmarks(self, mock_exec, mock_runner_client_class, mock_client_class):
        # The __init__.py creates GenieClient for get_config
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # runner.GenieClient is used for ask()
        mock_runner_client = MagicMock()
        mock_runner_client_class.return_value = mock_runner_client

        # Mock get_config to return space with 1 benchmark question
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
        mock_runner_client.ask.return_value = GenieResponse(
            question="How many?",
            status="COMPLETED",
            generated_sql="SELECT COUNT(*) FROM t",
            result={"columns": [{"name": "count"}], "data": [["42"]], "row_count": 1},
            duration_seconds=2.0,
        )

        # Mock expected SQL execution
        mock_exec.return_value = {
            "error": None,
            "columns": [{"name": "count", "type_name": "LONG"}],
            "data": [["42"]],
            "row_count": 1,
            "truncated": False,
        }

        result = run_benchmarks("space-1", "wh-1")

        assert isinstance(result, BenchmarkResult)
        assert result.total == 1
        assert result.correct == 1
        assert result.accuracy == 1.0
        assert result.space_config is not None

    def test_raises_on_no_questions(self):
        with patch("genie_world.benchmarks.GenieClient") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.get_config.return_value = {"benchmarks": {"questions": []}}

            with pytest.raises(ValueError, match="No questions"):
                run_benchmarks("space-1", "wh-1")

    def test_raises_on_no_questions_with_no_custom(self):
        """No space questions and no custom_questions → ValueError."""
        with patch("genie_world.benchmarks.GenieClient") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.get_config.return_value = {}

            with pytest.raises(ValueError, match="No questions"):
                run_benchmarks("space-1", "wh-1", custom_questions=[])

    @patch("genie_world.benchmarks.GenieClient")
    @patch("genie_world.benchmarks.runner.GenieClient")
    @patch("genie_world.benchmarks.evaluator.execute_sql")
    def test_merges_custom_questions(self, mock_exec, mock_runner_client_class, mock_client_class):
        """Custom questions are merged with space config questions."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_runner_client = MagicMock()
        mock_runner_client_class.return_value = mock_runner_client

        mock_client.get_config.return_value = {
            "benchmarks": {
                "questions": [{
                    "question": ["Count rows"],
                    "answer": [{"format": "SQL", "content": ["SELECT COUNT(*) FROM t"]}],
                }]
            },
        }

        mock_runner_client.ask.return_value = GenieResponse(
            question="Count rows",
            status="COMPLETED",
            generated_sql="SELECT COUNT(*) FROM t",
            result={"columns": [{"name": "count"}], "data": [["1"]], "row_count": 1},
            duration_seconds=1.0,
        )
        mock_exec.return_value = {
            "error": None,
            "columns": [{"name": "count"}],
            "data": [["1"]],
            "row_count": 1,
            "truncated": False,
        }

        result = run_benchmarks(
            "space-1",
            "wh-1",
            custom_questions=[
                {"question": "How many rows?", "expected_sql": "SELECT COUNT(*) FROM t"},
            ],
        )

        # Should have 2 questions: 1 from config + 1 custom
        assert result.total == 2

    @patch("genie_world.benchmarks.GenieClient")
    @patch("genie_world.benchmarks.runner.GenieClient")
    @patch("genie_world.benchmarks.evaluator.execute_sql")
    def test_custom_questions_only_no_config_questions(
        self, mock_exec, mock_runner_client_class, mock_client_class
    ):
        """Custom questions alone (no config questions) should succeed."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_runner_client = MagicMock()
        mock_runner_client_class.return_value = mock_runner_client

        mock_client.get_config.return_value = {}  # no benchmarks section

        mock_runner_client.ask.return_value = GenieResponse(
            question="Revenue total?",
            status="COMPLETED",
            generated_sql="SELECT SUM(amount) FROM sales",
            result={"columns": [{"name": "sum"}], "data": [["100"]], "row_count": 1},
            duration_seconds=1.0,
        )
        mock_exec.return_value = {
            "error": None,
            "columns": [{"name": "sum"}],
            "data": [["100"]],
            "row_count": 1,
            "truncated": False,
        }

        result = run_benchmarks(
            "space-1",
            "wh-1",
            custom_questions=[
                {"question": "Revenue total?", "expected_sql": "SELECT SUM(amount) FROM sales"},
            ],
        )

        assert result.total == 1
        assert result.space_config is not None

    @patch("genie_world.benchmarks.GenieClient")
    @patch("genie_world.benchmarks.runner.GenieClient")
    def test_accuracy_computation_excludes_errors_and_uncertain(
        self, mock_runner_client_class, mock_client_class
    ):
        """Accuracy = correct / (correct + incorrect + no_sql), excluding expected_sql_errors and uncertain."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_runner_client = MagicMock()
        mock_runner_client_class.return_value = mock_runner_client

        # 1 question in config
        mock_client.get_config.return_value = {
            "benchmarks": {
                "questions": [{
                    "question": ["How many?"],
                    "answer": [{"format": "SQL", "content": ["SELECT COUNT(*) FROM t"]}],
                }]
            },
        }

        # Genie produces no SQL
        mock_runner_client.ask.return_value = GenieResponse(
            question="How many?",
            status="COMPLETED",
            generated_sql=None,  # no SQL → NO_SQL label
            duration_seconds=1.0,
        )

        result = run_benchmarks("space-1", "wh-1")

        assert result.no_sql == 1
        assert result.correct == 0
        # accuracy = correct / (correct + incorrect + no_sql) = 0 / 1 = 0.0
        assert result.accuracy == 0.0

    @patch("genie_world.benchmarks.GenieClient")
    @patch("genie_world.benchmarks.runner.GenieClient")
    @patch("genie_world.benchmarks.evaluator.execute_sql")
    def test_space_config_stored_in_result(
        self, mock_exec, mock_runner_client_class, mock_client_class
    ):
        """BenchmarkResult.space_config should contain the fetched config."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_runner_client = MagicMock()
        mock_runner_client_class.return_value = mock_runner_client

        space_config = {
            "version": 3,
            "benchmarks": {
                "questions": [{
                    "question": ["Test?"],
                    "answer": [{"format": "SQL", "content": ["SELECT 1"]}],
                }]
            },
        }
        mock_client.get_config.return_value = space_config

        mock_runner_client.ask.return_value = GenieResponse(
            question="Test?",
            status="COMPLETED",
            generated_sql="SELECT 1",
            result={"columns": [{"name": "x"}], "data": [["1"]], "row_count": 1},
            duration_seconds=0.5,
        )
        mock_exec.return_value = {
            "error": None,
            "columns": [{"name": "x"}],
            "data": [["1"]],
            "row_count": 1,
            "truncated": False,
        }

        result = run_benchmarks("space-1", "wh-1")
        assert result.space_config == space_config
        assert result.space_config["version"] == 3

    @patch("genie_world.benchmarks.GenieClient")
    @patch("genie_world.benchmarks.runner.GenieClient")
    @patch("genie_world.benchmarks.evaluator.execute_sql")
    def test_custom_question_source_is_custom(
        self, mock_exec, mock_runner_client_class, mock_client_class
    ):
        """Custom questions should have source=CUSTOM."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_config.return_value = {}  # no config questions

        mock_runner_client = MagicMock()
        mock_runner_client_class.return_value = mock_runner_client
        mock_runner_client.ask.return_value = GenieResponse(
            question="Custom Q?",
            status="COMPLETED",
            generated_sql="SELECT 1",
            result={"columns": [{"name": "x"}], "data": [["1"]], "row_count": 1},
            duration_seconds=1.0,
        )
        mock_exec.return_value = {
            "error": None,
            "columns": [{"name": "x"}],
            "data": [["1"]],
            "row_count": 1,
            "truncated": False,
        }

        result = run_benchmarks(
            "space-1",
            "wh-1",
            custom_questions=[{"question": "Custom Q?", "expected_sql": "SELECT 1"}],
        )

        assert len(result.questions) == 1
        assert result.questions[0].source == QuestionSource.CUSTOM


# ---------------------------------------------------------------------------
# TestTuneSpace – auto_approve=False
# ---------------------------------------------------------------------------

class TestTuneSpaceManual:
    @patch("genie_world.benchmarks.run_benchmarks")
    @patch("genie_world.benchmarks.diagnose_failures")
    @patch("genie_world.benchmarks.generate_suggestions")
    def test_returns_tune_result_with_pending_suggestions(
        self, mock_suggest, mock_diagnose, mock_run
    ):
        """auto_approve=False → runs one iteration, returns pending suggestions."""
        benchmark_result = _make_benchmark_result(correct=0, incorrect=1, accuracy=0.0)
        mock_run.return_value = benchmark_result
        mock_diagnose.return_value = [
            Diagnosis(
                question="Q1",
                failure_type=FailureType.WRONG_TABLE,
                detail="wrong table",
                affected_config_section="data_sources",
            )
        ]
        suggestion = Suggestion(
            section="text_instructions",
            action="add",
            content={"content": ["Use table foo"]},
            rationale="fix wrong table",
            addresses_questions=["Q1"],
        )
        mock_suggest.return_value = [suggestion]

        result = tune_space("space-1", "wh-1", auto_approve=False)

        assert isinstance(result, TuneResult)
        assert len(result.iterations) == 1
        # No suggestions were applied (no update_space called)
        assert result.suggestions_applied == []
        assert result.final_accuracy == 0.0
        assert result.target_reached is False

    @patch("genie_world.benchmarks.run_benchmarks")
    @patch("genie_world.benchmarks.diagnose_failures")
    @patch("genie_world.benchmarks.generate_suggestions")
    def test_no_update_space_called_when_not_auto_approve(
        self, mock_suggest, mock_diagnose, mock_run
    ):
        """auto_approve=False → update_space should NOT be called."""
        benchmark_result = _make_benchmark_result(correct=0, incorrect=1, accuracy=0.0)
        mock_run.return_value = benchmark_result
        mock_diagnose.return_value = []
        mock_suggest.return_value = []

        with patch("genie_world.benchmarks.update_space") as mock_update:
            tune_space("space-1", "wh-1", auto_approve=False)
            mock_update.assert_not_called()


# ---------------------------------------------------------------------------
# TestTuneSpace – auto_approve=True
# ---------------------------------------------------------------------------

class TestTuneSpaceAutoApprove:
    @patch("genie_world.benchmarks.run_benchmarks")
    @patch("genie_world.benchmarks.diagnose_failures")
    @patch("genie_world.benchmarks.generate_suggestions")
    @patch("genie_world.benchmarks.update_space")
    def test_stops_when_target_accuracy_reached(
        self, mock_update, mock_suggest, mock_diagnose, mock_run
    ):
        """Stops early when target accuracy is reached."""
        benchmark_result = _make_benchmark_result(correct=1, accuracy=1.0)
        mock_run.return_value = benchmark_result
        mock_diagnose.return_value = []
        mock_suggest.return_value = []

        result = tune_space("space-1", "wh-1", target_accuracy=0.9, auto_approve=True)

        assert result.target_reached is True
        assert result.final_accuracy == 1.0
        assert len(result.iterations) == 1
        mock_update.assert_not_called()  # accuracy already met, no update needed

    @patch("genie_world.benchmarks.run_benchmarks")
    @patch("genie_world.benchmarks.diagnose_failures")
    @patch("genie_world.benchmarks.generate_suggestions")
    @patch("genie_world.benchmarks.update_space")
    def test_applies_suggestions_and_reruns(
        self, mock_update, mock_suggest, mock_diagnose, mock_run
    ):
        """auto_approve=True → applies suggestions and re-runs benchmarks."""
        low_result = _make_benchmark_result(correct=0, incorrect=1, accuracy=0.0)
        high_result = _make_benchmark_result(correct=1, accuracy=1.0)

        # First call returns low accuracy, second returns high accuracy
        mock_run.side_effect = [low_result, high_result]
        mock_diagnose.return_value = [
            Diagnosis(
                question="Q1",
                failure_type=FailureType.MISSING_EXAMPLE,
                detail="missing example",
                affected_config_section="example_question_sqls",
            )
        ]
        suggestion = Suggestion(
            section="example_question_sqls",
            action="add",
            content={"question": ["Q1"], "sql": ["SELECT 1"]},
            rationale="add example",
            addresses_questions=["Q1"],
        )
        mock_suggest.return_value = [suggestion]
        mock_update.return_value = UpdateResult(
            space_id="space-1",
            changes_applied=1,
            updated_config={},
        )

        result = tune_space("space-1", "wh-1", target_accuracy=0.9, auto_approve=True)

        assert result.target_reached is True
        assert len(result.iterations) == 2
        assert len(result.suggestions_applied) == 1
        mock_update.assert_called_once()

    @patch("genie_world.benchmarks.run_benchmarks")
    @patch("genie_world.benchmarks.diagnose_failures")
    @patch("genie_world.benchmarks.generate_suggestions")
    @patch("genie_world.benchmarks.update_space")
    def test_stops_when_no_suggestions(
        self, mock_update, mock_suggest, mock_diagnose, mock_run
    ):
        """Stops when no suggestions are generated (nothing to improve)."""
        low_result = _make_benchmark_result(correct=0, incorrect=1, accuracy=0.0)
        mock_run.return_value = low_result
        mock_diagnose.return_value = []
        mock_suggest.return_value = []  # nothing to suggest

        result = tune_space("space-1", "wh-1", target_accuracy=0.9, auto_approve=True)

        assert result.target_reached is False
        assert len(result.iterations) == 1
        mock_update.assert_not_called()

    @patch("genie_world.benchmarks.run_benchmarks")
    @patch("genie_world.benchmarks.diagnose_failures")
    @patch("genie_world.benchmarks.generate_suggestions")
    @patch("genie_world.benchmarks.update_space")
    def test_respects_max_iterations(
        self, mock_update, mock_suggest, mock_diagnose, mock_run
    ):
        """Stops after max_iterations even if accuracy is not reached."""
        low_result = _make_benchmark_result(correct=0, incorrect=1, accuracy=0.0)
        mock_run.return_value = low_result
        mock_diagnose.return_value = [
            Diagnosis(
                question="Q1",
                failure_type=FailureType.WRONG_TABLE,
                detail="wrong table",
                affected_config_section="data_sources",
            )
        ]
        suggestion = Suggestion(
            section="text_instructions",
            action="add",
            content={"content": ["hint"]},
            rationale="fix",
            addresses_questions=["Q1"],
        )
        mock_suggest.return_value = [suggestion]
        mock_update.return_value = UpdateResult(
            space_id="space-1", changes_applied=1, updated_config={}
        )

        result = tune_space(
            "space-1", "wh-1", target_accuracy=0.9, max_iterations=2, auto_approve=True
        )

        assert result.target_reached is False
        assert len(result.iterations) == 2
        assert mock_run.call_count == 2


# ---------------------------------------------------------------------------
# TestExports – verify all public names are importable
# ---------------------------------------------------------------------------

class TestExports:
    def test_run_benchmarks_importable(self):
        from genie_world.benchmarks import run_benchmarks
        assert callable(run_benchmarks)

    def test_tune_space_importable(self):
        from genie_world.benchmarks import tune_space
        assert callable(tune_space)

    def test_diagnose_failures_importable(self):
        from genie_world.benchmarks import diagnose_failures
        assert callable(diagnose_failures)

    def test_generate_suggestions_importable(self):
        from genie_world.benchmarks import generate_suggestions
        assert callable(generate_suggestions)

    def test_update_space_importable(self):
        from genie_world.benchmarks import update_space
        assert callable(update_space)

    def test_models_importable(self):
        from genie_world.benchmarks import (
            BenchmarkResult,
            QuestionResult,
            Diagnosis,
            Suggestion,
            UpdateResult,
            TuneResult,
            BenchmarkLabel,
            FailureType,
            QuestionSource,
        )
        # All classes/enums should be importable
        assert BenchmarkResult is not None
        assert QuestionResult is not None
        assert Diagnosis is not None
        assert Suggestion is not None
        assert UpdateResult is not None
        assert TuneResult is not None
        assert BenchmarkLabel is not None
        assert FailureType is not None
        assert QuestionSource is not None
