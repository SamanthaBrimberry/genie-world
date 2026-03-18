"""Tests for the benchmarks diagnoser module."""
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

    @patch("genie_world.benchmarks.diagnoser.call_llm")
    def test_diagnoses_no_sql_question(self, mock_llm):
        mock_llm.return_value = json.dumps({
            "failure_type": "missing_example",
            "detail": "Genie did not generate SQL for this question",
            "affected_config_section": "example_question_sqls",
        })

        results = BenchmarkResult(
            space_id="s1", accuracy=0.0, total=1, correct=0,
            incorrect=0, no_sql=1, uncertain=0, expected_sql_errors=0,
            warnings=[], run_at=datetime(2026, 3, 18),
            space_config={},
            questions=[QuestionResult(
                question="What is churn?", expected_sql="SELECT COUNT(*) FROM churned",
                source=QuestionSource.CUSTOM, label=BenchmarkLabel.NO_SQL,
                genie_response=GenieResponse(
                    question="What is churn?", status="COMPLETED",
                    duration_seconds=1.0,
                ),
            )],
        )

        diagnoses = diagnose_failures(results)
        assert len(diagnoses) == 1
        assert diagnoses[0].failure_type == FailureType.MISSING_EXAMPLE

    @patch("genie_world.benchmarks.diagnoser.call_llm")
    def test_diagnoses_uncertain_question(self, mock_llm):
        mock_llm.return_value = json.dumps({
            "failure_type": "wrong_aggregation",
            "detail": "Genie used COUNT instead of SUM",
            "affected_config_section": "example_question_sqls",
        })

        results = BenchmarkResult(
            space_id="s1", accuracy=0.0, total=1, correct=0,
            incorrect=0, no_sql=0, uncertain=1, expected_sql_errors=0,
            warnings=[], run_at=datetime(2026, 3, 18),
            space_config={},
            questions=[QuestionResult(
                question="Revenue total", expected_sql="SELECT SUM(amount) FROM orders",
                source=QuestionSource.CUSTOM, label=BenchmarkLabel.UNCERTAIN,
                genie_response=GenieResponse(
                    question="Revenue total", status="COMPLETED",
                    generated_sql="SELECT COUNT(amount) FROM orders",
                    duration_seconds=1.5,
                ),
            )],
        )

        diagnoses = diagnose_failures(results)
        assert len(diagnoses) == 1
        assert diagnoses[0].failure_type == FailureType.WRONG_AGGREGATION

    @patch("genie_world.benchmarks.diagnoser.call_llm")
    def test_skips_expected_sql_error_questions(self, mock_llm):
        results = BenchmarkResult(
            space_id="s1", accuracy=0.0, total=1, correct=0,
            incorrect=0, no_sql=0, uncertain=0, expected_sql_errors=1,
            warnings=[], run_at=datetime(2026, 3, 18),
            space_config={},
            questions=[QuestionResult(
                question="test", expected_sql="SELECT BROKEN",
                source=QuestionSource.CUSTOM, label=BenchmarkLabel.EXPECTED_SQL_ERROR,
            )],
        )

        diagnoses = diagnose_failures(results)
        mock_llm.assert_not_called()
        assert diagnoses == []

    @patch("genie_world.benchmarks.diagnoser.call_llm")
    def test_performance_flag_included_in_failure_diagnosis(self, mock_llm):
        """A failing question with performance issue should have both failure_type and warning."""
        mock_llm.return_value = json.dumps({
            "failure_type": "wrong_filter",
            "detail": "Wrong filter applied",
            "affected_config_section": "sql_snippets",
        })

        results = BenchmarkResult(
            space_id="s1", accuracy=0.0, total=1, correct=0,
            incorrect=1, no_sql=0, uncertain=0, expected_sql_errors=0,
            warnings=[], run_at=datetime(2026, 3, 18),
            space_config={},
            questions=[QuestionResult(
                question="filtered q", expected_sql="SELECT * FROM t WHERE status='active'",
                source=QuestionSource.CUSTOM, label=BenchmarkLabel.INCORRECT,
                expected_metrics=ExecutionMetrics(execution_time_ms=50),
                genie_metrics=ExecutionMetrics(execution_time_ms=1000),
                genie_response=GenieResponse(
                    question="filtered q", status="COMPLETED",
                    generated_sql="SELECT * FROM t",
                    duration_seconds=1.0,
                ),
            )],
        )

        diagnoses = diagnose_failures(results)
        assert len(diagnoses) == 1
        assert diagnoses[0].failure_type == FailureType.WRONG_FILTER
        assert diagnoses[0].performance_warning is not None
        assert "slow" in diagnoses[0].performance_warning.lower()

    @patch("genie_world.benchmarks.diagnoser.call_llm")
    def test_no_performance_flag_when_not_slow(self, mock_llm):
        """No performance warning when execution time is acceptable."""
        mock_llm.return_value = json.dumps({
            "failure_type": "wrong_column",
            "detail": "Wrong column used",
            "affected_config_section": "text_instructions",
        })

        results = BenchmarkResult(
            space_id="s1", accuracy=0.0, total=1, correct=0,
            incorrect=1, no_sql=0, uncertain=0, expected_sql_errors=0,
            warnings=[], run_at=datetime(2026, 3, 18),
            space_config={},
            questions=[QuestionResult(
                question="col q", expected_sql="SELECT name FROM users",
                source=QuestionSource.CUSTOM, label=BenchmarkLabel.INCORRECT,
                expected_metrics=ExecutionMetrics(execution_time_ms=100),
                genie_metrics=ExecutionMetrics(execution_time_ms=500),  # 5x, not 10x
                genie_response=GenieResponse(
                    question="col q", status="COMPLETED",
                    generated_sql="SELECT username FROM users",
                    duration_seconds=0.5,
                ),
            )],
        )

        diagnoses = diagnose_failures(results)
        assert len(diagnoses) == 1
        assert diagnoses[0].performance_warning is None

    @patch("genie_world.benchmarks.diagnoser.call_llm")
    def test_llm_called_with_relevant_context(self, mock_llm):
        """LLM should be called with question, expected SQL, genie SQL, and config."""
        mock_llm.return_value = json.dumps({
            "failure_type": "wrong_table",
            "detail": "used wrong table",
            "affected_config_section": "example_question_sqls",
        })

        results = BenchmarkResult(
            space_id="s1", accuracy=0.0, total=1, correct=0,
            incorrect=1, no_sql=0, uncertain=0, expected_sql_errors=0,
            warnings=[], run_at=datetime(2026, 3, 18),
            space_config={"data_sources": {"tables": [{"table": "orders"}]}},
            questions=[QuestionResult(
                question="Total sales", expected_sql="SELECT SUM(amount) FROM orders",
                source=QuestionSource.CUSTOM, label=BenchmarkLabel.INCORRECT,
                genie_response=GenieResponse(
                    question="Total sales", status="COMPLETED",
                    generated_sql="SELECT SUM(price) FROM items",
                    duration_seconds=1.0,
                ),
            )],
        )

        diagnose_failures(results)
        mock_llm.assert_called_once()
        call_args = mock_llm.call_args
        messages = call_args[1].get("messages") or call_args[0][0]
        # The prompt should contain the question and SQLs
        prompt_text = str(messages)
        assert "Total sales" in prompt_text
        assert "orders" in prompt_text
