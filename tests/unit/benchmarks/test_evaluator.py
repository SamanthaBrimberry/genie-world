"""Tests for the benchmarks evaluator module."""
import pytest
from unittest.mock import patch
from genie_world.benchmarks.evaluator import (
    evaluate_question,
    _compare_results,
    _normalize_columns,
    _detect_order_by,
    EvaluationResult,
)
from genie_world.benchmarks.models import BenchmarkLabel
from genie_world.core.genie_client import GenieResponse


class TestNormalizeColumns:
    def test_lowercases_and_strips(self):
        assert _normalize_columns(["Name", "`ID`", "  Status "]) == ["name", "id", "status"]

    def test_strips_backticks(self):
        assert _normalize_columns(["`order_id`"]) == ["order_id"]

    def test_strips_double_quotes(self):
        assert _normalize_columns(['"Revenue"']) == ["revenue"]

    def test_empty_list(self):
        assert _normalize_columns([]) == []

    def test_already_normalized(self):
        assert _normalize_columns(["id", "name", "status"]) == ["id", "name", "status"]


class TestDetectOrderBy:
    def test_simple_order_by(self):
        assert _detect_order_by("SELECT id FROM t ORDER BY id") is True

    def test_no_order_by(self):
        assert _detect_order_by("SELECT id FROM t") is False

    def test_order_by_case_insensitive(self):
        assert _detect_order_by("SELECT id FROM t order by id ASC") is True

    def test_order_by_in_subquery_only(self):
        # Top-level query has no ORDER BY
        assert _detect_order_by("SELECT x FROM (SELECT id FROM t ORDER BY id) s") is False

    def test_top_level_order_by_with_subquery(self):
        assert _detect_order_by("SELECT x FROM (SELECT id FROM t) s ORDER BY x") is True


class TestCompareResults:
    def test_exact_match(self):
        expected = {"columns": [{"name": "id"}], "data": [["1"], ["2"]], "row_count": 2}
        genie = {"columns": [{"name": "id"}], "data": [["1"], ["2"]], "row_count": 2}
        label, detail = _compare_results(expected, genie, order_sensitive=False)
        assert label == BenchmarkLabel.CORRECT

    def test_different_column_names_same_count_is_uncertain(self):
        """Same number of columns but different names — use positional match, route to LLM."""
        expected = {"columns": [{"name": "id"}], "data": [["1"]], "row_count": 1}
        genie = {"columns": [{"name": "name"}], "data": [["a"]], "row_count": 1}
        label, detail = _compare_results(expected, genie, order_sensitive=False)
        # Data differs ("1" vs "a") so even positional match fails → UNCERTAIN for LLM
        assert label in (BenchmarkLabel.UNCERTAIN, BenchmarkLabel.INCORRECT)

    def test_different_column_count_is_uncertain(self):
        """Different number of columns → UNCERTAIN for LLM judgment."""
        expected = {"columns": [{"name": "id"}, {"name": "val"}], "data": [["1", "2"]], "row_count": 1}
        genie = {"columns": [{"name": "id"}], "data": [["1"]], "row_count": 1}
        label, detail = _compare_results(expected, genie, order_sensitive=False)
        assert label == BenchmarkLabel.UNCERTAIN

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
        assert label_unordered == BenchmarkLabel.CORRECT  # order doesn't matter

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

    def test_empty_results_match(self):
        expected = {"columns": [{"name": "id"}], "data": [], "row_count": 0}
        genie = {"columns": [{"name": "id"}], "data": [], "row_count": 0}
        label, _ = _compare_results(expected, genie, order_sensitive=False)
        assert label == BenchmarkLabel.CORRECT

    def test_one_empty_one_not_is_incorrect(self):
        expected = {"columns": [{"name": "id"}], "data": [], "row_count": 0}
        genie = {"columns": [{"name": "id"}], "data": [["1"]], "row_count": 1}
        label, _ = _compare_results(expected, genie, order_sensitive=False)
        assert label == BenchmarkLabel.INCORRECT

    def test_numeric_tolerance_absolute(self):
        """Values too small for relative comparison should use absolute tolerance."""
        expected = {"columns": [{"name": "val"}], "data": [["0.001"]], "row_count": 1}
        genie = {"columns": [{"name": "val"}], "data": [["0.0015"]], "row_count": 1}
        label, _ = _compare_results(expected, genie, order_sensitive=False)
        assert label == BenchmarkLabel.CORRECT  # within 0.01 absolute tolerance

    def test_truncation_skips_row_count(self):
        """When either result is truncated, row count mismatch is still uncertain."""
        expected = {"columns": [{"name": "id"}], "data": [["1"]] * 5, "row_count": 5, "truncated": True}
        genie = {"columns": [{"name": "id"}], "data": [["1"]] * 3, "row_count": 3}
        label, detail = _compare_results(expected, genie, order_sensitive=False)
        # Truncated expected means we can't reliably compare row counts — uncertain
        assert label in (BenchmarkLabel.UNCERTAIN, BenchmarkLabel.CORRECT)

    def test_column_name_normalization(self):
        """Column names with backticks/quotes/different case should match."""
        expected = {"columns": [{"name": "Order_ID"}], "data": [["1"]], "row_count": 1}
        genie = {"columns": [{"name": "`order_id`"}], "data": [["1"]], "row_count": 1}
        label, _ = _compare_results(expected, genie, order_sensitive=False)
        assert label == BenchmarkLabel.CORRECT

    def test_large_row_count_difference_is_incorrect(self):
        """More than 2x row count difference is INCORRECT."""
        expected = {"columns": [{"name": "id"}], "data": [["1"]], "row_count": 1}
        genie = {
            "columns": [{"name": "id"}],
            "data": [["1"], ["2"], ["3"], ["4"], ["5"]],
            "row_count": 5,
        }
        label, _ = _compare_results(expected, genie, order_sensitive=False)
        assert label == BenchmarkLabel.INCORRECT


class TestEvaluateQuestion:
    @patch("genie_world.benchmarks.evaluator.execute_sql")
    def test_no_sql_generated(self, mock_exec):
        resp = GenieResponse(question="Q", status="COMPLETED", duration_seconds=1.0)
        result = evaluate_question("Q", "SELECT 1", resp, "wh-1")
        assert result.label == BenchmarkLabel.NO_SQL
        mock_exec.assert_not_called()

    @patch("genie_world.benchmarks.evaluator.execute_sql")
    def test_expected_sql_error(self, mock_exec):
        mock_exec.return_value = {
            "error": "Table not found",
            "columns": [],
            "data": [],
            "row_count": 0,
            "truncated": False,
        }
        resp = GenieResponse(
            question="Q",
            status="COMPLETED",
            generated_sql="SELECT 1",
            duration_seconds=1.0,
            result={"columns": [{"name": "x"}], "data": [["1"]], "row_count": 1},
        )
        result = evaluate_question("Q", "SELECT bad", resp, "wh-1")
        assert result.label == BenchmarkLabel.EXPECTED_SQL_ERROR

    @patch("genie_world.benchmarks.evaluator.execute_sql")
    def test_correct_match(self, mock_exec):
        mock_exec.return_value = {
            "error": None,
            "columns": [{"name": "count", "type_name": "LONG"}],
            "data": [["42"]],
            "row_count": 1,
            "truncated": False,
        }
        resp = GenieResponse(
            question="Q",
            status="COMPLETED",
            generated_sql="SELECT COUNT(*) FROM t",
            duration_seconds=1.0,
            result={"columns": [{"name": "count"}], "data": [["42"]], "row_count": 1},
        )
        result = evaluate_question("Q", "SELECT COUNT(*) FROM t", resp, "wh-1")
        assert result.label == BenchmarkLabel.CORRECT

    @patch("genie_world.benchmarks.evaluator.execute_sql")
    def test_result_from_genie_response(self, mock_exec):
        """When GenieResponse.result is available, it's used as the Genie result."""
        mock_exec.return_value = {
            "error": None,
            "columns": [{"name": "id"}],
            "data": [["1"]],
            "row_count": 1,
            "truncated": False,
        }
        resp = GenieResponse(
            question="Q",
            status="COMPLETED",
            generated_sql="SELECT id FROM t",
            duration_seconds=1.0,
            result={"columns": [{"name": "id"}], "data": [["1"]], "row_count": 1},
        )
        result = evaluate_question("Q", "SELECT id FROM t", resp, "wh-1")
        assert result.label == BenchmarkLabel.CORRECT
        assert result.expected_result is not None

    @patch("genie_world.benchmarks.evaluator.execute_sql")
    def test_incorrect_match(self, mock_exec):
        mock_exec.return_value = {
            "error": None,
            "columns": [{"name": "count"}],
            "data": [["10"]],
            "row_count": 1,
            "truncated": False,
        }
        resp = GenieResponse(
            question="Q",
            status="COMPLETED",
            generated_sql="SELECT COUNT(*) FROM t",
            duration_seconds=1.0,
            result={"columns": [{"name": "count"}], "data": [["999"]], "row_count": 1},
        )
        result = evaluate_question("Q", "SELECT COUNT(*) FROM t", resp, "wh-1")
        # 10 vs 999 is a large difference — should be INCORRECT or UNCERTAIN
        assert result.label in (BenchmarkLabel.INCORRECT, BenchmarkLabel.UNCERTAIN)

    @patch("genie_world.benchmarks.evaluator.execute_sql")
    def test_metrics_captured(self, mock_exec):
        mock_exec.return_value = {
            "error": None,
            "columns": [{"name": "id"}],
            "data": [["1"]],
            "row_count": 1,
            "truncated": False,
        }
        resp = GenieResponse(
            question="Q",
            status="COMPLETED",
            generated_sql="SELECT id FROM t",
            duration_seconds=2.5,
            result={"columns": [{"name": "id"}], "data": [["1"]], "row_count": 1},
        )
        result = evaluate_question("Q", "SELECT id FROM t", resp, "wh-1")
        assert result.expected_metrics is not None
        assert result.expected_metrics.row_count == 1
        assert result.genie_metrics is not None
        assert result.genie_metrics.row_count == 1

    @patch("genie_world.benchmarks.evaluator.execute_sql")
    def test_no_genie_result_executes_sql(self, mock_exec):
        """When genie_response.result is None, fall back to executing generated_sql."""
        mock_exec.side_effect = [
            # First call: expected SQL
            {
                "error": None,
                "columns": [{"name": "id"}],
                "data": [["1"]],
                "row_count": 1,
                "truncated": False,
            },
            # Second call: generated SQL fallback
            {
                "error": None,
                "columns": [{"name": "id"}],
                "data": [["1"]],
                "row_count": 1,
                "truncated": False,
            },
        ]
        resp = GenieResponse(
            question="Q",
            status="COMPLETED",
            generated_sql="SELECT id FROM t",
            duration_seconds=1.0,
            result=None,  # No pre-fetched result
        )
        result = evaluate_question("Q", "SELECT id FROM t", resp, "wh-1")
        assert result.label == BenchmarkLabel.CORRECT
        assert mock_exec.call_count == 2


class TestEvaluationResult:
    def test_evaluation_result_fields(self):
        from genie_world.benchmarks.evaluator import EvaluationResult
        from genie_world.benchmarks.models import ExecutionMetrics

        result = EvaluationResult(
            label=BenchmarkLabel.CORRECT,
            confidence=1.0,
            expected_result={"columns": [], "data": [], "row_count": 0},
            expected_metrics=ExecutionMetrics(row_count=0),
            genie_metrics=ExecutionMetrics(row_count=0),
            comparison_detail="Exact match",
        )
        assert result.label == BenchmarkLabel.CORRECT
        assert result.confidence == 1.0
