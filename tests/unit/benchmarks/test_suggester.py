"""Tests for the benchmarks suggester module."""
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

    @patch("genie_world.benchmarks.suggester.validate_and_fix_sql")
    @patch("genie_world.benchmarks.suggester.call_llm")
    def test_generates_add_example_for_wrong_aggregation(self, mock_llm, mock_validate):
        mock_llm.return_value = json.dumps({
            "question": "Total sales amount",
            "sql": "SELECT SUM(sales_amount) FROM transactions",
        })
        mock_validate.return_value = ("SELECT SUM(sales_amount) FROM transactions", [])

        diagnoses = [Diagnosis(
            question="Total sales amount", failure_type=FailureType.WRONG_AGGREGATION,
            detail="Used COUNT instead of SUM",
            affected_config_section="example_question_sqls",
        )]
        results = BenchmarkResult(
            space_id="s1", accuracy=0.0, total=1, correct=0,
            incorrect=1, no_sql=0, uncertain=0, expected_sql_errors=0,
            warnings=[], run_at=datetime(2026, 3, 18),
            space_config={},
            questions=[],
        )

        suggestions = generate_suggestions(diagnoses, results, "wh-1")
        assert len(suggestions) >= 1
        add_sugg = [s for s in suggestions if s.action == "add"]
        assert len(add_sugg) >= 1
        assert add_sugg[0].section == "example_question_sqls"

    @patch("genie_world.benchmarks.suggester.call_llm")
    def test_generates_text_instruction_for_wrong_table(self, mock_llm):
        mock_llm.return_value = json.dumps({
            "instruction": "When asked about revenue, always use the orders table, not products.",
        })

        diagnoses = [Diagnosis(
            question="Total revenue", failure_type=FailureType.WRONG_TABLE,
            detail="Used products table instead of orders",
            affected_config_section="text_instructions",
        )]
        results = BenchmarkResult(
            space_id="s1", accuracy=0.0, total=1, correct=0,
            incorrect=1, no_sql=0, uncertain=0, expected_sql_errors=0,
            warnings=[], run_at=datetime(2026, 3, 18),
            space_config={},
            questions=[],
        )

        suggestions = generate_suggestions(diagnoses, results, "wh-1")
        assert len(suggestions) >= 1
        assert suggestions[0].section == "text_instructions"
        assert suggestions[0].action == "add"

    @patch("genie_world.benchmarks.suggester.call_llm")
    def test_generates_text_instruction_for_wrong_column(self, mock_llm):
        mock_llm.return_value = json.dumps({
            "instruction": "Use 'full_name' column instead of 'username' for user names.",
        })

        diagnoses = [Diagnosis(
            question="List user names", failure_type=FailureType.WRONG_COLUMN,
            detail="Used username instead of full_name",
            affected_config_section="text_instructions",
        )]
        results = BenchmarkResult(
            space_id="s1", accuracy=0.0, total=1, correct=0,
            incorrect=1, no_sql=0, uncertain=0, expected_sql_errors=0,
            warnings=[], run_at=datetime(2026, 3, 18),
            space_config={},
            questions=[],
        )

        suggestions = generate_suggestions(diagnoses, results, "wh-1")
        assert len(suggestions) >= 1
        assert suggestions[0].section == "text_instructions"

    @patch("genie_world.benchmarks.suggester.call_llm")
    def test_generates_filter_snippet_for_missing_filter(self, mock_llm):
        mock_llm.return_value = json.dumps({
            "filter": "WHERE deleted_at IS NULL",
            "description": "Always filter out soft-deleted records",
        })

        diagnoses = [Diagnosis(
            question="Active users", failure_type=FailureType.MISSING_FILTER,
            detail="Did not filter out deleted users",
            affected_config_section="sql_snippets",
        )]
        results = BenchmarkResult(
            space_id="s1", accuracy=0.0, total=1, correct=0,
            incorrect=1, no_sql=0, uncertain=0, expected_sql_errors=0,
            warnings=[], run_at=datetime(2026, 3, 18),
            space_config={},
            questions=[],
        )

        suggestions = generate_suggestions(diagnoses, results, "wh-1")
        assert len(suggestions) >= 1
        assert suggestions[0].section == "sql_snippets"
        assert suggestions[0].action == "add"

    @patch("genie_world.benchmarks.suggester.call_llm")
    def test_generates_filter_snippet_for_wrong_filter(self, mock_llm):
        mock_llm.return_value = json.dumps({
            "filter": "WHERE status = 'active'",
            "description": "Use status='active' for active user filter",
        })

        diagnoses = [Diagnosis(
            question="Active users count", failure_type=FailureType.WRONG_FILTER,
            detail="Used is_active=1 instead of status='active'",
            affected_config_section="sql_snippets",
        )]
        results = BenchmarkResult(
            space_id="s1", accuracy=0.0, total=1, correct=0,
            incorrect=1, no_sql=0, uncertain=0, expected_sql_errors=0,
            warnings=[], run_at=datetime(2026, 3, 18),
            space_config={},
            questions=[],
        )

        suggestions = generate_suggestions(diagnoses, results, "wh-1")
        assert len(suggestions) >= 1
        assert suggestions[0].section == "sql_snippets"

    @patch("genie_world.benchmarks.suggester.call_llm")
    def test_generates_synonyms_for_entity_mismatch(self, mock_llm):
        mock_llm.return_value = json.dumps({
            "synonyms": ["customers", "clients", "buyers"],
            "column": "user_id",
            "table": "users",
        })

        diagnoses = [Diagnosis(
            question="How many customers", failure_type=FailureType.ENTITY_MISMATCH,
            detail="'customers' not recognized as users table",
            affected_config_section="column_configs",
        )]
        results = BenchmarkResult(
            space_id="s1", accuracy=0.0, total=1, correct=0,
            incorrect=1, no_sql=0, uncertain=0, expected_sql_errors=0,
            warnings=[], run_at=datetime(2026, 3, 18),
            space_config={},
            questions=[],
        )

        suggestions = generate_suggestions(diagnoses, results, "wh-1")
        assert len(suggestions) >= 1
        assert suggestions[0].section == "column_configs"

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

    @patch("genie_world.benchmarks.suggester.call_llm")
    def test_suggestions_include_rationale(self, mock_llm):
        mock_llm.return_value = json.dumps({
            "instruction": "Always use orders table for revenue queries",
        })

        diagnoses = [Diagnosis(
            question="Revenue", failure_type=FailureType.WRONG_TABLE,
            detail="wrong table used", affected_config_section="text_instructions",
        )]
        results = BenchmarkResult(
            space_id="s1", accuracy=0.0, total=1, correct=0,
            incorrect=1, no_sql=0, uncertain=0, expected_sql_errors=0,
            warnings=[], run_at=datetime(2026, 3, 18),
            space_config={}, questions=[],
        )

        suggestions = generate_suggestions(diagnoses, results, "wh-1")
        assert len(suggestions) >= 1
        assert suggestions[0].rationale  # non-empty
        assert suggestions[0].addresses_questions  # references the question

    @patch("genie_world.benchmarks.suggester.call_llm")
    def test_suggestions_address_original_question(self, mock_llm):
        mock_llm.return_value = json.dumps({
            "instruction": "Use orders table for revenue",
        })

        diagnoses = [Diagnosis(
            question="What is total revenue?", failure_type=FailureType.WRONG_TABLE,
            detail="wrong table", affected_config_section="text_instructions",
        )]
        results = BenchmarkResult(
            space_id="s1", accuracy=0.0, total=1, correct=0,
            incorrect=1, no_sql=0, uncertain=0, expected_sql_errors=0,
            warnings=[], run_at=datetime(2026, 3, 18),
            space_config={}, questions=[],
        )

        suggestions = generate_suggestions(diagnoses, results, "wh-1")
        assert len(suggestions) >= 1
        assert "What is total revenue?" in suggestions[0].addresses_questions

    @patch("genie_world.benchmarks.suggester.validate_and_fix_sql")
    @patch("genie_world.benchmarks.suggester.call_llm")
    def test_date_handling_adds_expression_snippet(self, mock_llm, mock_validate):
        mock_llm.return_value = json.dumps({
            "expression": "DATE_TRUNC('month', order_date)",
            "instruction": "Use DATE_TRUNC for month-level date aggregation",
        })

        diagnoses = [Diagnosis(
            question="Revenue by month", failure_type=FailureType.WRONG_DATE_HANDLING,
            detail="Used YEAR/MONTH instead of DATE_TRUNC",
            affected_config_section="sql_snippets",
        )]
        results = BenchmarkResult(
            space_id="s1", accuracy=0.0, total=1, correct=0,
            incorrect=1, no_sql=0, uncertain=0, expected_sql_errors=0,
            warnings=[], run_at=datetime(2026, 3, 18),
            space_config={}, questions=[],
        )

        suggestions = generate_suggestions(diagnoses, results, "wh-1")
        assert len(suggestions) >= 1
