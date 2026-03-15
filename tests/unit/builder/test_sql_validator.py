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
