"""Tests for the data profiler (Tier 2)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from genie_world.profiler.models import ColumnProfile, ProfilingWarning, TableProfile
from genie_world.profiler.data_profiler import _build_profile_sql, enrich_table_with_stats


def _make_table(columns: list[ColumnProfile]) -> TableProfile:
    return TableProfile(
        catalog="main",
        schema_name="sales",
        table="orders",
        columns=columns,
    )


class TestBuildProfileSql:
    def test_basic_sql_structure(self):
        """SQL should include COUNT(*) and per-column stats."""
        columns = [
            ColumnProfile(name="id", data_type="BIGINT", nullable=False),
            ColumnProfile(name="name", data_type="STRING", nullable=True),
        ]
        sql = _build_profile_sql("main.sales.orders", columns)

        assert "SELECT" in sql
        assert "COUNT(*)" in sql
        assert "`id`" in sql
        assert "`name`" in sql
        assert "`main`.`sales`.`orders`" in sql

    def test_numeric_columns_get_min_max(self):
        """Numeric columns should get MIN/MAX expressions."""
        columns = [
            ColumnProfile(name="amount", data_type="DOUBLE", nullable=True),
        ]
        sql = _build_profile_sql("main.sales.orders", columns)

        assert "MIN(`amount`)" in sql
        assert "MAX(`amount`)" in sql

    def test_date_columns_get_min_max(self):
        """Date/timestamp columns should get MIN/MAX expressions."""
        columns = [
            ColumnProfile(name="created_at", data_type="TIMESTAMP", nullable=True),
        ]
        sql = _build_profile_sql("main.sales.orders", columns)

        assert "MIN(`created_at`)" in sql
        assert "MAX(`created_at`)" in sql

    def test_string_columns_no_min_max(self):
        """String columns should NOT get MIN/MAX expressions."""
        columns = [
            ColumnProfile(name="status", data_type="STRING", nullable=True),
        ]
        sql = _build_profile_sql("main.sales.orders", columns)

        assert "MIN(`status`)" not in sql
        assert "MAX(`status`)" not in sql

    def test_null_percent_and_distinct(self):
        """SQL should include null % and COUNT(DISTINCT col) for all columns."""
        columns = [
            ColumnProfile(name="category", data_type="STRING", nullable=True),
        ]
        sql = _build_profile_sql("main.sales.orders", columns)

        assert "COUNT(DISTINCT `category`)" in sql
        # null percent: SUM(CASE WHEN col IS NULL THEN 1 ELSE 0 END)
        assert "`category` IS NULL" in sql


class TestEnrichTableWithStats:
    def test_enriches_columns_with_stats(self):
        """Should enrich column profiles with cardinality, null_percent, min/max."""
        columns = [
            ColumnProfile(name="id", data_type="BIGINT", nullable=False),
            ColumnProfile(name="status", data_type="STRING", nullable=True),
        ]
        table = _make_table(columns)

        # execute_sql returns one row: total_count, id_distinct, id_null_sum,
        # id_min, id_max, status_distinct, status_null_sum
        mock_result = {
            "error": None,
            "columns": [
                {"name": "total_count", "type_name": "BIGINT"},
                {"name": "id__distinct", "type_name": "BIGINT"},
                {"name": "id__null_sum", "type_name": "BIGINT"},
                {"name": "id__min", "type_name": "BIGINT"},
                {"name": "id__max", "type_name": "BIGINT"},
                {"name": "status__distinct", "type_name": "BIGINT"},
                {"name": "status__null_sum", "type_name": "BIGINT"},
            ],
            "data": [["1000", "1000", "0", "1", "9999", "5", "100"]],
            "row_count": 1,
            "truncated": False,
        }

        with patch(
            "genie_world.profiler.data_profiler.execute_sql",
            return_value=mock_result,
        ):
            enriched_table, warnings = enrich_table_with_stats(table, "wh-123")

        assert warnings == []
        id_col = next(c for c in enriched_table.columns if c.name == "id")
        assert id_col.cardinality == 1000
        assert id_col.null_percent == 0.0
        assert id_col.min_value == "1"
        assert id_col.max_value == "9999"

        status_col = next(c for c in enriched_table.columns if c.name == "status")
        assert status_col.cardinality == 5
        assert status_col.null_percent == pytest.approx(10.0)
        assert status_col.min_value is None  # STRING type has no min/max
        assert status_col.max_value is None

    def test_sql_error_returns_warning(self):
        """A SQL error should return a warning and the original (unenriched) table."""
        columns = [ColumnProfile(name="id", data_type="BIGINT", nullable=False)]
        table = _make_table(columns)

        mock_result = {
            "error": "Permission denied",
            "columns": [],
            "data": [],
            "row_count": 0,
            "truncated": False,
        }

        with patch(
            "genie_world.profiler.data_profiler.execute_sql",
            return_value=mock_result,
        ):
            enriched_table, warnings = enrich_table_with_stats(table, "wh-123")

        assert len(warnings) == 1
        assert warnings[0].tier == "data"
        assert "Permission denied" in warnings[0].message
        # Original table returned unchanged
        assert enriched_table is table

    def test_empty_result_returns_warning(self):
        """Empty data (no rows) should yield a warning and original table."""
        columns = [ColumnProfile(name="id", data_type="BIGINT", nullable=False)]
        table = _make_table(columns)

        mock_result = {
            "error": None,
            "columns": [],
            "data": [],
            "row_count": 0,
            "truncated": False,
        }

        with patch(
            "genie_world.profiler.data_profiler.execute_sql",
            return_value=mock_result,
        ):
            enriched_table, warnings = enrich_table_with_stats(table, "wh-123")

        assert len(warnings) == 1
        assert warnings[0].tier == "data"
        assert enriched_table is table

    def test_no_columns_returns_table_unchanged(self):
        """A table with no columns should return unchanged without error."""
        table = _make_table([])

        with patch(
            "genie_world.profiler.data_profiler.execute_sql",
        ) as mock_exec:
            enriched_table, warnings = enrich_table_with_stats(table, "wh-123")

        mock_exec.assert_not_called()
        assert enriched_table is table
        assert warnings == []
