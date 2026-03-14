"""Tests for the usage profiler (Tier 3)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from genie_world.profiler.models import (
    ColumnProfile,
    DetectionMethod,
    ProfilingWarning,
    Relationship,
    TableProfile,
)
from genie_world.profiler.usage_profiler import (
    _validate_identifier,
    enrich_with_usage,
    get_declared_relationships,
)


def _make_table(catalog: str, schema: str, name: str) -> TableProfile:
    return TableProfile(
        catalog=catalog,
        schema_name=schema,
        table=name,
        columns=[ColumnProfile(name="id", data_type="BIGINT", nullable=False)],
    )


class TestValidateIdentifier:
    def test_valid_identifiers(self):
        assert _validate_identifier("my_catalog", "catalog") == "my_catalog"
        assert _validate_identifier("sales123", "schema") == "sales123"
        assert _validate_identifier("MySchema", "schema") == "MySchema"

    def test_rejects_dot_injection(self):
        with pytest.raises(ValueError, match="Invalid catalog"):
            _validate_identifier("main.evil", "catalog")

    def test_rejects_semicolon_injection(self):
        with pytest.raises(ValueError, match="Invalid schema"):
            _validate_identifier("sales; DROP TABLE", "schema")

    def test_rejects_backtick_injection(self):
        with pytest.raises(ValueError, match="Invalid catalog"):
            _validate_identifier("`main`", "catalog")


class TestGetDeclaredRelationships:
    def test_returns_relationships_from_fk_constraints(self):
        """Should parse FK constraints from system tables into Relationship objects."""
        mock_result = {
            "error": None,
            "columns": [
                {"name": "fk_table", "type_name": "STRING"},
                {"name": "fk_column", "type_name": "STRING"},
                {"name": "pk_table", "type_name": "STRING"},
                {"name": "pk_column", "type_name": "STRING"},
            ],
            "data": [
                ["main.sales.orders", "customer_id", "main.sales.customers", "id"],
            ],
            "row_count": 1,
            "truncated": False,
        }

        with patch(
            "genie_world.profiler.usage_profiler.execute_sql",
            return_value=mock_result,
        ):
            relationships, warnings = get_declared_relationships(
                "main", "sales", warehouse_id="wh-123"
            )

        assert warnings == []
        assert len(relationships) == 1
        rel = relationships[0]
        assert isinstance(rel, Relationship)
        assert rel.source_table == "main.sales.orders"
        assert rel.source_column == "customer_id"
        assert rel.target_table == "main.sales.customers"
        assert rel.target_column == "id"
        assert rel.confidence == 1.0
        assert rel.detection_method == DetectionMethod.UC_CONSTRAINT

    def test_system_table_not_accessible_returns_warning(self):
        """Should gracefully handle system table permission errors."""
        mock_result = {
            "error": "PERMISSION_DENIED: Cannot access system.information_schema",
            "columns": [],
            "data": [],
            "row_count": 0,
            "truncated": False,
        }

        with patch(
            "genie_world.profiler.usage_profiler.execute_sql",
            return_value=mock_result,
        ):
            relationships, warnings = get_declared_relationships(
                "main", "sales", warehouse_id="wh-123"
            )

        assert relationships == []
        assert len(warnings) == 1
        assert warnings[0].tier == "usage"
        assert "PERMISSION_DENIED" in warnings[0].message

    def test_rejects_invalid_catalog(self):
        """Should raise ValueError for invalid catalog name."""
        with pytest.raises(ValueError, match="Invalid catalog"):
            get_declared_relationships("bad-catalog!", "sales", warehouse_id="wh-123")

    def test_rejects_invalid_schema(self):
        """Should raise ValueError for invalid schema name."""
        with pytest.raises(ValueError, match="Invalid schema"):
            get_declared_relationships("main", "bad schema", warehouse_id="wh-123")


class TestEnrichWithUsage:
    def test_enriches_tables_with_query_frequency(self):
        """Should enrich tables with query frequency from system.query.history."""
        tables = [
            _make_table("main", "sales", "orders"),
            _make_table("main", "sales", "customers"),
        ]

        mock_result = {
            "error": None,
            "columns": [
                {"name": "table_name", "type_name": "STRING"},
                {"name": "query_count", "type_name": "BIGINT"},
            ],
            "data": [
                ["main.sales.orders", "42"],
                ["main.sales.customers", "17"],
            ],
            "row_count": 2,
            "truncated": False,
        }

        with patch(
            "genie_world.profiler.usage_profiler.execute_sql",
            return_value=mock_result,
        ):
            enriched_tables, warnings = enrich_with_usage(
                tables, "main", "sales", warehouse_id="wh-123"
            )

        assert warnings == []
        by_name = {t.table: t for t in enriched_tables}
        assert by_name["orders"].query_frequency == 42
        assert by_name["customers"].query_frequency == 17

    def test_system_table_not_accessible_returns_warning(self):
        """Should gracefully handle inaccessible system.query.history."""
        tables = [_make_table("main", "sales", "orders")]

        mock_result = {
            "error": "TABLE_OR_VIEW_NOT_FOUND: system.query.history",
            "columns": [],
            "data": [],
            "row_count": 0,
            "truncated": False,
        }

        with patch(
            "genie_world.profiler.usage_profiler.execute_sql",
            return_value=mock_result,
        ):
            enriched_tables, warnings = enrich_with_usage(
                tables, "main", "sales", warehouse_id="wh-123"
            )

        # Returns original tables unchanged, with a warning
        assert len(warnings) == 1
        assert warnings[0].tier == "usage"
        assert "TABLE_OR_VIEW_NOT_FOUND" in warnings[0].message
        assert enriched_tables[0].query_frequency is None

    def test_tables_not_in_history_keep_none_frequency(self):
        """Tables not found in query history should retain None query_frequency."""
        tables = [
            _make_table("main", "sales", "orders"),
            _make_table("main", "sales", "rarely_queried"),
        ]

        # Only 'orders' appears in results
        mock_result = {
            "error": None,
            "columns": [
                {"name": "table_name", "type_name": "STRING"},
                {"name": "query_count", "type_name": "BIGINT"},
            ],
            "data": [["main.sales.orders", "5"]],
            "row_count": 1,
            "truncated": False,
        }

        with patch(
            "genie_world.profiler.usage_profiler.execute_sql",
            return_value=mock_result,
        ):
            enriched_tables, warnings = enrich_with_usage(
                tables, "main", "sales", warehouse_id="wh-123"
            )

        assert warnings == []
        by_name = {t.table: t for t in enriched_tables}
        assert by_name["orders"].query_frequency == 5
        assert by_name["rarely_queried"].query_frequency is None

    def test_empty_tables_list(self):
        """An empty tables list should return empty with no SQL calls."""
        with patch(
            "genie_world.profiler.usage_profiler.execute_sql"
        ) as mock_exec:
            enriched_tables, warnings = enrich_with_usage(
                [], "main", "sales", warehouse_id="wh-123"
            )

        mock_exec.assert_not_called()
        assert enriched_tables == []
        assert warnings == []
