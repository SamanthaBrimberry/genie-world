"""Tests for the metadata profiler (Tier 1)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from genie_world.profiler.models import TableProfile
from genie_world.profiler.metadata_profiler import (
    profile_schema_metadata,
    profile_table_metadata,
)


def _make_mock_column(name: str, type_text: str, nullable: bool, comment: str | None = None):
    col = MagicMock()
    col.name = name
    col.type_text = type_text
    col.nullable = nullable
    col.comment = comment
    return col


def _make_mock_table(
    catalog: str,
    schema: str,
    name: str,
    comment: str | None = None,
    columns: list | None = None,
):
    tbl = MagicMock()
    tbl.catalog_name = catalog
    tbl.schema_name = schema
    tbl.name = name
    tbl.comment = comment
    tbl.columns = columns or []
    return tbl


class TestProfileTableMetadata:
    def test_single_table(self):
        mock_col = _make_mock_column("id", "bigint", False, "Primary key")
        mock_tbl = _make_mock_table("main", "sales", "orders", "All orders", [mock_col])

        mock_client = MagicMock()
        mock_client.tables.get.return_value = mock_tbl

        with patch(
            "genie_world.profiler.metadata_profiler.get_workspace_client",
            return_value=mock_client,
        ):
            result = profile_table_metadata("main", "sales", "orders")

        assert isinstance(result, TableProfile)
        assert result.catalog == "main"
        assert result.schema_name == "sales"
        assert result.table == "orders"
        assert result.description == "All orders"
        assert len(result.columns) == 1
        assert result.columns[0].name == "id"
        assert result.columns[0].data_type == "bigint"
        assert result.columns[0].nullable is False
        assert result.columns[0].description == "Primary key"

        mock_client.tables.get.assert_called_once_with("main.sales.orders")

    def test_table_no_columns_no_description(self):
        mock_tbl = _make_mock_table("main", "raw", "events", comment=None, columns=[])

        mock_client = MagicMock()
        mock_client.tables.get.return_value = mock_tbl

        with patch(
            "genie_world.profiler.metadata_profiler.get_workspace_client",
            return_value=mock_client,
        ):
            result = profile_table_metadata("main", "raw", "events")

        assert result.description is None
        assert result.columns == []


class TestProfileSchemaMetadata:
    def test_all_tables_success(self):
        col1 = _make_mock_column("id", "bigint", False)
        col2 = _make_mock_column("name", "string", True, "Customer name")
        tbl1 = _make_mock_table("main", "sales", "orders", columns=[col1])
        tbl2 = _make_mock_table("main", "sales", "customers", columns=[col2])

        mock_client = MagicMock()
        mock_client.tables.list.return_value = [tbl1, tbl2]
        mock_client.tables.get.side_effect = [tbl1, tbl2]

        with patch(
            "genie_world.profiler.metadata_profiler.get_workspace_client",
            return_value=mock_client,
        ):
            result = profile_schema_metadata("main", "sales")

        assert isinstance(result, list)
        assert len(result) == 2
        tables_by_name = {t.table: t for t in result}
        assert "orders" in tables_by_name
        assert "customers" in tables_by_name
        assert tables_by_name["customers"].columns[0].description == "Customer name"

    def test_error_handling_with_warnings(self):
        tbl_ok = _make_mock_table("main", "sales", "orders")
        tbl_bad = _make_mock_table("main", "sales", "broken")

        mock_client = MagicMock()
        mock_client.tables.list.return_value = [tbl_ok, tbl_bad]
        mock_client.tables.get.side_effect = [
            tbl_ok,
            Exception("Permission denied"),
        ]

        with patch(
            "genie_world.profiler.metadata_profiler.get_workspace_client",
            return_value=mock_client,
        ):
            tables, warnings = profile_schema_metadata("main", "sales", return_warnings=True)

        assert len(tables) == 1
        assert tables[0].table == "orders"
        assert len(warnings) == 1
        assert warnings[0].table == "main.sales.broken"
        assert warnings[0].tier == "metadata"
        assert "Permission denied" in warnings[0].message
