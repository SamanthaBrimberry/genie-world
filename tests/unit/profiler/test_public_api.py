"""Tests for the public profiler API (profile_schema / profile_tables)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from genie_world.profiler import profile_schema, profile_tables
from genie_world.profiler.models import (
    ColumnProfile,
    ProfilingWarning,
    SchemaProfile,
    TableProfile,
)


def _make_table(catalog: str, schema: str, name: str) -> TableProfile:
    return TableProfile(
        catalog=catalog,
        schema_name=schema,
        table=name,
        columns=[
            ColumnProfile(name="id", data_type="BIGINT", nullable=False),
            ColumnProfile(name="name", data_type="STRING", nullable=True),
        ],
    )


class TestProfileSchema:
    def test_metadata_only(self):
        """profile_schema with no flags should run only Tier 1 metadata."""
        mock_tables = [
            _make_table("main", "sales", "orders"),
            _make_table("main", "sales", "customers"),
        ]

        with (
            patch(
                "genie_world.profiler.metadata_profiler.profile_schema_metadata",
                return_value=(mock_tables, []),
            ) as mock_meta,
            patch(
                "genie_world.profiler.data_profiler.enrich_table_with_stats"
            ) as mock_data,
            patch(
                "genie_world.profiler.usage_profiler.enrich_with_usage"
            ) as mock_usage,
            patch(
                "genie_world.profiler.synonym_generator.generate_synonyms_for_table"
            ) as mock_syn,
        ):
            result = profile_schema("main", "sales")

        assert isinstance(result, SchemaProfile)
        assert result.schema_version == "1.0"
        assert result.catalog == "main"
        assert result.schema_name == "sales"
        assert len(result.tables) == 2

        # Tier 1 called once
        mock_meta.assert_called_once_with("main", "sales", return_warnings=True)
        # Tier 2, 3, synonyms NOT called
        mock_data.assert_not_called()
        mock_usage.assert_not_called()
        mock_syn.assert_not_called()

    def test_deep_and_synonyms(self):
        """profile_schema with deep=True and synonyms=True should run Tiers 1+2+synonyms."""
        mock_tables = [_make_table("main", "sales", "orders")]
        enriched_table = mock_tables[0].model_copy(
            update={"row_count": 1000}
        )
        syn_table = enriched_table.model_copy()

        with (
            patch(
                "genie_world.profiler.metadata_profiler.profile_schema_metadata",
                return_value=(mock_tables, []),
            ),
            patch(
                "genie_world.profiler.data_profiler.enrich_table_with_stats",
                return_value=(enriched_table, []),
            ) as mock_data,
            patch(
                "genie_world.profiler.usage_profiler.enrich_with_usage"
            ) as mock_usage,
            patch(
                "genie_world.profiler.generate_synonyms_for_table",
                return_value=(syn_table, []),
            ) as mock_syn,
        ):
            result = profile_schema(
                "main", "sales",
                deep=True,
                synonyms=True,
                warehouse_id="wh-test",
            )

        assert isinstance(result, SchemaProfile)
        # Tier 2 was called
        mock_data.assert_called_once()
        # Usage NOT called (usage=False)
        mock_usage.assert_not_called()
        # Synonyms were called
        mock_syn.assert_called_once()

    def test_relationships_detected_from_naming(self):
        """profile_schema should always detect naming-pattern relationships."""
        customers = _make_table("main", "sales", "customers")
        orders = TableProfile(
            catalog="main",
            schema_name="sales",
            table="orders",
            columns=[
                ColumnProfile(name="id", data_type="BIGINT", nullable=False),
                ColumnProfile(name="customer_id", data_type="BIGINT", nullable=True),
            ],
        )

        with patch(
            "genie_world.profiler.metadata_profiler.profile_schema_metadata",
            return_value=([customers, orders], []),
        ):
            result = profile_schema("main", "sales")

        # Should have detected orders.customer_id -> customers.id
        assert len(result.relationships) >= 1
        rel = result.relationships[0]
        assert rel.source_column == "customer_id"
        assert "customers" in rel.target_table


class TestProfileTables:
    def test_profiles_specific_tables(self):
        """profile_tables should call profile_table_metadata for each specified table."""
        mock_table = _make_table("main", "sales", "orders")

        with (
            patch(
                "genie_world.profiler.metadata_profiler.profile_table_metadata",
                return_value=mock_table,
            ) as mock_meta,
            patch(
                "genie_world.profiler.data_profiler.enrich_table_with_stats"
            ) as mock_data,
        ):
            result = profile_tables(["main.sales.orders"])

        assert isinstance(result, SchemaProfile)
        assert result.catalog == "main"
        assert result.schema_name == "sales"
        assert len(result.tables) == 1
        mock_meta.assert_called_once_with("main", "sales", "orders")
        mock_data.assert_not_called()

    def test_rejects_tables_from_different_schemas(self):
        """profile_tables should raise ValueError if tables span multiple catalog.schema pairs."""
        with pytest.raises(ValueError, match="same catalog.schema"):
            profile_tables(["main.sales.orders", "main.hr.employees"])

    def test_rejects_invalid_table_format(self):
        """profile_tables should raise ValueError for table names not in 3-part format."""
        with pytest.raises(ValueError, match="catalog.schema.table"):
            profile_tables(["orders"])

    def test_profile_tables_multiple_tables(self):
        """profile_tables should handle multiple tables in the same schema."""
        orders = _make_table("main", "sales", "orders")
        customers = _make_table("main", "sales", "customers")

        call_map = {
            ("main", "sales", "orders"): orders,
            ("main", "sales", "customers"): customers,
        }

        def mock_profile(cat, sch, tbl):
            return call_map[(cat, sch, tbl)]

        with patch(
            "genie_world.profiler.metadata_profiler.profile_table_metadata",
            side_effect=mock_profile,
        ):
            result = profile_tables(["main.sales.orders", "main.sales.customers"])

        assert len(result.tables) == 2
        table_names = {t.table for t in result.tables}
        assert table_names == {"orders", "customers"}
