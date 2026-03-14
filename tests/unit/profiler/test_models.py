"""Tests for profiler models."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from genie_world.profiler.models import (
    ColumnProfile,
    DetectionMethod,
    ProfilingWarning,
    Relationship,
    SchemaProfile,
    TableProfile,
)


class TestDetectionMethod:
    def test_enum_values(self):
        assert DetectionMethod.UC_CONSTRAINT == "uc_constraint"
        assert DetectionMethod.LINEAGE == "lineage"
        assert DetectionMethod.QUERY_COOCCURRENCE == "query_cooccurrence"
        assert DetectionMethod.NAMING_PATTERN == "naming_pattern"
        assert DetectionMethod.VALUE_OVERLAP == "value_overlap"


class TestColumnProfile:
    def test_minimal_creation(self):
        col = ColumnProfile(name="id", data_type="bigint", nullable=False)
        assert col.name == "id"
        assert col.data_type == "bigint"
        assert col.nullable is False
        assert col.description is None
        assert col.cardinality is None
        assert col.top_values is None

    def test_full_creation(self):
        col = ColumnProfile(
            name="status",
            data_type="string",
            nullable=True,
            description="Order status",
            cardinality=5,
            null_percent=0.01,
            top_values=["active", "pending", "closed"],
            min_value="active",
            max_value="pending",
            sample_values=["active", "closed"],
            synonyms=["state", "order_state"],
            tags={"pii": "false"},
            query_frequency=42,
            co_queried_columns=["order_id", "customer_id"],
        )
        assert col.cardinality == 5
        assert col.top_values == ["active", "pending", "closed"]
        assert col.tags == {"pii": "false"}
        assert col.co_queried_columns == ["order_id", "customer_id"]


class TestTableProfile:
    def test_minimal_creation(self):
        tbl = TableProfile(catalog="main", schema_name="sales", table="orders")
        assert tbl.catalog == "main"
        assert tbl.schema_name == "sales"
        assert tbl.table == "orders"
        assert tbl.columns == []
        assert tbl.description is None

    def test_with_columns(self):
        col = ColumnProfile(name="id", data_type="bigint", nullable=False)
        tbl = TableProfile(
            catalog="main",
            schema_name="sales",
            table="orders",
            description="All orders",
            row_count=1000,
            columns=[col],
            upstream_tables=["main.raw.source"],
            downstream_tables=["main.reporting.summary"],
        )
        assert len(tbl.columns) == 1
        assert tbl.row_count == 1000
        assert tbl.upstream_tables == ["main.raw.source"]


class TestRelationship:
    def test_creation(self):
        rel = Relationship(
            source_table="main.sales.orders",
            source_column="customer_id",
            target_table="main.sales.customers",
            target_column="id",
            confidence=0.95,
            detection_method=DetectionMethod.UC_CONSTRAINT,
        )
        assert rel.confidence == 0.95
        assert rel.detection_method == DetectionMethod.UC_CONSTRAINT


class TestProfilingWarning:
    def test_creation(self):
        warn = ProfilingWarning(
            table="main.sales.orders",
            tier="metadata",
            message="Table not found",
        )
        assert warn.table == "main.sales.orders"
        assert warn.tier == "metadata"


class TestSchemaProfile:
    def test_minimal_creation(self):
        now = datetime.now(tz=timezone.utc)
        profile = SchemaProfile(
            schema_version="1.0",
            catalog="main",
            schema_name="sales",
            tables=[],
            relationships=[],
            profiled_at=now,
        )
        assert profile.schema_version == "1.0"
        assert profile.warnings is None

    def test_serialization_roundtrip(self):
        col = ColumnProfile(name="id", data_type="bigint", nullable=False)
        tbl = TableProfile(catalog="main", schema_name="sales", table="orders", columns=[col])
        rel = Relationship(
            source_table="main.sales.orders",
            source_column="customer_id",
            target_table="main.sales.customers",
            target_column="id",
            confidence=0.9,
            detection_method=DetectionMethod.NAMING_PATTERN,
        )
        warn = ProfilingWarning(table="main.sales.bad", tier="data", message="SQL error")
        now = datetime.now(tz=timezone.utc)
        profile = SchemaProfile(
            schema_version="1.0",
            catalog="main",
            schema_name="sales",
            tables=[tbl],
            relationships=[rel],
            warnings=[warn],
            profiled_at=now,
        )
        data = profile.model_dump()
        restored = SchemaProfile.model_validate(data)
        assert restored.catalog == "main"
        assert len(restored.tables) == 1
        assert restored.tables[0].columns[0].name == "id"
        assert restored.relationships[0].detection_method == DetectionMethod.NAMING_PATTERN
        assert restored.warnings[0].message == "SQL error"
