"""Tests for the relationship detector."""

from __future__ import annotations

from genie_world.profiler.models import ColumnProfile, DetectionMethod, Relationship, TableProfile
from genie_world.profiler.relationship_detector import (
    detect_by_naming_patterns,
    merge_relationships,
)


def _make_table(catalog: str, schema: str, name: str, columns: list[ColumnProfile]) -> TableProfile:
    return TableProfile(
        catalog=catalog,
        schema_name=schema,
        table=name,
        columns=columns,
    )


def _col(name: str, data_type: str = "BIGINT", nullable: bool = True) -> ColumnProfile:
    return ColumnProfile(name=name, data_type=data_type, nullable=nullable)


class TestDetectByNamingPatterns:
    def test_id_suffix_matches_target_table(self):
        """A column ending in _id should produce a relationship to the matching table."""
        customers = _make_table(
            "main", "sales", "customers",
            [_col("id"), _col("name")],
        )
        orders = _make_table(
            "main", "sales", "orders",
            [_col("order_id"), _col("customer_id"), _col("amount")],
        )

        relationships = detect_by_naming_patterns([customers, orders])

        # Should detect orders.customer_id -> customers.id
        assert len(relationships) == 1
        rel = relationships[0]
        assert rel.source_table == "main.sales.orders"
        assert rel.source_column == "customer_id"
        assert rel.target_table == "main.sales.customers"
        assert rel.target_column == "id"
        assert rel.confidence == 0.6
        assert rel.detection_method == DetectionMethod.NAMING_PATTERN

    def test_no_self_references(self):
        """A table referencing itself via a column should not produce a relationship."""
        employees = _make_table(
            "main", "hr", "employee",
            # employee_id refers to "employee" table but that IS the same table
            [_col("id"), _col("employee_id"), _col("name")],
        )

        relationships = detect_by_naming_patterns([employees])

        assert relationships == []

    def test_key_suffix_matches_target_table(self):
        """A column ending in _key should also produce a relationship."""
        products = _make_table(
            "main", "store", "products",
            [_col("key"), _col("name")],
        )
        order_items = _make_table(
            "main", "store", "order_items",
            [_col("id"), _col("product_key")],
        )

        relationships = detect_by_naming_patterns([products, order_items])

        assert len(relationships) == 1
        rel = relationships[0]
        assert rel.source_table == "main.store.order_items"
        assert rel.source_column == "product_key"
        assert rel.target_table == "main.store.products"
        assert rel.target_column == "key"
        assert rel.confidence == 0.6
        assert rel.detection_method == DetectionMethod.NAMING_PATTERN

    def test_fk_suffix_matches_target_table(self):
        """A column ending in _fk should also produce a relationship."""
        users = _make_table(
            "main", "app", "users",
            [_col("id"), _col("email")],
        )
        sessions = _make_table(
            "main", "app", "sessions",
            [_col("id"), _col("user_fk")],
        )

        relationships = detect_by_naming_patterns([users, sessions])

        assert len(relationships) == 1
        rel = relationships[0]
        assert rel.source_column == "user_fk"
        assert rel.target_table == "main.app.users"
        assert rel.target_column == "id"


class TestMergeRelationships:
    def test_deduplicates_keeping_highest_confidence(self):
        """merge_relationships should deduplicate by key and keep highest confidence."""
        low_conf = Relationship(
            source_table="main.s.orders",
            source_column="customer_id",
            target_table="main.s.customers",
            target_column="id",
            confidence=0.6,
            detection_method=DetectionMethod.NAMING_PATTERN,
        )
        high_conf = Relationship(
            source_table="main.s.orders",
            source_column="customer_id",
            target_table="main.s.customers",
            target_column="id",
            confidence=1.0,
            detection_method=DetectionMethod.UC_CONSTRAINT,
        )

        merged = merge_relationships([low_conf], [high_conf])

        assert len(merged) == 1
        assert merged[0].confidence == 1.0
        assert merged[0].detection_method == DetectionMethod.UC_CONSTRAINT

    def test_keeps_unique_relationships(self):
        """Distinct relationships should all be preserved."""
        rel1 = Relationship(
            source_table="main.s.orders",
            source_column="customer_id",
            target_table="main.s.customers",
            target_column="id",
            confidence=0.6,
            detection_method=DetectionMethod.NAMING_PATTERN,
        )
        rel2 = Relationship(
            source_table="main.s.order_items",
            source_column="order_id",
            target_table="main.s.orders",
            target_column="id",
            confidence=0.6,
            detection_method=DetectionMethod.NAMING_PATTERN,
        )

        merged = merge_relationships([rel1], [rel2])

        assert len(merged) == 2

    def test_empty_sources(self):
        """merge_relationships with empty inputs should return empty list."""
        assert merge_relationships([], []) == []
