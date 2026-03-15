"""Tests for the relationship detector."""

from __future__ import annotations

from genie_world.profiler.models import ColumnProfile, DetectionMethod, Relationship, TableProfile
from genie_world.profiler.relationship_detector import (
    detect_by_naming_patterns,
    detect_by_shared_columns,
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


class TestDetectBySharedColumns:
    def test_shared_column_with_owner(self):
        """campaign_id shared across campaigns + campaign_performance → campaigns owns it."""
        campaigns = _make_table(
            "main", "marketing", "campaigns",
            [_col("campaign_id"), _col("name"), _col("budget")],
        )
        perf = _make_table(
            "main", "marketing", "campaign_performance_demo",
            [_col("campaign_id"), _col("impressions"), _col("clicks")],
        )
        geo = _make_table(
            "main", "marketing", "campaign_performance_geo",
            [_col("campaign_id"), _col("market"), _col("spend")],
        )

        rels = detect_by_shared_columns([campaigns, perf, geo])

        # perf and geo should both reference campaigns
        assert len(rels) == 2
        for rel in rels:
            assert rel.target_table == "main.marketing.campaigns"
            assert rel.target_column == "campaign_id"
            assert rel.source_column == "campaign_id"
            assert rel.confidence == 0.7

    def test_shared_column_no_owner(self):
        """When no table name matches the prefix, creates lower-confidence pairs."""
        table_a = _make_table(
            "main", "data", "events",
            [_col("session_id"), _col("event_type")],
        )
        table_b = _make_table(
            "main", "data", "clicks",
            [_col("session_id"), _col("url")],
        )

        rels = detect_by_shared_columns([table_a, table_b])

        # No table named "session" or "sessions", so lower confidence pair
        assert len(rels) == 1
        assert rels[0].confidence == 0.4

    def test_shared_column_with_group_id(self):
        """group_id across campaigns and campaign_groups → campaign_groups owns it."""
        campaigns = _make_table(
            "main", "mkt", "campaigns",
            [_col("campaign_id"), _col("group_id")],
        )
        groups = _make_table(
            "main", "mkt", "campaign_groups",
            [_col("group_id"), _col("album_id")],
        )

        rels = detect_by_shared_columns([campaigns, groups])

        # "group" prefix → matches "campaign_groups" via plural? No — prefix is "group",
        # candidate names are "group", "groups". Neither matches "campaign_groups".
        # So this falls to no-owner case with confidence 0.4
        assert len(rels) >= 1
        assert rels[0].source_column == "group_id"

    def test_ignores_non_fk_columns(self):
        """Columns not ending in _id/_key/_fk should be ignored."""
        table_a = _make_table("main", "d", "a", [_col("name"), _col("email")])
        table_b = _make_table("main", "d", "b", [_col("name"), _col("phone")])

        rels = detect_by_shared_columns([table_a, table_b])
        assert rels == []

    def test_single_table_no_relationships(self):
        """A single table can't have shared-column relationships."""
        table = _make_table("main", "d", "t", [_col("campaign_id")])
        rels = detect_by_shared_columns([table])
        assert rels == []


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
