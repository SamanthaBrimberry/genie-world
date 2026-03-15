from datetime import datetime
from genie_world.profiler.models import DetectionMethod, Relationship, SchemaProfile, TableProfile
from genie_world.builder.join_specs import generate_join_specs


def _make_profile_with_rels():
    return SchemaProfile(
        schema_version="1.0", catalog="main", schema_name="sales",
        tables=[
            TableProfile(catalog="main", schema_name="sales", table="orders", columns=[]),
            TableProfile(catalog="main", schema_name="sales", table="customers", columns=[]),
        ],
        relationships=[
            Relationship(
                source_table="main.sales.orders", source_column="customer_id",
                target_table="main.sales.customers", target_column="id",
                confidence=0.7, detection_method=DetectionMethod.NAMING_PATTERN,
            ),
            Relationship(
                source_table="main.sales.orders", source_column="internal_ref",
                target_table="main.sales.logs", target_column="ref_id",
                confidence=0.3, detection_method=DetectionMethod.VALUE_OVERLAP,
            ),
        ],
        profiled_at=datetime(2026, 3, 15),
    )


class TestGenerateJoinSpecs:
    def test_generates_join_for_high_confidence(self):
        profile = _make_profile_with_rels()
        specs = generate_join_specs(profile)

        assert len(specs) == 1  # Only confidence >= 0.6
        spec = specs[0]
        assert spec["left"]["identifier"] == "main.sales.orders"
        assert spec["right"]["identifier"] == "main.sales.customers"
        assert "orders.customer_id = customers.id" in spec["sql"][0]

    def test_filters_low_confidence(self):
        profile = _make_profile_with_rels()
        specs = generate_join_specs(profile)
        # Low confidence relationship (0.3) should be excluded
        assert all("internal_ref" not in str(s) for s in specs)

    def test_includes_comment_and_instruction(self):
        profile = _make_profile_with_rels()
        specs = generate_join_specs(profile)
        spec = specs[0]
        assert "comment" in spec
        assert "instruction" in spec
        assert isinstance(spec["comment"], str)
        assert isinstance(spec["instruction"], str)

    def test_empty_relationships(self):
        profile = SchemaProfile(
            schema_version="1.0", catalog="main", schema_name="s",
            tables=[], relationships=[],
            profiled_at=datetime(2026, 3, 15),
        )
        specs = generate_join_specs(profile)
        assert specs == []
