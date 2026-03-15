from genie_world.profiler.models import ColumnProfile, SchemaProfile, TableProfile
from genie_world.builder.data_sources import generate_data_sources
from datetime import datetime


def _make_profile():
    return SchemaProfile(
        schema_version="1.0",
        catalog="main",
        schema_name="sales",
        tables=[
            TableProfile(
                catalog="main", schema_name="sales", table="orders",
                description="Customer orders",
                columns=[
                    ColumnProfile(name="order_id", data_type="STRING", nullable=False, description="Unique ID"),
                    ColumnProfile(name="order_date", data_type="TIMESTAMP", nullable=True, description="When placed"),
                    ColumnProfile(name="status", data_type="STRING", nullable=True, cardinality=5, synonyms=["state", "order status"]),
                    ColumnProfile(name="amount", data_type="DOUBLE", nullable=True),
                    ColumnProfile(name="_metadata", data_type="STRING", nullable=True),
                ],
            ),
        ],
        relationships=[],
        profiled_at=datetime(2026, 3, 15),
    )


class TestGenerateDataSources:
    def test_basic_structure(self):
        profile = _make_profile()
        result = generate_data_sources(profile)

        assert "tables" in result
        assert len(result["tables"]) == 1
        table = result["tables"][0]
        assert table["identifier"] == "main.sales.orders"
        assert table["description"] == ["Customer orders"]

    def test_column_configs_sorted(self):
        profile = _make_profile()
        result = generate_data_sources(profile)
        col_names = [c["column_name"] for c in result["tables"][0]["column_configs"]]
        assert col_names == sorted(col_names)

    def test_entity_matching_for_low_cardinality_string(self):
        profile = _make_profile()
        result = generate_data_sources(profile)
        configs = {c["column_name"]: c for c in result["tables"][0]["column_configs"]}
        assert configs["status"].get("enable_entity_matching") is True

    def test_format_assistance_for_timestamp(self):
        profile = _make_profile()
        result = generate_data_sources(profile)
        configs = {c["column_name"]: c for c in result["tables"][0]["column_configs"]}
        assert configs["order_date"].get("enable_format_assistance") is True

    def test_excludes_internal_columns(self):
        profile = _make_profile()
        result = generate_data_sources(profile)
        configs = {c["column_name"]: c for c in result["tables"][0]["column_configs"]}
        assert configs["_metadata"].get("exclude") is True

    def test_synonyms_included(self):
        profile = _make_profile()
        result = generate_data_sources(profile)
        configs = {c["column_name"]: c for c in result["tables"][0]["column_configs"]}
        assert configs["status"]["synonyms"] == ["state", "order status"]

    def test_tables_sorted_by_identifier(self):
        profile = SchemaProfile(
            schema_version="1.0", catalog="main", schema_name="s",
            tables=[
                TableProfile(catalog="main", schema_name="s", table="zebra", columns=[]),
                TableProfile(catalog="main", schema_name="s", table="alpha", columns=[]),
            ],
            relationships=[], profiled_at=datetime(2026, 3, 15),
        )
        result = generate_data_sources(profile)
        ids = [t["identifier"] for t in result["tables"]]
        assert ids == ["main.s.alpha", "main.s.zebra"]
