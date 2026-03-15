import json
import pytest
from unittest.mock import patch
from datetime import datetime
from genie_world.profiler.models import ColumnProfile, SchemaProfile, TableProfile
from genie_world.builder.example_sqls import generate_example_sqls


def _make_profile():
    return SchemaProfile(
        schema_version="1.0", catalog="main", schema_name="sales",
        tables=[TableProfile(
            catalog="main", schema_name="sales", table="orders",
            description="Order records",
            columns=[
                ColumnProfile(name="id", data_type="INT", nullable=False),
                ColumnProfile(name="amount", data_type="DOUBLE", nullable=True),
            ],
        )],
        relationships=[], profiled_at=datetime(2026, 3, 15),
    )


class TestGenerateExampleSqls:
    @patch("genie_world.builder.example_sqls.call_llm")
    def test_generates_examples(self, mock_llm):
        mock_llm.return_value = json.dumps([
            {"question": "Total orders", "sql": "SELECT COUNT(*) FROM main.sales.orders"},
            {"question": "Average amount", "sql": "SELECT AVG(amount) FROM main.sales.orders"},
        ])

        examples, warnings = generate_example_sqls(_make_profile(), [], {}, count=2)

        assert len(examples) == 2
        assert examples[0]["question"] == "Total orders"
        assert examples[0]["sql"] == "SELECT COUNT(*) FROM main.sales.orders"

    @patch("genie_world.builder.example_sqls.validate_and_fix_sql")
    @patch("genie_world.builder.example_sqls.call_llm")
    def test_validates_with_warehouse(self, mock_llm, mock_validate):
        mock_llm.return_value = json.dumps([
            {"question": "Total orders", "sql": "SELECT COUNT(*) FROM orders"},
        ])
        mock_validate.return_value = ("SELECT COUNT(*) FROM main.sales.orders", [])

        examples, warnings = generate_example_sqls(
            _make_profile(), [], {}, warehouse_id="wh-123", count=1
        )

        mock_validate.assert_called_once()
        assert examples[0]["sql"] == "SELECT COUNT(*) FROM main.sales.orders"

    @patch("genie_world.builder.example_sqls.call_llm")
    def test_handles_llm_error(self, mock_llm):
        mock_llm.side_effect = RuntimeError("unavailable")

        examples, warnings = generate_example_sqls(_make_profile(), [], {}, count=5)

        assert examples == []
        assert len(warnings) == 1
