import json
import pytest
from unittest.mock import patch
from datetime import datetime
from genie_world.profiler.models import ColumnProfile, SchemaProfile, TableProfile
from genie_world.builder.snippets import generate_snippets


def _make_profile():
    return SchemaProfile(
        schema_version="1.0", catalog="main", schema_name="sales",
        tables=[TableProfile(
            catalog="main", schema_name="sales", table="orders",
            columns=[
                ColumnProfile(name="order_date", data_type="TIMESTAMP", nullable=True),
                ColumnProfile(name="amount", data_type="DOUBLE", nullable=True),
                ColumnProfile(name="status", data_type="STRING", nullable=True, cardinality=5,
                              top_values=["active", "completed", "cancelled"]),
            ],
        )],
        relationships=[], profiled_at=datetime(2026, 3, 15),
    )


class TestGenerateSnippets:
    @patch("genie_world.builder.snippets.call_llm")
    def test_returns_all_three_sections(self, mock_llm):
        mock_llm.return_value = json.dumps({
            "filters": [
                {"sql": "status = 'active'", "display_name": "active orders",
                 "synonyms": ["current orders"], "comment": "Active only", "instruction": "Use for active"}
            ],
            "expressions": [
                {"alias": "order_year", "sql": "YEAR(order_date)", "display_name": "year",
                 "synonyms": ["fiscal year"], "comment": "Extract year", "instruction": "Year analysis"}
            ],
            "measures": [
                {"alias": "total_revenue", "sql": "SUM(amount)", "display_name": "total revenue",
                 "synonyms": ["revenue"], "comment": "Sum amounts", "instruction": "Revenue calc"}
            ],
        })

        result = generate_snippets(_make_profile())

        assert "filters" in result
        assert "expressions" in result
        assert "measures" in result
        assert len(result["filters"]) == 1
        assert result["expressions"][0]["alias"] == "order_year"
        assert result["measures"][0]["alias"] == "total_revenue"

    @patch("genie_world.builder.snippets.call_llm")
    def test_handles_llm_error(self, mock_llm):
        mock_llm.side_effect = RuntimeError("LLM unavailable")

        result = generate_snippets(_make_profile())

        assert result == {"filters": [], "expressions": [], "measures": []}
