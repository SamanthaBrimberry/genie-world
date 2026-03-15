import json
import pytest
from unittest.mock import patch
from datetime import datetime
from genie_world.profiler.models import ColumnProfile, SchemaProfile, TableProfile
from genie_world.builder.benchmarks import generate_benchmarks


def _make_profile():
    return SchemaProfile(
        schema_version="1.0", catalog="main", schema_name="sales",
        tables=[TableProfile(
            catalog="main", schema_name="sales", table="orders",
            columns=[ColumnProfile(name="id", data_type="INT", nullable=False)],
        )],
        relationships=[], profiled_at=datetime(2026, 3, 15),
    )


class TestGenerateBenchmarks:
    @patch("genie_world.builder.benchmarks.call_llm")
    def test_generates_benchmarks(self, mock_llm):
        mock_llm.return_value = json.dumps([
            {"question": "How many orders?", "sql": "SELECT COUNT(*) FROM main.sales.orders"},
        ])

        existing_examples = [{"question": "Total revenue", "sql": "SELECT SUM(amount) FROM orders"}]
        result, warnings = generate_benchmarks(_make_profile(), [], {}, existing_examples, count=1)

        assert "questions" in result
        assert len(result["questions"]) == 1
        q = result["questions"][0]
        assert q["question"] == "How many orders?"
        assert q["answer"] == [{"format": "SQL", "content": ["SELECT COUNT(*) FROM main.sales.orders"]}]

    @patch("genie_world.builder.benchmarks.call_llm")
    def test_handles_llm_error(self, mock_llm):
        mock_llm.side_effect = RuntimeError("unavailable")

        result, warnings = generate_benchmarks(_make_profile(), [], {}, [], count=5)

        assert result == {"questions": []}
        assert len(warnings) == 1
