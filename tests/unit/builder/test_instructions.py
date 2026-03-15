import json
import pytest
from unittest.mock import patch
from datetime import datetime
from genie_world.profiler.models import ColumnProfile, SchemaProfile, TableProfile
from genie_world.builder.instructions import generate_instructions


def _make_profile():
    return SchemaProfile(
        schema_version="1.0", catalog="main", schema_name="sales",
        tables=[TableProfile(
            catalog="main", schema_name="sales", table="orders",
            description="Order records",
            columns=[ColumnProfile(name="amount", data_type="DOUBLE", nullable=True)],
        )],
        relationships=[], profiled_at=datetime(2026, 3, 15),
    )


class TestGenerateInstructions:
    @patch("genie_world.builder.instructions.call_llm")
    def test_generates_single_instruction(self, mock_llm):
        mock_llm.return_value = json.dumps({
            "content": [
                "When calculating revenue, sum the amount column.",
                "Round monetary values to 2 decimal places."
            ]
        })

        result = generate_instructions(_make_profile(), [], {}, [])

        assert len(result) == 1
        assert isinstance(result[0]["content"], list)
        assert len(result[0]["content"]) == 2

    @patch("genie_world.builder.instructions.call_llm")
    def test_handles_llm_error(self, mock_llm):
        mock_llm.side_effect = RuntimeError("unavailable")

        result = generate_instructions(_make_profile(), [], {}, [])

        assert result == []
