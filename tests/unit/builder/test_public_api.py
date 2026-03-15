import json
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from genie_world.profiler.models import ColumnProfile, SchemaProfile, TableProfile
from genie_world.builder import build_space, BuildResult


def _make_profile():
    return SchemaProfile(
        schema_version="1.0", catalog="main", schema_name="sales",
        tables=[TableProfile(
            catalog="main", schema_name="sales", table="orders",
            description="Orders",
            columns=[
                ColumnProfile(name="id", data_type="INT", nullable=False, description="PK"),
                ColumnProfile(name="amount", data_type="DOUBLE", nullable=True, description="Total"),
            ],
        )],
        relationships=[], profiled_at=datetime(2026, 3, 15),
    )


class TestBuildSpace:
    @patch("genie_world.builder.generate_instructions")
    @patch("genie_world.builder.generate_benchmarks")
    @patch("genie_world.builder.generate_example_sqls")
    @patch("genie_world.builder.generate_snippets")
    def test_orchestrates_all_generators(self, mock_snippets, mock_examples, mock_benchmarks, mock_instructions):
        mock_snippets.return_value = {"filters": [], "expressions": [], "measures": []}
        mock_examples.return_value = ([{"question": "Q", "sql": "SELECT 1"}], [])
        mock_benchmarks.return_value = ({"questions": []}, [])
        mock_instructions.return_value = [{"content": ["Test"]}]

        result = build_space(_make_profile())

        assert isinstance(result, BuildResult)
        assert result.config["version"] == 2
        assert "data_sources" in result.config
        assert "instructions" in result.config
        mock_snippets.assert_called_once()
        mock_examples.assert_called_once()
        mock_benchmarks.assert_called_once()
        mock_instructions.assert_called_once()

    @patch("genie_world.builder.generate_instructions")
    @patch("genie_world.builder.generate_benchmarks")
    @patch("genie_world.builder.generate_example_sqls")
    @patch("genie_world.builder.generate_snippets")
    def test_warns_when_no_warehouse(self, mock_snippets, mock_examples, mock_benchmarks, mock_instructions):
        mock_snippets.return_value = {"filters": [], "expressions": [], "measures": []}
        mock_examples.return_value = ([], [])
        mock_benchmarks.return_value = ({"questions": []}, [])
        mock_instructions.return_value = []

        result = build_space(_make_profile(), warehouse_id=None)

        assert any("validation skipped" in w.message.lower() for w in result.warnings)
