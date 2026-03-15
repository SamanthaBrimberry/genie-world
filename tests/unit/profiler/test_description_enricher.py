import json
import pytest
from unittest.mock import patch
from genie_world.profiler.models import ColumnProfile, TableProfile, ProfilingWarning
from genie_world.profiler.description_enricher import enrich_descriptions_for_table


class TestEnrichDescriptionsForTable:
    @patch("genie_world.profiler.description_enricher.call_llm")
    def test_fills_missing_descriptions(self, mock_llm):
        mock_llm.return_value = json.dumps({
            "table_description": "Customer order records",
            "columns": {
                "id": "Unique order identifier",
                "amount": "Total order value in USD"
            }
        })

        table = TableProfile(
            catalog="main", schema_name="sales", table="orders",
            description=None,
            columns=[
                ColumnProfile(name="id", data_type="INT", nullable=False),
                ColumnProfile(name="amount", data_type="DOUBLE", nullable=True),
            ],
        )

        enriched, warnings = enrich_descriptions_for_table(table)

        assert enriched.description == "Customer order records"
        assert enriched.columns[0].description == "Unique order identifier"
        assert enriched.columns[1].description == "Total order value in USD"
        assert len(warnings) == 0

    @patch("genie_world.profiler.description_enricher.call_llm")
    def test_preserves_existing_descriptions(self, mock_llm):
        mock_llm.return_value = json.dumps({
            "table_description": "LLM generated description",
            "columns": {
                "id": "LLM generated",
                "name": "LLM generated"
            }
        })

        table = TableProfile(
            catalog="main", schema_name="sales", table="customers",
            description="Existing table description",
            columns=[
                ColumnProfile(name="id", data_type="INT", nullable=False, description="Existing col desc"),
                ColumnProfile(name="name", data_type="STRING", nullable=True),
            ],
        )

        enriched, warnings = enrich_descriptions_for_table(table)

        # Existing descriptions should be preserved
        assert enriched.description == "Existing table description"
        assert enriched.columns[0].description == "Existing col desc"
        # Missing description should be filled
        assert enriched.columns[1].description == "LLM generated"

    @patch("genie_world.profiler.description_enricher.call_llm")
    def test_skips_table_with_all_descriptions(self, mock_llm):
        table = TableProfile(
            catalog="main", schema_name="sales", table="orders",
            description="Has description",
            columns=[
                ColumnProfile(name="id", data_type="INT", nullable=False, description="Has desc"),
            ],
        )

        enriched, warnings = enrich_descriptions_for_table(table)

        # Should not call LLM at all
        mock_llm.assert_not_called()
        assert enriched.description == "Has description"

    @patch("genie_world.profiler.description_enricher.call_llm")
    def test_handles_llm_error(self, mock_llm):
        mock_llm.side_effect = RuntimeError("LLM unavailable")

        table = TableProfile(
            catalog="main", schema_name="sales", table="orders",
            description=None,
            columns=[ColumnProfile(name="id", data_type="INT", nullable=False)],
        )

        enriched, warnings = enrich_descriptions_for_table(table)

        assert enriched.description is None  # unchanged
        assert len(warnings) == 1
        assert "LLM unavailable" in warnings[0].message
