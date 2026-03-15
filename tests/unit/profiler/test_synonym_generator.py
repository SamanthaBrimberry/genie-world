"""Tests for the synonym generator."""

from __future__ import annotations

import json
from unittest.mock import patch

from genie_world.profiler.models import ColumnProfile, TableProfile
from genie_world.profiler.synonym_generator import (
    _build_synonym_prompt,
    generate_synonyms_for_table,
)


def _make_table() -> TableProfile:
    return TableProfile(
        catalog="main",
        schema_name="sales",
        table="orders",
        columns=[
            ColumnProfile(name="order_id", data_type="BIGINT", nullable=False),
            ColumnProfile(name="customer_id", data_type="BIGINT", nullable=False),
            ColumnProfile(name="total_amount", data_type="DECIMAL", nullable=True),
        ],
    )


class TestBuildSynonymPrompt:
    def test_prompt_contains_table_name(self):
        """The prompt should include the fully-qualified table name."""
        table = _make_table()
        messages = _build_synonym_prompt("main.sales.orders", table.columns)

        full_text = " ".join(m["content"] for m in messages)
        assert "main.sales.orders" in full_text

    def test_prompt_contains_all_column_names(self):
        """The prompt should reference every column name."""
        table = _make_table()
        messages = _build_synonym_prompt("main.sales.orders", table.columns)

        full_text = " ".join(m["content"] for m in messages)
        for col in table.columns:
            assert col.name in full_text

    def test_prompt_has_system_and_user_messages(self):
        """The prompt should be in OpenAI chat format with system + user messages."""
        table = _make_table()
        messages = _build_synonym_prompt("main.sales.orders", table.columns)

        roles = [m["role"] for m in messages]
        assert "system" in roles
        assert "user" in roles

    def test_prompt_requests_json_output(self):
        """The prompt should ask the LLM to return JSON."""
        table = _make_table()
        messages = _build_synonym_prompt("main.sales.orders", table.columns)

        full_text = " ".join(m["content"] for m in messages)
        assert "JSON" in full_text or "json" in full_text.lower()


class TestGenerateSynonymsForTable:
    def test_successful_generation_applies_synonyms(self):
        """When the LLM returns valid JSON, synonyms should be applied to columns."""
        table = _make_table()

        llm_response = json.dumps({
            "order_id": ["order number", "purchase id"],
            "customer_id": ["client id", "buyer id"],
            "total_amount": ["price", "cost", "invoice total"],
        })

        with patch(
            "genie_world.profiler.synonym_generator.call_llm",
            return_value=llm_response,
        ):
            enriched_table, warnings = generate_synonyms_for_table(table)

        assert warnings == []
        by_name = {col.name: col for col in enriched_table.columns}
        assert by_name["order_id"].synonyms == ["order number", "purchase id"]
        assert by_name["customer_id"].synonyms == ["client id", "buyer id"]
        assert by_name["total_amount"].synonyms == ["price", "cost", "invoice total"]

    def test_llm_error_returns_original_table_with_warning(self):
        """When the LLM raises an exception, the original table is returned with a warning."""
        table = _make_table()

        with patch(
            "genie_world.profiler.synonym_generator.call_llm",
            side_effect=RuntimeError("LLM service unavailable"),
        ):
            enriched_table, warnings = generate_synonyms_for_table(table)

        # Original table returned unchanged
        assert enriched_table == table

        assert len(warnings) == 1
        w = warnings[0]
        assert w.tier == "synonyms"
        assert w.table == "main.sales.orders"
        assert "LLM service unavailable" in w.message

    def test_partial_llm_response_keeps_unmatched_columns(self):
        """Columns not in the LLM response should keep their original (no synonyms)."""
        table = _make_table()

        # LLM only returns synonyms for one column
        llm_response = json.dumps({"order_id": ["purchase id"]})

        with patch(
            "genie_world.profiler.synonym_generator.call_llm",
            return_value=llm_response,
        ):
            enriched_table, warnings = generate_synonyms_for_table(table)

        assert warnings == []
        by_name = {col.name: col for col in enriched_table.columns}
        assert by_name["order_id"].synonyms == ["purchase id"]
        # Unmatched columns keep None synonyms
        assert by_name["customer_id"].synonyms is None
        assert by_name["total_amount"].synonyms is None
