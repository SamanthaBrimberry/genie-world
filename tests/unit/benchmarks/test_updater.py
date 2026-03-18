"""Tests for the benchmarks updater module."""
import pytest
from unittest.mock import patch, MagicMock
from genie_world.benchmarks.updater import update_space, _merge_suggestions
from genie_world.benchmarks.models import Suggestion


class TestMergeSuggestions:
    def test_add_example(self):
        config = {"instructions": {"example_question_sqls": [
            {"id": "existing1", "question": ["Q1"], "sql": ["SELECT 1"]},
        ]}}
        suggestions = [Suggestion(
            section="example_question_sqls", action="add",
            content={"question": ["Q2"], "sql": ["SELECT 2"]},
            rationale="test", addresses_questions=["Q2"],
        )]

        merged = _merge_suggestions(config, suggestions)
        examples = merged["instructions"]["example_question_sqls"]
        assert len(examples) == 2
        ids = {e["id"] for e in examples}
        assert "existing1" in ids  # preserved
        assert len(ids) == 2  # new ID generated (different from existing)

    def test_remove_by_target_id(self):
        config = {"instructions": {"example_question_sqls": [
            {"id": "keep-me", "question": ["Q1"]},
            {"id": "remove-me", "question": ["Q2"]},
        ]}}
        suggestions = [Suggestion(
            section="example_question_sqls", action="remove",
            target_id="remove-me",
            rationale="test", addresses_questions=[],
        )]

        merged = _merge_suggestions(config, suggestions)
        examples = merged["instructions"]["example_question_sqls"]
        assert len(examples) == 1
        assert examples[0]["id"] == "keep-me"

    def test_update_instruction_content(self):
        config = {"instructions": {"text_instructions": [
            {"id": "instr-1", "content": ["Old instruction"]},
        ]}}
        suggestions = [Suggestion(
            section="text_instructions", action="update",
            target_id="instr-1",
            content={"content": ["Updated instruction"]},
            rationale="test", addresses_questions=[],
        )]

        merged = _merge_suggestions(config, suggestions)
        instr = merged["instructions"]["text_instructions"]
        assert instr[0]["content"] == ["Updated instruction"]
        assert instr[0]["id"] == "instr-1"  # ID preserved

    def test_add_to_empty_section(self):
        config = {"instructions": {"text_instructions": []}}
        suggestions = [Suggestion(
            section="text_instructions", action="add",
            content={"content": ["New instruction"]},
            rationale="test", addresses_questions=["Q1"],
        )]

        merged = _merge_suggestions(config, suggestions)
        instrs = merged["instructions"]["text_instructions"]
        assert len(instrs) == 1
        assert "id" in instrs[0]
        assert instrs[0]["content"] == ["New instruction"]

    def test_add_creates_new_section_if_missing(self):
        config = {}
        suggestions = [Suggestion(
            section="text_instructions", action="add",
            content={"content": ["Instruction"]},
            rationale="test", addresses_questions=[],
        )]

        merged = _merge_suggestions(config, suggestions)
        assert "instructions" in merged
        assert "text_instructions" in merged["instructions"]
        assert len(merged["instructions"]["text_instructions"]) == 1

    def test_remove_nonexistent_id_does_not_error(self):
        config = {"instructions": {"example_question_sqls": [
            {"id": "keep-me", "question": ["Q1"]},
        ]}}
        suggestions = [Suggestion(
            section="example_question_sqls", action="remove",
            target_id="does-not-exist",
            rationale="test", addresses_questions=[],
        )]

        # Should not raise; config unchanged
        merged = _merge_suggestions(config, suggestions)
        examples = merged["instructions"]["example_question_sqls"]
        assert len(examples) == 1

    def test_update_nonexistent_id_does_not_error(self):
        config = {"instructions": {"text_instructions": []}}
        suggestions = [Suggestion(
            section="text_instructions", action="update",
            target_id="no-such-id",
            content={"content": ["Something"]},
            rationale="test", addresses_questions=[],
        )]

        # Should not raise; config unchanged
        merged = _merge_suggestions(config, suggestions)
        instrs = merged["instructions"]["text_instructions"]
        assert len(instrs) == 0

    def test_multiple_suggestions_applied_in_order(self):
        config = {"instructions": {"text_instructions": [
            {"id": "instr-1", "content": ["Old"]},
        ]}}
        suggestions = [
            Suggestion(
                section="text_instructions", action="add",
                content={"content": ["New A"]},
                rationale="test", addresses_questions=[],
            ),
            Suggestion(
                section="text_instructions", action="update",
                target_id="instr-1",
                content={"content": ["Updated"]},
                rationale="test", addresses_questions=[],
            ),
        ]

        merged = _merge_suggestions(config, suggestions)
        instrs = merged["instructions"]["text_instructions"]
        assert len(instrs) == 2
        updated = next(i for i in instrs if i["id"] == "instr-1")
        assert updated["content"] == ["Updated"]

    def test_add_to_sql_snippets_filters(self):
        config = {
            "instructions": {
                "sql_snippets": {"filters": [], "expressions": [], "measures": []}
            }
        }
        suggestions = [Suggestion(
            section="sql_snippets", action="add",
            content={"content": ["WHERE deleted_at IS NULL"], "description": "Exclude deleted"},
            rationale="test", addresses_questions=[],
        )]

        merged = _merge_suggestions(config, suggestions)
        filters = merged["instructions"]["sql_snippets"]["filters"]
        assert len(filters) == 1
        assert "id" in filters[0]

    def test_add_generates_unique_ids(self):
        config = {"instructions": {"text_instructions": []}}
        suggestions = [
            Suggestion(
                section="text_instructions", action="add",
                content={"content": ["Instruction A"]},
                rationale="test", addresses_questions=[],
            ),
            Suggestion(
                section="text_instructions", action="add",
                content={"content": ["Instruction B"]},
                rationale="test", addresses_questions=[],
            ),
        ]

        merged = _merge_suggestions(config, suggestions)
        instrs = merged["instructions"]["text_instructions"]
        ids = [i["id"] for i in instrs]
        assert len(set(ids)) == 2  # all unique

    def test_sorted_by_id_after_merge(self):
        config = {"instructions": {"text_instructions": [
            {"id": "zzz-last", "content": ["Z"]},
            {"id": "aaa-first", "content": ["A"]},
        ]}}
        # Adding an item causes the section to be sorted
        suggestions = [Suggestion(
            section="text_instructions", action="add",
            content={"content": ["New item"]},
            rationale="test", addresses_questions=[],
        )]

        merged = _merge_suggestions(config, suggestions)
        instrs = merged["instructions"]["text_instructions"]
        ids = [i["id"] for i in instrs]
        # The two original items should be sorted relative to each other
        # (new item's uuid is somewhere in the sorted list)
        original_ids = [i for i in ids if i in ("zzz-last", "aaa-first")]
        assert original_ids == sorted(original_ids)


class TestUpdateSpace:
    @patch("genie_world.benchmarks.updater.GenieClient")
    def test_fetches_merges_patches(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_config.return_value = {
            "version": 2,
            "instructions": {"example_question_sqls": [], "text_instructions": [],
                             "sql_functions": [], "join_specs": [],
                             "sql_snippets": {"filters": [], "expressions": [], "measures": []}},
            "data_sources": {"tables": []},
            "config": {"sample_questions": []},
        }
        mock_client.update_config.return_value = {"space_id": "s1"}

        suggestions = [Suggestion(
            section="example_question_sqls", action="add",
            content={"question": ["test"], "sql": ["SELECT 1"]},
            rationale="test", addresses_questions=["Q1"],
        )]

        result = update_space("s1", suggestions, "wh-1")
        assert result.changes_applied == 1
        mock_client.update_config.assert_called_once()

    @patch("genie_world.benchmarks.updater.GenieClient")
    def test_returns_update_result(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_config.return_value = {
            "instructions": {"text_instructions": []},
        }
        mock_client.update_config.return_value = {}

        suggestions = [Suggestion(
            section="text_instructions", action="add",
            content={"content": ["New instruction"]},
            rationale="test", addresses_questions=[],
        )]

        result = update_space("space-abc", suggestions, "wh-1")
        assert result.space_id == "space-abc"
        assert result.changes_applied == 1
        assert isinstance(result.updated_config, dict)

    @patch("genie_world.benchmarks.updater.GenieClient")
    def test_zero_suggestions_still_patches(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_config.return_value = {"instructions": {}}
        mock_client.update_config.return_value = {}

        result = update_space("s1", [], "wh-1")
        assert result.changes_applied == 0
        mock_client.update_config.assert_called_once()

    @patch("genie_world.benchmarks.updater.GenieClient")
    def test_creates_genie_client_with_space_id(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_config.return_value = {}
        mock_client.update_config.return_value = {}

        update_space("my-space-id", [], "wh-1")
        mock_client_class.assert_called_once_with("my-space-id")
