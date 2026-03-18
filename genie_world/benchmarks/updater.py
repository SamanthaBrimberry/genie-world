"""Benchmarks updater: merge suggestions into space config and PATCH via GenieClient."""

from __future__ import annotations

import copy
import logging
from uuid import uuid4

from genie_world.benchmarks.models import Suggestion, UpdateResult
from genie_world.core.genie_client import GenieClient
from genie_world.core.tracing import trace

logger = logging.getLogger(__name__)

# Sections that live inside config["instructions"]
_INSTRUCTION_SECTIONS = {
    "example_question_sqls",
    "text_instructions",
    "sql_functions",
    "join_specs",
    "column_configs",
}

# sql_snippets sub-sections (nested under instructions.sql_snippets)
_SQL_SNIPPETS_SUBSECTIONS = {"filters", "expressions", "measures"}


def _get_section_list(config: dict, section: str) -> list:
    """Return a mutable reference to the list for the given section, creating it if needed.

    Handles three location patterns:
    1. instructions.<section> (flat list like text_instructions, example_question_sqls)
    2. instructions.sql_snippets.<section> (for filters, expressions, measures)
    3. "sql_snippets" as the section name → defaults to the "filters" sublist
    4. Direct top-level section as fallback
    """
    instructions = config.setdefault("instructions", {})

    if section in _SQL_SNIPPETS_SUBSECTIONS:
        sql_snippets = instructions.setdefault("sql_snippets", {})
        return sql_snippets.setdefault(section, [])

    # When section is "sql_snippets" directly, default to the "filters" sublist
    if section == "sql_snippets":
        sql_snippets = instructions.setdefault("sql_snippets", {})
        return sql_snippets.setdefault("filters", [])

    if section in _INSTRUCTION_SECTIONS:
        return instructions.setdefault(section, [])

    # Fallback: try instructions first, then top-level
    if section in instructions:
        val = instructions[section]
        if isinstance(val, list):
            return val
    return config.setdefault(section, [])


def _merge_suggestions(config: dict, suggestions: list[Suggestion]) -> dict:
    """Merge a list of suggestions into a config dict (deep copy, non-destructive).

    Actions:
    - "add"    → append item with a new uuid4().hex ID
    - "update" → find by target_id, replace content fields, preserve ID
    - "remove" → find by target_id, delete item

    After all merges, sorts each modified section list by "id" (lexicographic).

    Args:
        config: Current space config dict.
        suggestions: List of Suggestion objects to apply.

    Returns:
        New config dict with suggestions applied.
    """
    result = copy.deepcopy(config)
    modified_sections: set[str] = set()

    for suggestion in suggestions:
        section = suggestion.section
        section_list = _get_section_list(result, section)
        action = suggestion.action

        if action == "add":
            # Special case: text_instructions is limited to 1 item
            # Append content to existing instruction instead of adding new entry
            if section == "text_instructions" and section_list:
                existing = section_list[0]
                new_content = (suggestion.content or {}).get("content", [])
                if isinstance(new_content, str):
                    new_content = [new_content]
                existing_content = existing.get("content", [])
                if isinstance(existing_content, str):
                    existing_content = [existing_content]
                existing["content"] = existing_content + new_content
                modified_sections.add(section)
            else:
                new_item = dict(suggestion.content or {})
                new_item["id"] = uuid4().hex
                section_list.append(new_item)
                modified_sections.add(section)

        elif action == "update":
            target_id = suggestion.target_id
            if target_id is None:
                logger.warning("Suggestion action='update' has no target_id; skipping.")
                continue
            for item in section_list:
                if item.get("id") == target_id:
                    preserved_id = item["id"]
                    item.update(suggestion.content or {})
                    item["id"] = preserved_id  # ensure ID is not overwritten
                    modified_sections.add(section)
                    break
            else:
                logger.warning(
                    "update: target_id %r not found in section %r; skipping.",
                    target_id,
                    section,
                )

        elif action == "remove":
            target_id = suggestion.target_id
            if target_id is None:
                logger.warning("Suggestion action='remove' has no target_id; skipping.")
                continue
            before_len = len(section_list)
            section_list[:] = [item for item in section_list if item.get("id") != target_id]
            if len(section_list) < before_len:
                modified_sections.add(section)
            else:
                logger.warning(
                    "remove: target_id %r not found in section %r; nothing removed.",
                    target_id,
                    section,
                )

        else:
            logger.warning("Unknown suggestion action %r; skipping.", action)

    # Sort modified sections by id
    for section in modified_sections:
        section_list = _get_section_list(result, section)
        try:
            section_list.sort(key=lambda item: item.get("id", ""))
        except Exception as e:
            logger.warning("Could not sort section %r: %s", section, e)

    return result


@trace(name="update_space", span_type="CHAIN")
def update_space(
    space_id: str,
    suggestions: list[Suggestion],
    warehouse_id: str,
) -> UpdateResult:
    """Fetch current config, apply suggestions, and PATCH via GenieClient.

    Steps:
    1. Fetch current config via GenieClient.get_config()
    2. Merge suggestions into config via _merge_suggestions()
    3. Sort modified sections by id
    4. PATCH via GenieClient.update_config()
    5. Return UpdateResult

    Args:
        space_id: Genie space ID.
        suggestions: List of Suggestion objects to apply.
        warehouse_id: Databricks warehouse (reserved for future SQL validation).

    Returns:
        UpdateResult with space_id, changes_applied count, and updated_config.
    """
    client = GenieClient(space_id)

    # Fetch current config
    current_config = client.get_config()

    # Merge suggestions
    updated_config = _merge_suggestions(current_config, suggestions)

    # PATCH via GenieClient
    client.update_config(updated_config)

    return UpdateResult(
        space_id=space_id,
        changes_applied=len(suggestions),
        updated_config=updated_config,
    )
