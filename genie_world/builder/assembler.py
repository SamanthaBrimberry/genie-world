"""Assembler: combines generated sections into a valid Genie Space config."""

from __future__ import annotations

import logging
import uuid

logger = logging.getLogger(__name__)

_STRING_ARRAY_FIELDS = {
    "description", "content", "question", "sql", "instruction",
    "synonyms", "usage_guidance", "comment",
}

_MAX_STRING_LENGTH = 1024  # 1 KB per string element


def _gen_id() -> str:
    """Generate a 32-character lowercase hex ID."""
    return uuid.uuid4().hex


def _ensure_string_array(value) -> list[str]:
    """Wrap a bare string in a list; split if > 1KB."""
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return value

    result = []
    for item in value:
        if isinstance(item, str) and len(item) > _MAX_STRING_LENGTH:
            # Split at sentence boundaries or at max length
            while len(item) > _MAX_STRING_LENGTH:
                split_at = item.rfind(". ", 0, _MAX_STRING_LENGTH)
                if split_at == -1:
                    split_at = _MAX_STRING_LENGTH
                else:
                    split_at += 1  # include the period
                result.append(item[:split_at])
                item = item[split_at:].lstrip()
            if item:
                result.append(item)
        else:
            result.append(item)
    return result


def _process_dict(obj: dict) -> dict:
    """Recursively enforce string arrays in a dict."""
    result = {}
    for key, value in obj.items():
        if key in _STRING_ARRAY_FIELDS and not isinstance(value, list):
            result[key] = _ensure_string_array(value)
        elif isinstance(value, dict):
            result[key] = _process_dict(value)
        elif isinstance(value, list):
            result[key] = [_process_dict(item) if isinstance(item, dict) else item for item in value]
        else:
            result[key] = value
    return result


def _sort_by_id(items: list[dict]) -> list[dict]:
    """Sort a list of dicts by their 'id' field."""
    return sorted(items, key=lambda x: x.get("id", ""))


def _add_ids(items: list[dict]) -> list[dict]:
    """Add 'id' field to each dict that doesn't already have one."""
    for item in items:
        if isinstance(item, dict) and "id" not in item:
            item["id"] = _gen_id()
    return items


def assemble_space(
    data_sources: dict,
    join_specs: list[dict],
    instructions: list[dict],
    snippets: dict,
    examples: list[dict],
    benchmarks: dict | None = None,
    *,
    sql_functions: list[dict] | None = None,
    metric_views: list[dict] | None = None,
) -> dict:
    """Combine all sections into a valid Genie Space config.

    Generates IDs, derives sample_questions, enforces string arrays,
    sorting, and schema constraints.
    """
    # Process string arrays in all sections
    # NOTE: join_specs are NOT processed — the Genie API expects bare strings
    # for sql, comment, and instruction fields in join_specs (not arrays)
    data_sources = _process_dict(data_sources)
    instructions = [_process_dict(inst) for inst in instructions]
    examples = [_process_dict(ex) for ex in examples]

    snippets = {
        "filters": [_process_dict(f) for f in snippets.get("filters", [])],
        "expressions": [_process_dict(e) for e in snippets.get("expressions", [])],
        "measures": [_process_dict(m) for m in snippets.get("measures", [])],
    }

    # Generate IDs and sort by id (API requirement)
    _add_ids(join_specs)
    _add_ids(instructions)
    _add_ids(examples)
    _add_ids(snippets["filters"])
    _add_ids(snippets["expressions"])
    _add_ids(snippets["measures"])

    if sql_functions:
        sql_functions = [_process_dict(f) for f in sql_functions]
        _add_ids(sql_functions)

    # Derive sample_questions from examples (3-5 questions, no SQL)
    sample_count = min(max(3, len(examples)), 5)
    sample_questions = []
    for ex in examples[:sample_count]:
        q = ex.get("question", "")
        if isinstance(q, str):
            q = [q]
        sample_questions.append({"id": _gen_id(), "question": q})

    # Sort all ID-bearing arrays by id (Genie API requirement)
    join_specs = _sort_by_id(join_specs)
    instructions = _sort_by_id(instructions)
    examples = _sort_by_id(examples)
    sample_questions = _sort_by_id(sample_questions)
    snippets["filters"] = _sort_by_id(snippets["filters"])
    snippets["expressions"] = _sort_by_id(snippets["expressions"])
    snippets["measures"] = _sort_by_id(snippets["measures"])
    if sql_functions:
        sql_functions = _sort_by_id(sql_functions)

    # Constraint: at most 1 text instruction
    if len(instructions) > 1:
        logger.warning("Truncating text_instructions from %d to 1", len(instructions))
        instructions = instructions[:1]

    # Build config
    config: dict = {
        "version": 2,
        "config": {"sample_questions": sample_questions},
        "data_sources": data_sources,
        "instructions": {
            "text_instructions": instructions,
            "example_question_sqls": examples,
            "sql_functions": sql_functions or [],
            "join_specs": join_specs,
            "sql_snippets": snippets,
        },
    }

    # Add metric_views if provided
    if metric_views:
        config["data_sources"]["metric_views"] = metric_views

    # Add benchmarks if provided
    if benchmarks and benchmarks.get("questions"):
        questions = [_process_dict(q) for q in benchmarks["questions"]]
        _add_ids(questions)
        questions = _sort_by_id(questions)
        config["benchmarks"] = {"questions": questions}

    return config
