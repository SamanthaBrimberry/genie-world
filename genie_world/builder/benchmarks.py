"""LLM generator: benchmark questions with SQL answers."""

from __future__ import annotations

import json
import logging

from genie_world.builder.sql_validator import validate_and_fix_sql
from genie_world.core.llm import call_llm, parse_json_from_llm_response
from genie_world.core.tracing import trace
from genie_world.profiler.models import SchemaProfile

logger = logging.getLogger(__name__)


def _parse_benchmarks_response(raw: str) -> list[dict]:
    """Parse LLM response that may be a JSON array or object."""
    content = raw.strip()
    # Strip markdown code fences if present
    if content.startswith("```"):
        lines = content.split("\n")
        end = len(lines)
        for i in range(len(lines) - 1, 0, -1):
            if lines[i].strip() == "```":
                end = i
                break
        content = "\n".join(lines[1:end])

    stripped = content.strip()
    if stripped.startswith("["):
        return json.loads(stripped)

    result = parse_json_from_llm_response(content)
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        return result.get("questions", result.get("benchmarks", [result]))
    return []


def _build_benchmarks_prompt(
    profile: SchemaProfile,
    join_specs: list[dict],
    snippets: dict,
    existing_examples: list[dict],
    count: int,
) -> list[dict]:
    """Build prompt for generating benchmark questions."""
    tables_info = "\n".join(
        f"  {t.catalog}.{t.schema_name}.{t.table}: "
        + ", ".join(f"{c.name} ({c.data_type})" for c in t.columns)
        for t in profile.tables
    )

    existing_qs = "\n".join(
        f"  - {ex.get('question', '')}"
        for ex in existing_examples
    ) if existing_examples else "  (none)"

    system_msg = (
        "You are a Databricks SQL expert generating benchmark questions for testing a Genie Space. "
        "Return ONLY a valid JSON array — no prose, no markdown."
    )

    user_msg = (
        f"Tables:\n{tables_info}\n\n"
        f"Existing example questions (DO NOT duplicate these):\n{existing_qs}\n\n"
        f"Generate {count} NEW benchmark questions that are DIFFERENT from the examples above.\n"
        "Include varied phrasings, edge cases, and ambiguous queries to test robustness.\n"
        "Use fully-qualified table names in SQL.\n\n"
        'Return: [{"question": "...", "sql": "SELECT ..."}]'
    )

    return [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": user_msg},
    ]


@trace(name="generate_benchmarks", span_type="CHAIN")
def generate_benchmarks(
    profile: SchemaProfile,
    join_specs: list[dict],
    snippets: dict,
    existing_examples: list[dict],
    warehouse_id: str | None = None,
    count: int = 10,
) -> tuple[dict, list[str]]:
    """Generate benchmark questions with SQL answers.

    Returns ({"questions": [...]}, warnings).
    """
    warnings: list[str] = []

    prompt = _build_benchmarks_prompt(profile, join_specs, snippets, existing_examples, count)

    try:
        raw = call_llm(prompt)
        items = _parse_benchmarks_response(raw)
        if not isinstance(items, list):
            items = [items]
    except Exception as exc:
        logger.warning("Benchmark generation failed: %s", exc)
        return {"questions": []}, [f"Benchmark generation failed: {exc}"]

    # Validate SQL if warehouse available
    if warehouse_id:
        for item in items:
            sql = item.get("sql", "")
            question = item.get("question", "")
            fixed_sql, sql_warnings = validate_and_fix_sql(
                sql, question, profile, warehouse_id
            )
            item["sql"] = fixed_sql
            warnings.extend(sql_warnings)

    # Convert to benchmark schema format
    questions = []
    for item in items:
        questions.append({
            "question": item.get("question", ""),
            "answer": [{"format": "SQL", "content": [item.get("sql", "")]}],
        })

    return {"questions": questions}, warnings
