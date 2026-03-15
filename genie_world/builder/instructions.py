"""LLM generator: text instructions (generated last to avoid conflicts)."""

from __future__ import annotations

import logging

from genie_world.core.llm import call_llm, parse_json_from_llm_response
from genie_world.core.tracing import trace
from genie_world.profiler.models import SchemaProfile

logger = logging.getLogger(__name__)


def _build_instructions_prompt(
    profile: SchemaProfile,
    join_specs: list[dict],
    snippets: dict,
    examples: list[dict],
) -> list[dict]:
    """Build prompt for generating text instructions."""
    tables_summary = ", ".join(t.table for t in profile.tables)

    joins_summary = "\n".join(
        f"  - {j['left']['alias']} <-> {j['right']['alias']}: {j['sql'][0]}"
        for j in join_specs
    ) if join_specs else "  (none)"

    examples_summary = "\n".join(
        f"  - Q: {ex.get('question', '')}"
        for ex in examples[:5]
    ) if examples else "  (none)"

    system_msg = (
        "You are a Databricks Genie Space configuration expert. Generate a single, focused "
        "text instruction that helps Genie answer questions accurately. "
        "Return ONLY valid JSON — no prose, no markdown."
    )

    user_msg = (
        f"Tables: {tables_summary}\n\n"
        f"Configured joins:\n{joins_summary}\n\n"
        f"Example questions already configured:\n{examples_summary}\n\n"
        "Generate ONE text instruction covering:\n"
        "- How to interpret key business terms\n"
        "- Default time period handling (e.g., 'last month' = previous calendar month)\n"
        "- Rounding/formatting conventions\n"
        "- When to ask for clarification\n\n"
        "IMPORTANT: Do NOT repeat or contradict the SQL examples above.\n"
        "Keep it concise and globally applicable.\n\n"
        'Return: {"content": ["instruction line 1", "instruction line 2", ...]}'
    )

    return [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": user_msg},
    ]


@trace(name="generate_instructions", span_type="CHAIN")
def generate_instructions(
    profile: SchemaProfile,
    join_specs: list[dict],
    snippets: dict,
    examples: list[dict],
) -> list[dict]:
    """Generate text instructions that complement existing examples and snippets.

    Returns a list with at most one instruction dict (no ID — assembler assigns it).
    On error, returns empty list.
    """
    prompt = _build_instructions_prompt(profile, join_specs, snippets, examples)

    try:
        raw = call_llm(prompt)
        result = parse_json_from_llm_response(raw)
    except Exception as exc:
        logger.warning("Instruction generation failed: %s", exc)
        return []

    content = result.get("content", [])
    if isinstance(content, str):
        content = [content]
    if not isinstance(content, list) or not content:
        return []

    return [{"content": content}]
