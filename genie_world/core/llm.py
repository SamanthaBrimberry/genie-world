"""LLM serving endpoint wrapper using Databricks SDK with JSON repair.

Uses the WorkspaceClient's raw API for FMAPI access, which accepts plain
dict messages and works across all SDK versions and auth modes (OBO, PAT, CLI).
"""

from __future__ import annotations

import json
import logging
import re
import time

from genie_world.core.auth import get_workspace_client
from genie_world.core.config import get_config

logger = logging.getLogger(__name__)


def call_llm(
    messages: list[dict],
    model: str | None = None,
    max_tokens: int | None = None,
) -> str:
    """Call an LLM via Databricks Foundation Model API.

    Uses the WorkspaceClient's raw API to POST to the serving endpoint.
    This avoids SDK serialization issues with plain dict messages.

    Args:
        messages: Chat messages in OpenAI format (list of dicts).
        model: Serving endpoint name. Defaults to config's llm_model.
        max_tokens: Optional max tokens for response.

    Returns:
        The assistant's response content.
    """
    if model is None:
        model = get_config().llm_model

    client = get_workspace_client()

    body: dict = {"messages": messages}
    if max_tokens is not None:
        body["max_tokens"] = max_tokens

    t0 = time.monotonic()
    response = client.api_client.do(
        "POST",
        f"/serving-endpoints/{model}/invocations",
        body=body,
    )
    elapsed = time.monotonic() - t0
    logger.info(f"LLM responded in {elapsed:.1f}s")

    if not isinstance(response, dict):
        raise ValueError(f"Unexpected response type: {type(response)}")

    choices = response.get("choices", [])
    if not choices:
        raise ValueError("LLM returned no choices")

    content = choices[0].get("message", {}).get("content", "")
    if not content:
        raise ValueError("LLM returned empty content")

    return content


def _repair_json(content: str) -> str:
    """Attempt to repair common JSON syntax errors from LLM responses."""
    # Remove trailing commas before closing brackets/braces
    content = re.sub(r",\s*([}\]])", r"\1", content)
    # Fix missing commas between closing and opening braces/brackets
    content = re.sub(r"([}\]])\s*\n?\s*([{\[])", r"\1,\n\2", content)
    # Fix missing commas between string values
    content = re.sub(r'(")\s*\n\s*(")', r'\1,\n\2', content)
    content = re.sub(r'(")\s+(")', r'\1, \2', content)
    # Fix missing commas after closing brace/bracket before string
    content = re.sub(r'([}\]])\s*\n\s*(")', r'\1,\n\2', content)
    content = re.sub(r'([}\]])\s+(")', r'\1, \2', content)
    return content


def parse_json_from_llm_response(content: str) -> dict:
    """Parse JSON from an LLM response, handling markdown code blocks and repairs."""
    content = content.strip()

    if not content:
        raise ValueError("LLM returned empty response")

    # Handle markdown code blocks
    if content.startswith("```"):
        lines = content.split("\n")
        start_idx = 1
        end_idx = len(lines)
        for i in range(len(lines) - 1, 0, -1):
            if lines[i].strip() == "```":
                end_idx = i
                break
        content = "\n".join(lines[start_idx:end_idx])

    # Handle text before JSON
    if not content.startswith("{"):
        json_start = content.find("{")
        if json_start != -1:
            brace_count = 0
            json_end = -1
            for i, char in enumerate(content[json_start:], json_start):
                if char == "{":
                    brace_count += 1
                elif char == "}":
                    brace_count -= 1
                    if brace_count == 0:
                        json_end = i + 1
                        break
            if json_end != -1:
                content = content[json_start:json_end]

    if not content:
        raise ValueError("No JSON found in LLM response")

    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        repaired = _repair_json(content)
        try:
            return json.loads(repaired)
        except json.JSONDecodeError:
            raise e
