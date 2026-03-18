"""Benchmarks runner: extract questions from config and run them against Genie."""

from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from genie_world.benchmarks.models import QuestionInput, QuestionSource
from genie_world.core.genie_client import GenieClient, GenieResponse

logger = logging.getLogger(__name__)


def extract_questions_from_config(config: dict) -> list[QuestionInput]:
    """Extract QuestionInput instances from a Genie space config.

    Parses the nested benchmarks.questions format:
      - question = " ".join(q["question"])
      - expected_sql = " ".join(q["answer"][0]["content"])

    Parameterized questions (those containing :param syntax) are:
      - Substituted with default_value if provided
      - Skipped with a warning if no default is available
    """
    benchmarks = config.get("benchmarks", {})
    raw_questions = benchmarks.get("questions", [])
    results: list[QuestionInput] = []

    for q in raw_questions:
        question_parts = q.get("question", [])
        answer_list = q.get("answer", [])
        parameters = q.get("parameters", [])

        if not question_parts or not answer_list:
            logger.warning("Skipping question with missing question or answer: %s", q)
            continue

        question_text = " ".join(question_parts)
        sql_text = " ".join(answer_list[0].get("content", []))

        # Handle parameterized questions
        if parameters:
            defaults: dict[str, str] = {}
            missing: list[str] = []

            for param in parameters:
                name = param.get("name", "")
                default = param.get("default_value")
                if default is not None:
                    defaults[name] = str(default)
                else:
                    missing.append(name)

            if missing:
                logger.warning(
                    "Skipping parameterized question %r — no defaults for: %s",
                    question_text,
                    missing,
                )
                continue

            # Substitute all :param with defaults
            for name, value in defaults.items():
                pattern = rf":{re.escape(name)}\b"
                question_text = re.sub(pattern, value, question_text)
                sql_text = re.sub(pattern, f"'{value}'", sql_text)

        results.append(
            QuestionInput(
                question=question_text,
                expected_sql=sql_text,
                source=QuestionSource.SPACE_CONFIG,
            )
        )

    return results


def run_questions(
    space_id: str,
    questions: list[QuestionInput],
    max_workers: int = 4,
) -> list[GenieResponse]:
    """Run a list of questions against a Genie space in parallel.

    Uses a ThreadPoolExecutor to ask all questions concurrently, preserving
    the original question order in the returned responses.

    Args:
        space_id: The Genie space ID.
        questions: List of QuestionInput to ask.
        max_workers: Maximum number of parallel threads.

    Returns:
        List of GenieResponse, one per question (same order as input).
    """
    if not questions:
        return []

    client = GenieClient(space_id)
    responses: list[GenieResponse | None] = [None] * len(questions)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_index = {
            executor.submit(client.ask, q.question): i
            for i, q in enumerate(questions)
        }

        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            try:
                responses[idx] = future.result()
            except Exception as e:
                logger.error("Error asking question %r: %s", questions[idx].question, e)
                responses[idx] = GenieResponse(
                    question=questions[idx].question,
                    status="FAILED",
                    error=str(e),
                    duration_seconds=0.0,
                )

    return [r for r in responses if r is not None]
