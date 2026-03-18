"""Benchmarks block for genie-world.

Public API for running benchmarks, diagnosing failures, generating suggestions,
updating spaces, and iterative auto-tuning.
"""

from __future__ import annotations

import logging
from datetime import datetime

from genie_world.benchmarks import evaluator, runner
from genie_world.benchmarks.diagnoser import diagnose_failures
from genie_world.benchmarks.models import (
    BenchmarkLabel,
    BenchmarkResult,
    Diagnosis,
    FailureType,
    QuestionInput,
    QuestionResult,
    QuestionSource,
    Suggestion,
    TuneResult,
    UpdateResult,
)
from genie_world.benchmarks.suggester import generate_suggestions
from genie_world.benchmarks.updater import update_space
from genie_world.core.genie_client import GenieClient

logger = logging.getLogger(__name__)

__all__ = [
    # Functions
    "run_benchmarks",
    "diagnose_failures",
    "generate_suggestions",
    "update_space",
    "tune_space",
    # Result models
    "BenchmarkResult",
    "QuestionResult",
    "Diagnosis",
    "Suggestion",
    "UpdateResult",
    "TuneResult",
    # Enums
    "BenchmarkLabel",
    "FailureType",
    "QuestionSource",
]


def run_benchmarks(
    space_id: str,
    warehouse_id: str,
    *,
    custom_questions: list[dict] | None = None,
    max_workers: int = 4,
) -> BenchmarkResult:
    """Run benchmarks against a Genie space and return evaluation results.

    Orchestration:
    1. Create GenieClient(space_id)
    2. Fetch space config via client.get_config()
    3. Extract benchmark questions from config via runner.extract_questions_from_config()
    4. Convert custom_questions dicts to QuestionInput(source=CUSTOM)
    5. Merge both lists
    6. Raise ValueError("No questions to benchmark") if empty
    7. Run questions via runner.run_questions()
    8. Evaluate each via evaluator.evaluate_question()
    9. Compute accuracy = correct / (correct + incorrect + no_sql), excluding
       expected_sql_errors and uncertain
    10. Return BenchmarkResult with space_config stored

    Args:
        space_id: Genie space ID.
        warehouse_id: Databricks warehouse ID for executing expected SQL.
        custom_questions: Optional list of dicts with 'question' and 'expected_sql' keys.
        max_workers: Maximum parallel threads for running questions.

    Returns:
        BenchmarkResult with all question results, accuracy, and space_config.

    Raises:
        ValueError: If there are no questions to benchmark.
    """
    # Step 1-2: Fetch space config
    client = GenieClient(space_id)
    space_config = client.get_config()

    # Step 3: Extract questions from config
    config_questions = runner.extract_questions_from_config(space_config)

    # Step 4: Convert custom questions
    custom_inputs: list[QuestionInput] = []
    for cq in (custom_questions or []):
        custom_inputs.append(
            QuestionInput(
                question=cq["question"],
                expected_sql=cq["expected_sql"],
                source=QuestionSource.CUSTOM,
            )
        )

    # Step 5: Merge
    all_questions = config_questions + custom_inputs

    # Step 6: Check for empty
    if not all_questions:
        raise ValueError("No questions to benchmark")

    # Step 7: Run questions in parallel
    genie_responses = runner.run_questions(space_id, all_questions, max_workers=max_workers)

    # Step 8: Evaluate each question
    question_results: list[QuestionResult] = []
    correct = incorrect = no_sql = uncertain = expected_sql_errors = 0

    for qi, genie_response in zip(all_questions, genie_responses):
        eval_result = evaluator.evaluate_question(
            question=qi.question,
            expected_sql=qi.expected_sql,
            genie_response=genie_response,
            warehouse_id=warehouse_id,
        )

        qr = QuestionResult(
            question=qi.question,
            expected_sql=qi.expected_sql,
            source=qi.source,
            label=eval_result.label,
            confidence=eval_result.confidence,
            expected_result=eval_result.expected_result,
            genie_response=genie_response,
            expected_metrics=eval_result.expected_metrics,
            genie_metrics=eval_result.genie_metrics,
            comparison_detail=eval_result.comparison_detail,
        )
        question_results.append(qr)

        # Tally labels
        if eval_result.label == BenchmarkLabel.CORRECT:
            correct += 1
        elif eval_result.label == BenchmarkLabel.INCORRECT:
            incorrect += 1
        elif eval_result.label == BenchmarkLabel.NO_SQL:
            no_sql += 1
        elif eval_result.label == BenchmarkLabel.UNCERTAIN:
            uncertain += 1
        elif eval_result.label == BenchmarkLabel.EXPECTED_SQL_ERROR:
            expected_sql_errors += 1

    # Step 9: Compute accuracy (exclude expected_sql_errors and uncertain)
    denominator = correct + incorrect + no_sql
    accuracy = correct / denominator if denominator > 0 else 0.0

    total = correct + incorrect + no_sql + uncertain + expected_sql_errors

    return BenchmarkResult(
        space_id=space_id,
        questions=question_results,
        accuracy=accuracy,
        total=total,
        correct=correct,
        incorrect=incorrect,
        no_sql=no_sql,
        uncertain=uncertain,
        expected_sql_errors=expected_sql_errors,
        warnings=[],
        space_config=space_config,
        run_at=datetime.utcnow(),
    )


def tune_space(
    space_id: str,
    warehouse_id: str,
    *,
    custom_questions: list[dict] | None = None,
    target_accuracy: float = 0.9,
    max_iterations: int = 3,
    max_workers: int = 4,
    auto_approve: bool = False,
) -> TuneResult:
    """Iterative benchmark → diagnose → suggest → update loop.

    When auto_approve=False:
        Runs ONE iteration (run_benchmarks → diagnose → suggest) and returns
        TuneResult with pending suggestions (not applied). The caller reviews
        suggestions and calls update_space() manually before re-running.

    When auto_approve=True:
        Loops up to max_iterations:
        1. run_benchmarks
        2. If accuracy >= target → stop (target reached)
        3. diagnose_failures
        4. generate_suggestions
        5. If no suggestions → stop
        6. update_space with suggestions
        7. Repeat

    Args:
        space_id: Genie space ID.
        warehouse_id: Databricks warehouse ID.
        custom_questions: Optional additional questions.
        target_accuracy: Accuracy threshold to stop iteration (default 0.9).
        max_iterations: Maximum number of improvement iterations.
        max_workers: Parallel threads for running questions.
        auto_approve: If True, apply suggestions automatically each iteration.

    Returns:
        TuneResult with all iterations, applied suggestions, final accuracy,
        and whether target was reached.
    """
    iterations: list[BenchmarkResult] = []
    suggestions_applied: list[Suggestion] = []
    target_reached = False

    if not auto_approve:
        # Run ONE iteration and return with pending suggestions (not applied)
        result = run_benchmarks(
            space_id,
            warehouse_id,
            custom_questions=custom_questions,
            max_workers=max_workers,
        )
        iterations.append(result)

        diagnoses = diagnose_failures(result)
        _pending = generate_suggestions(diagnoses, result, warehouse_id)  # noqa: F841

        target_reached = result.accuracy >= target_accuracy
        return TuneResult(
            iterations=iterations,
            suggestions_applied=[],  # nothing applied in manual mode
            final_accuracy=result.accuracy,
            target_reached=target_reached,
        )

    # auto_approve=True: full iterative loop
    for _iteration in range(max_iterations):
        result = run_benchmarks(
            space_id,
            warehouse_id,
            custom_questions=custom_questions,
            max_workers=max_workers,
        )
        iterations.append(result)

        if result.accuracy >= target_accuracy:
            target_reached = True
            break

        diagnoses = diagnose_failures(result)
        new_suggestions = generate_suggestions(diagnoses, result, warehouse_id)

        if not new_suggestions:
            # Nothing to improve; stop early
            break

        update_space(space_id, new_suggestions, warehouse_id)
        suggestions_applied.extend(new_suggestions)

    final_accuracy = iterations[-1].accuracy if iterations else 0.0

    return TuneResult(
        iterations=iterations,
        suggestions_applied=suggestions_applied,
        final_accuracy=final_accuracy,
        target_reached=target_reached,
    )
