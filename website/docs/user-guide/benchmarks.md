---
sidebar_position: 3
title: Benchmarks
---

# Benchmarks

The Benchmarks block runs questions against a live Genie Space, evaluates accuracy, diagnoses failures, and generates targeted improvements.

## Pipeline

```
run → evaluate → diagnose → suggest → update
```

1. **Runner** — sends questions to the Genie API via `GenieClient`
2. **Evaluator** — hybrid comparison (programmatic matching + LLM fallback for ambiguous cases)
3. **Diagnoser** — classifies failure types (wrong table, missing join, wrong aggregation, etc.)
4. **Suggester** — generates config change suggestions from diagnoses
5. **Updater** — fetches current config, merges suggestions, applies via PATCH API

## Basic Usage

```python
from genie_world.benchmarks import run_benchmarks, diagnose_failures, generate_suggestions, update_space

# Run benchmarks
results = run_benchmarks(space_id, warehouse_id)
print(f"Accuracy: {results.accuracy:.0%}")

# Diagnose and fix
diagnoses = diagnose_failures(results)
suggestions = generate_suggestions(diagnoses, results, warehouse_id)
update_space(space_id, suggestions, warehouse_id)
```

## Auto-Tune

The `tune_space()` function wraps the full pipeline in an iterative loop:

```python
from genie_world.benchmarks import tune_space

result = tune_space(
    space_id,
    warehouse_id,
    target_accuracy=0.9,
    max_iterations=5,
    auto_approve=True,
)

print(f"Final accuracy: {result.final_accuracy:.0%}")
print(f"Iterations: {len(result.iterations)}")
```

Each iteration runs the full benchmark → diagnose → suggest → update cycle until the target accuracy is reached or max iterations are exhausted.

## Evaluation Labels

The evaluator assigns one of three labels to each question:

| Label | Meaning |
|-------|---------|
| `correct` | Genie's answer matches the expected result |
| `incorrect` | Genie's answer is wrong |
| `partial` | Genie's answer is partially correct (e.g., right table but wrong aggregation) |

## Failure Types

The diagnoser classifies failures into categories:

- `wrong_table` — Genie queried the wrong table
- `missing_join` — A required join was missing
- `wrong_aggregation` — Incorrect GROUP BY or aggregate function
- `wrong_filter` — Incorrect WHERE clause
- `missing_column` — A needed column wasn't available
- `syntax_error` — Generated SQL had syntax issues
