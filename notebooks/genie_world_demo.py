# Databricks notebook source
# MAGIC %md
# MAGIC # Genie World Demo
# MAGIC
# MAGIC End-to-end demo: **Profile** your data → **Build** a Genie Space → **Deploy** → **Benchmark** accuracy → **Improve** automatically.
# MAGIC
# MAGIC All key operations are wrapped with MLflow spans so you can view the full trace tree in **Machine Learning → Experiments → Traces**.

# COMMAND ----------

# MAGIC %pip install git+https://github.com/SamanthaBrimberry/genie-world.git --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: MLflow Experiment
# MAGIC
# MAGIC All traces will be logged to this experiment. View them at **Machine Learning → Experiments → genie-world-demo → Traces**.

# COMMAND ----------

import mlflow

EXPERIMENT_NAME = "/Shared/genie-world-demo"
mlflow.set_experiment(EXPERIMENT_NAME)
print(f"Traces will appear in: {EXPERIMENT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Set your catalog, schema, and warehouse ID below.

# COMMAND ----------

CATALOG = "sammy_demo_workspace_catalog"
SCHEMA = "album"
WAREHOUSE_ID = "4dcffba4857fc161"
SPACE_NAME = "Album Campaign Analytics — Genie World"
PARENT_PATH = "/Workspace/Users/sammy.brimberry@databricks.com/"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Profile Your Data
# MAGIC
# MAGIC Scans your Unity Catalog tables and builds a rich profile — column types, descriptions, relationships, synonyms. Each tier is opt-in.

# COMMAND ----------

from genie_world.profiler import profile_schema

with mlflow.start_span(name="profile_schema") as span:
    span.set_inputs({"catalog": CATALOG, "schema": SCHEMA})

    profile = profile_schema(
        CATALOG, SCHEMA,
        deep=True,                  # Tier 2: cardinality, null %, min/max via SQL
        synonyms=True,              # LLM-generated business synonyms
        enrich_descriptions=True,   # LLM fills missing descriptions
        warehouse_id=WAREHOUSE_ID,
    )

    span.set_attributes({
        "tables_profiled": len(profile.tables),
        "relationships_found": len(profile.relationships),
        "columns_total": sum(len(t.columns) for t in profile.tables),
        "columns_with_synonyms": sum(1 for t in profile.tables for c in t.columns if c.synonyms),
    })
    span.set_outputs({"tables": [t.table for t in profile.tables]})

print(f"Tables: {len(profile.tables)}")
print(f"Relationships: {len(profile.relationships)}")
print(f"Columns: {sum(len(t.columns) for t in profile.tables)}")
print(f"Synonyms: {sum(1 for t in profile.tables for c in t.columns if c.synonyms)}")

if profile.warnings:
    print(f"\nWarnings: {len(profile.warnings)}")
    for w in profile.warnings:
        print(f"  [{w.tier}] {w.table}: {w.message}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Profiled Tables

# COMMAND ----------

for table in profile.tables:
    print(f"\n{'='*60}")
    print(f"{table.table} — {len(table.columns)} columns")
    if table.description:
        print(f"  {table.description[:100]}")
    for col in table.columns[:8]:
        desc = col.description[:60] if col.description else "(no description)"
        syns = f" | synonyms: {col.synonyms}" if col.synonyms else ""
        print(f"  {col.name}: {col.data_type} — {desc}{syns}")
    if len(table.columns) > 8:
        print(f"  ... and {len(table.columns) - 8} more columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detected Relationships

# COMMAND ----------

for rel in profile.relationships:
    src = rel.source_table.split(".")[-1]
    tgt = rel.target_table.split(".")[-1]
    print(f"  {src}.{rel.source_column} → {tgt}.{rel.target_column}  (confidence={rel.confidence}, method={rel.detection_method.value})")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Build a Genie Space Config
# MAGIC
# MAGIC Generates instructions, example Q&A pairs (with SQL validation), snippets, and benchmarks from the profile. SQL is validated against the warehouse with up to 3 retry fixes.

# COMMAND ----------

from genie_world.builder import build_space, suggest_table_exclusions

# Check if any tables should be excluded
exclusion_suggestions = suggest_table_exclusions(profile)
if exclusion_suggestions:
    print("Table exclusion suggestions:")
    for s in exclusion_suggestions:
        print(f"  {s['table']}: {s['reason']}")
else:
    print("No exclusion suggestions — all tables look queryable.")

# COMMAND ----------

# Choose which tables to include (adjust as needed)
INCLUDE_TABLES = [
    "campaigns", "campaign_groups", "campaign_creatives",
    "campaign_performance_demo", "campaign_performance_geo",
    "campaign_platform_totals",
]

with mlflow.start_span(name="build_space") as span:
    span.set_inputs({"catalog": CATALOG, "schema": SCHEMA, "include_tables": INCLUDE_TABLES})

    result = build_space(
        profile,
        warehouse_id=WAREHOUSE_ID,
        include_tables=INCLUDE_TABLES,
    )

    config = result.config
    instr = config["instructions"]

    span.set_attributes({
        "tables": len(config["data_sources"]["tables"]),
        "examples": len(instr["example_question_sqls"]),
        "benchmarks": len(config.get("benchmarks", {}).get("questions", [])),
        "warnings": len(result.warnings),
    })
    span.set_outputs({"warnings": [w.message for w in result.warnings]})

print(f"Tables: {len(config['data_sources']['tables'])}")
print(f"Example SQLs: {len(instr['example_question_sqls'])}")
print(f"Filters: {len(instr['sql_snippets']['filters'])}")
print(f"Expressions: {len(instr['sql_snippets']['expressions'])}")
print(f"Measures: {len(instr['sql_snippets']['measures'])}")
print(f"Benchmarks: {len(config.get('benchmarks', {}).get('questions', []))}")
print(f"Text instructions: {len(instr['text_instructions'])}")

# Entity matching / format assistance counts
em = sum(1 for t in config["data_sources"]["tables"] for c in t.get("column_configs", []) if c.get("enable_entity_matching"))
fa = sum(1 for t in config["data_sources"]["tables"] for c in t.get("column_configs", []) if c.get("enable_format_assistance"))
syn = sum(1 for t in config["data_sources"]["tables"] for c in t.get("column_configs", []) if c.get("synonyms"))
print(f"Entity matching: {em} columns | Format assistance: {fa} columns | Synonyms: {syn} columns")

if result.warnings:
    print(f"\nWarnings: {len(result.warnings)}")
    for w in result.warnings:
        print(f"  [{w.section}] {w.message}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generated Example SQLs

# COMMAND ----------

for i, ex in enumerate(instr["example_question_sqls"], 1):
    q = ex.get("question", [""])[0] if isinstance(ex.get("question"), list) else ex.get("question", "")
    sql = ex.get("sql", [""])[0] if isinstance(ex.get("sql"), list) else ex.get("sql", "")
    print(f"\n--- Example {i} ---")
    print(f"Q: {q}")
    print(f"SQL: {sql[:120]}{'...' if len(sql) > 120 else ''}")
    if ex.get("usage_guidance"):
        ug = ex["usage_guidance"][0] if isinstance(ex["usage_guidance"], list) else ex["usage_guidance"]
        print(f"Guidance: {ug}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Deploy the Genie Space

# COMMAND ----------

from genie_world.builder import create_space

space = create_space(
    result.config,
    display_name=SPACE_NAME,
    warehouse_id=WAREHOUSE_ID,
    parent_path=PARENT_PATH,
)

print(f"Space ID: {space['space_id']}")
print(f"URL: {space['space_url']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Benchmark Accuracy
# MAGIC
# MAGIC Runs the benchmark questions against the live space, compares Genie's SQL results against expected results using hybrid evaluation (programmatic + LLM fallback).

# COMMAND ----------

from genie_world.benchmarks import run_benchmarks

with mlflow.start_span(name="run_benchmarks") as span:
    span.set_inputs({"space_id": space["space_id"], "warehouse_id": WAREHOUSE_ID})

    results = run_benchmarks(
        space["space_id"],
        WAREHOUSE_ID,
        max_workers=2,
    )

    span.set_attributes({
        "accuracy": results.accuracy,
        "total": results.total,
        "correct": results.correct,
        "incorrect": results.incorrect,
        "no_sql": results.no_sql,
        "uncertain": results.uncertain,
    })
    span.set_outputs({
        "accuracy": f"{results.accuracy:.0%}",
        "results": [
            {"question": q.question[:50], "label": q.label.value, "confidence": q.confidence}
            for q in results.questions
        ],
    })

print(f"Accuracy: {results.accuracy:.0%} ({results.correct}/{results.total})")
print(f"Correct: {results.correct} | Incorrect: {results.incorrect} | No SQL: {results.no_sql} | Uncertain: {results.uncertain}")
print()

for q in results.questions:
    status = "✓" if q.label.value == "correct" else "✗" if q.label.value in ("incorrect", "no_sql") else "?"
    conf = f" (conf={q.confidence:.1f})" if q.confidence < 1.0 else ""
    print(f"  {status} [{q.label.value}]{conf} {q.question[:70]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Diagnose Failures
# MAGIC
# MAGIC Classifies each failure by type — wrong table, missing join, wrong aggregation, etc. Also flags performance issues.

# COMMAND ----------

from genie_world.benchmarks import diagnose_failures

with mlflow.start_span(name="diagnose_failures") as span:
    span.set_inputs({"total_questions": results.total, "failures": results.incorrect + results.no_sql})

    diagnoses = diagnose_failures(results)

    failure_types = {}
    for d in diagnoses:
        ft = d.failure_type.value
        failure_types[ft] = failure_types.get(ft, 0) + 1

    span.set_attributes({
        "diagnosis_count": len(diagnoses),
        "failure_types": str(failure_types),
    })
    span.set_outputs({"diagnoses": [{"question": d.question[:50], "failure_type": d.failure_type.value} for d in diagnoses]})

if diagnoses:
    print(f"Diagnoses: {len(diagnoses)}")
    for d in diagnoses:
        print(f"\n  [{d.failure_type.value}] {d.question[:60]}")
        print(f"    Detail: {d.detail[:120]}")
        print(f"    Fix in: {d.affected_config_section}")
        if d.performance_warning:
            print(f"    Perf: {d.performance_warning}")
else:
    print("No failures to diagnose!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Generate Improvement Suggestions
# MAGIC
# MAGIC Targeted config changes for each failure — add examples, update instructions, create snippets.

# COMMAND ----------

from genie_world.benchmarks import generate_suggestions

with mlflow.start_span(name="generate_suggestions") as span:
    span.set_inputs({"diagnosis_count": len(diagnoses)})

    suggestions = generate_suggestions(diagnoses, results, WAREHOUSE_ID)

    sections = {}
    for s in suggestions:
        sections[s.section] = sections.get(s.section, 0) + 1

    span.set_attributes({
        "suggestion_count": len(suggestions),
        "sections": str(sections),
    })
    span.set_outputs({"suggestions": [{"section": s.section, "action": s.action[:60]} for s in suggestions]})

if suggestions:
    print(f"Suggestions: {len(suggestions)}")
    for s in suggestions:
        print(f"\n  [{s.section}] {s.action}")
        print(f"    Rationale: {s.rationale[:100]}")
        print(f"    Addresses: {s.addresses_questions[0][:60]}")
else:
    print("No suggestions — space is already performing well!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Apply Suggestions and Re-Benchmark
# MAGIC
# MAGIC Updates the live space via PATCH API, then re-runs benchmarks to measure improvement.

# COMMAND ----------

from genie_world.benchmarks import update_space

with mlflow.start_span(name="update_and_rebenchmark") as span:
    span.set_inputs({"space_id": space["space_id"], "suggestion_count": len(suggestions)})

    if suggestions:
        update_result = update_space(space["space_id"], suggestions, WAREHOUSE_ID)
        print(f"Applied {update_result.changes_applied} changes to {space['space_id']}")

        # Re-benchmark
        results_v2 = run_benchmarks(space["space_id"], WAREHOUSE_ID, max_workers=2)

        span.set_attributes({
            "changes_applied": update_result.changes_applied,
            "accuracy_before": results.accuracy,
            "accuracy_after": results_v2.accuracy,
            "accuracy_delta": results_v2.accuracy - results.accuracy,
        })
        span.set_outputs({
            "accuracy_before": f"{results.accuracy:.0%}",
            "accuracy_after": f"{results_v2.accuracy:.0%}",
        })

        print(f"\nAccuracy: {results.accuracy:.0%} → {results_v2.accuracy:.0%}")
        print(f"Correct: {results_v2.correct}/{results_v2.total}")

        for q in results_v2.questions:
            status = "✓" if q.label.value == "correct" else "✗"
            print(f"  {status} [{q.label.value}] {q.question[:70]}")
    else:
        span.set_attributes({"skipped": True, "reason": "no_suggestions"})
        print("Nothing to apply — skipping re-benchmark.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Alternative: Auto-Tune (One Call)
# MAGIC
# MAGIC Wraps the full benchmark → diagnose → suggest → update loop with a target accuracy and max iterations.

# COMMAND ----------

# from genie_world.benchmarks import tune_space
#
# with mlflow.start_span(name="tune_space") as span:
#     span.set_inputs({
#         "space_id": space["space_id"],
#         "target_accuracy": 0.9,
#         "max_iterations": 3,
#     })
#
#     tune_result = tune_space(
#         space["space_id"],
#         WAREHOUSE_ID,
#         target_accuracy=0.9,
#         max_iterations=3,
#         auto_approve=True,
#     )
#
#     span.set_attributes({
#         "final_accuracy": tune_result.final_accuracy,
#         "iterations": len(tune_result.iterations),
#         "target_reached": tune_result.target_reached,
#         "suggestions_applied": len(tune_result.suggestions_applied),
#     })
#     span.set_outputs({
#         "final_accuracy": f"{tune_result.final_accuracy:.0%}",
#         "target_reached": tune_result.target_reached,
#     })
#
# print(f"Final accuracy: {tune_result.final_accuracy:.0%}")
# print(f"Iterations: {len(tune_result.iterations)}")
# print(f"Target reached: {tune_result.target_reached}")
# print(f"Suggestions applied: {len(tune_result.suggestions_applied)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## View Your Traces
# MAGIC
# MAGIC 1. Go to **Machine Learning → Experiments**
# MAGIC 2. Open the **genie-world-demo** experiment
# MAGIC 3. Click the **Traces** tab
# MAGIC 4. Click any trace to see the full span tree. Example for `run_benchmarks`:
# MAGIC
# MAGIC ```
# MAGIC run_benchmarks
# MAGIC └── genie_ask (per question)
# MAGIC     ├── state_FETCHING_METADATA   → full API response
# MAGIC     ├── state_FILTERING_CONTEXT   → full API response
# MAGIC     ├── state_ASKING_AI           → generated_sql + full response
# MAGIC     ├── state_EXECUTING_QUERY     → generated_sql + full response
# MAGIC     └── state_COMPLETED           → generated_sql, row_count, sample rows
# MAGIC ```
# MAGIC
# MAGIC ### Span tree per step:
# MAGIC | Outer Span | Child Spans | Key Attributes |
# MAGIC |------------|-------------|---------------|
# MAGIC | `profile_schema` | `profile_schema_metadata`, `generate_synonyms_for_table`, `enrich_descriptions_for_table` | tables_profiled, relationships_found, columns_total |
# MAGIC | `build_space` | `generate_snippets`, `generate_example_sqls`, `generate_instructions`, `validate_and_fix_sql` | tables, examples, benchmarks, warnings |
# MAGIC | `run_benchmarks` | `genie_ask` → `state_*` per question | accuracy, correct, incorrect, no_sql, uncertain |
# MAGIC | `diagnose_failures` | per-failure diagnosis | diagnosis_count, failure_types |
# MAGIC | `generate_suggestions` | per-suggestion generation | suggestion_count, sections |
# MAGIC | `update_and_rebenchmark` | `genie_ask` → `state_*` (re-benchmark) | accuracy_before, accuracy_after, accuracy_delta |
# MAGIC
# MAGIC Each `genie_ask` span contains `state_*` child spans showing every Genie state transition with the full API response — click any `state_*` span → **Outputs** tab → `full_response` to inspect.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC Delete the test space when done.

# COMMAND ----------

# from databricks.sdk import WorkspaceClient
# w = WorkspaceClient()
# w.api_client.do("DELETE", f"/api/2.0/genie/spaces/{space['space_id']}")
# print(f"Deleted space {space['space_id']}")
