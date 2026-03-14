# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Full Visibility Demo (REST API)
# MAGIC
# MAGIC ## Why REST API?
# MAGIC
# MAGIC The `databricks_ai_bridge` SDK creates trace spans (`asking_ai`, `fetching_metadata`, etc.) but **hides the contents**.
# MAGIC
# MAGIC | Visibility | SDK | REST API |
# MAGIC |------------|-----|----------|
# MAGIC | State names | ✅ | ✅ |
# MAGIC | Full API response | ❌ | ✅ |
# MAGIC | Generated SQL per step | ❌ | ✅ |
# MAGIC | What changed between steps | ❌ | ✅ |
# MAGIC | Error details | Minimal | ✅ Full |
# MAGIC
# MAGIC **This notebook uses REST API for complete observability.**

# COMMAND ----------

# MAGIC %pip install mlflow requests -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os, time, json, requests, mlflow

# === CONFIGURATION ===
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "<YOUR_SPACE_ID>")
EXPERIMENT = "/Shared/genie-visibility-demo"

# Auto-detect Databricks environment
try:
    HOST = spark.conf.get("spark.databricks.workspaceUrl")
    TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
except:
    HOST = os.getenv("DATABRICKS_HOST", "")
    TOKEN = os.getenv("DATABRICKS_TOKEN", "")

mlflow.set_experiment(EXPERIMENT)
print(f"Space: {GENIE_SPACE_ID} | Host: {HOST}")

# COMMAND ----------

def ask_genie_with_full_trace(question: str, space_id: str, host: str, token: str):
    """
    Ask Genie using REST API with full MLflow tracing.
    Every state change logs the complete API response.
    """
    base_url = f"https://{host.replace('https://', '')}/api/2.0/genie/spaces/{space_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    start = time.time()
    result = {"question": question, "sql": None, "rows": 0, "error": None}

    with mlflow.start_span(name="genie_query") as root:
        root.set_inputs({"question": question, "space_id": space_id})

        # === 1. START CONVERSATION ===
        with mlflow.start_span(name="1_start_conversation") as span:
            span.set_inputs({"question": question})

            resp = requests.post(f"{base_url}/start-conversation", headers=headers, json={"content": question})

            if resp.status_code != 200:
                span.set_attributes({"error": resp.text[:500]})
                result["error"] = resp.text
                return result

            data = resp.json()
            conv_id = data.get("conversation_id") or data.get("conversation", {}).get("id")
            msg_id = data.get("message_id") or data.get("message", {}).get("id")

            span.set_outputs({"conversation_id": conv_id, "message_id": msg_id, "response": data})

        # === 2. POLL UNTIL COMPLETE ===
        with mlflow.start_span(name="2_poll_states") as poll_span:
            last_status = None
            message = None

            while (time.time() - start) < 300:  # 5 min timeout
                resp = requests.get(f"{base_url}/conversations/{conv_id}/messages/{msg_id}", headers=headers)
                message = resp.json()
                status = message.get("status", "UNKNOWN")

                # Log each state change with FULL details
                if status != last_status:
                    with mlflow.start_span(name=f"state_{status}") as state_span:
                        elapsed = round(time.time() - start, 2)

                        # Extract SQL if present
                        sql = None
                        for att in (message.get("attachments") or []):
                            if att.get("query"):
                                sql = att["query"].get("query")

                        # === KEY: Log everything ===
                        state_span.set_inputs({"elapsed_sec": elapsed})
                        state_span.set_attributes({
                            "status": status,
                            "has_sql": sql is not None,
                            "attachment_count": len(message.get("attachments") or []),
                        })
                        if sql:
                            state_span.set_attributes({"generated_sql": sql})
                        if message.get("error"):
                            state_span.set_attributes({"error": json.dumps(message["error"])})

                        # Full response in outputs
                        state_span.set_outputs({
                            "status": status,
                            "generated_sql": sql,
                            "full_response": message,  # <-- THE FULL API RESPONSE
                        })

                    print(f"  [{elapsed:5.1f}s] {status}" + (f" | SQL: {sql[:50]}..." if sql else ""))
                    last_status = status

                if status in ["COMPLETED", "FAILED", "CANCELLED"]:
                    break
                time.sleep(1.5)

            poll_span.set_outputs({"final_status": status, "total_time": round(time.time() - start, 2)})

        # === 3. EXTRACT RESULTS ===
        if message and message.get("status") == "COMPLETED":
            with mlflow.start_span(name="3_extract_results") as extract_span:
                for att in (message.get("attachments") or []):
                    if att.get("query"):
                        result["sql"] = att["query"].get("query")
                        result["description"] = att["query"].get("description")
                        att_id = att.get("id")

                        extract_span.set_outputs({
                            "generated_sql": result["sql"],
                            "description": result["description"],
                        })

                        # Fetch query results
                        if att_id:
                            with mlflow.start_span(name="4_fetch_data") as data_span:
                                resp = requests.get(
                                    f"{base_url}/conversations/{conv_id}/messages/{msg_id}/query-result/{att_id}",
                                    headers=headers
                                )
                                if resp.status_code == 200:
                                    stmt = resp.json().get("statement_response", {})
                                    cols = [c["name"] for c in stmt.get("manifest", {}).get("schema", {}).get("columns", [])]
                                    rows = stmt.get("result", {}).get("data_array", [])
                                    result["columns"] = cols
                                    result["rows"] = len(rows)
                                    result["sample"] = [dict(zip(cols, r)) for r in rows[:3]]

                                    data_span.set_outputs({
                                        "columns": cols,
                                        "row_count": len(rows),
                                        "sample": result["sample"],
                                    })

        # === FINAL SUMMARY ===
        result["duration"] = round(time.time() - start, 2)
        root.set_outputs({
            "success": result["sql"] is not None,
            "generated_sql": result["sql"],
            "row_count": result["rows"],
            "duration_sec": result["duration"],
        })

    return result

# COMMAND ----------

# === RUN DEMO ===
with mlflow.start_run(run_name="genie_full_visibility"):

    question = "What were the top 10 offers by redemption rate last month?"
    print(f"Question: {question}\n")

    result = ask_genie_with_full_trace(question, GENIE_SPACE_ID, HOST, TOKEN)

    print(f"\n{'='*60}")
    print(f"Duration: {result['duration']}s")
    print(f"\nGenerated SQL:\n{'-'*40}\n{result.get('sql', 'N/A')}\n{'-'*40}")
    print(f"\nResults: {result['rows']} rows")
    if result.get("sample"):
        for i, row in enumerate(result["sample"]):
            print(f"  {i+1}. {row}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What You'll See in MLflow Traces
# MAGIC
# MAGIC ```
# MAGIC genie_query
# MAGIC ├── 1_start_conversation
# MAGIC │     └── Outputs: conversation_id, message_id, full response
# MAGIC │
# MAGIC ├── 2_poll_states
# MAGIC │     ├── state_FETCHING_METADATA   → Outputs: full_response
# MAGIC │     ├── state_FILTERING_CONTEXT   → Outputs: full_response
# MAGIC │     ├── state_ASKING_AI           → Outputs: generated_sql, full_response
# MAGIC │     ├── state_EXECUTING_QUERY     → Outputs: generated_sql, full_response
# MAGIC │     └── state_COMPLETED           → Outputs: generated_sql, full_response
# MAGIC │
# MAGIC ├── 3_extract_results
# MAGIC │     └── Outputs: generated_sql, description
# MAGIC │
# MAGIC └── 4_fetch_data
# MAGIC       └── Outputs: columns, row_count, sample rows
# MAGIC ```
# MAGIC
# MAGIC **Click any `state_*` span → Outputs tab → `full_response` to see the complete API payload.**
