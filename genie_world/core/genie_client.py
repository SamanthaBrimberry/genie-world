"""Genie Conversation API wrapper with full state-transition visibility.

Ports the polling pattern from genie_tracing_demo.py and attachment
parsing from databricks-ai-bridge.
"""

from __future__ import annotations

import json
import logging
import time

from pydantic import BaseModel

from genie_world.core.auth import get_workspace_client
from genie_world.core.tracing import trace

logger = logging.getLogger(__name__)

_TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"}
_POLL_INTERVAL = 1.5  # seconds between polls


class GenieResponse(BaseModel):
    """Full response from a Genie conversation question."""
    question: str
    status: str = "UNKNOWN"
    generated_sql: str | None = None
    description: str | None = None
    result: dict | None = None
    duration_seconds: float = 0.0
    states: list[str] = []
    error: str | None = None
    conversation_id: str | None = None


class GenieClient:
    """Wraps the Genie Conversation API."""

    def __init__(self, space_id: str):
        self.space_id = space_id

    @trace(name="genie_ask", span_type="CHAIN")
    def ask(self, question: str, timeout: int = 300) -> GenieResponse:
        """Send a question, poll until complete, return full response."""
        client = get_workspace_client()
        base = f"/api/2.0/genie/spaces/{self.space_id}"
        start = time.time()
        states: list[str] = []

        # 1. Start conversation
        try:
            resp = client.api_client.do("POST", f"{base}/start-conversation", body={"content": question})
        except Exception as e:
            return GenieResponse(
                question=question, status="FAILED",
                error=str(e), duration_seconds=time.time() - start,
            )

        conv_id = resp.get("conversation_id") or resp.get("conversation", {}).get("id")
        msg_id = resp.get("message_id") or resp.get("message", {}).get("id")

        if not conv_id or not msg_id:
            return GenieResponse(
                question=question, status="FAILED",
                error=f"Missing conversation_id or message_id: {resp}",
                duration_seconds=time.time() - start,
            )

        # 2. Poll until terminal state
        last_status = None
        message = None

        while (time.time() - start) < timeout:
            try:
                message = client.api_client.do(
                    "GET", f"{base}/conversations/{conv_id}/messages/{msg_id}"
                )
            except Exception as e:
                error_str = str(e).lower()
                if "429" in error_str or "rate" in error_str:
                    return GenieResponse(
                        question=question, status="FAILED",
                        error="rate_limited", duration_seconds=time.time() - start,
                        states=states, conversation_id=conv_id,
                    )
                return GenieResponse(
                    question=question, status="FAILED",
                    error=str(e), duration_seconds=time.time() - start,
                    states=states, conversation_id=conv_id,
                )

            status = message.get("status", "UNKNOWN")

            if status != last_status:
                states.append(status)
                last_status = status

            if status in _TERMINAL_STATES:
                break

            time.sleep(_POLL_INTERVAL)
        else:
            return GenieResponse(
                question=question, status="TIMEOUT",
                error=f"Timed out after {timeout}s",
                duration_seconds=time.time() - start,
                states=states, conversation_id=conv_id,
            )

        # 3. Parse response
        duration = time.time() - start

        if status == "FAILED":
            error_msg = message.get("error", {})
            if isinstance(error_msg, dict):
                error_msg = error_msg.get("message", str(error_msg))
            return GenieResponse(
                question=question, status="FAILED",
                error=str(error_msg), duration_seconds=duration,
                states=states, conversation_id=conv_id,
            )

        if status != "COMPLETED":
            return GenieResponse(
                question=question, status=status,
                error=f"Terminal state: {status}",
                duration_seconds=duration, states=states, conversation_id=conv_id,
            )

        # 4. Extract SQL and result from attachments
        generated_sql = None
        description = None
        result = None
        attachment_id = None

        for att in message.get("attachments") or []:
            if "query" in att:
                query_obj = att["query"]
                generated_sql = query_obj.get("query")
                description = query_obj.get("description")
                attachment_id = att.get("attachment_id") or att.get("id")

        # 5. Fetch query result if we have an attachment_id
        if attachment_id:
            try:
                qr_resp = client.api_client.do(
                    "GET",
                    f"{base}/conversations/{conv_id}/messages/{msg_id}/attachments/{attachment_id}/query-result",
                )
                stmt = qr_resp.get("statement_response", {})
                columns = []
                if stmt.get("manifest", {}).get("schema", {}).get("columns"):
                    columns = [
                        {"name": c["name"], "type_name": c.get("type_name", "")}
                        for c in stmt["manifest"]["schema"]["columns"]
                    ]
                data = stmt.get("result", {}).get("data_array", [])
                result = {"columns": columns, "data": data, "row_count": len(data)}
            except Exception as e:
                logger.warning("Failed to fetch query result: %s", e)

        return GenieResponse(
            question=question, status="COMPLETED",
            generated_sql=generated_sql, description=description,
            result=result, duration_seconds=duration,
            states=states, conversation_id=conv_id,
        )

    @trace(name="genie_get_config", span_type="CHAIN")
    def get_config(self) -> dict:
        """Fetch the current space config."""
        client = get_workspace_client()
        resp = client.api_client.do(
            "GET",
            f"/api/2.0/genie/spaces/{self.space_id}",
            query={"include_serialized_space": "true"},
        )
        serialized = resp.get("serialized_space", "{}")
        return json.loads(serialized)

    @trace(name="genie_update_config", span_type="CHAIN")
    def update_config(self, config: dict) -> dict:
        """Update the space config via PATCH. Strips _-prefixed fields."""
        client = get_workspace_client()
        clean = {k: v for k, v in config.items() if not k.startswith("_")}
        return client.api_client.do(
            "PATCH",
            f"/api/2.0/genie/spaces/{self.space_id}",
            body={"serialized_space": json.dumps(clean)},
        )
