"""Databricks authentication utilities."""

from __future__ import annotations

import contextvars
import logging
import os

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

_obo_token: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "_obo_token", default=None
)


def set_obo_token(token: str | None) -> None:
    _obo_token.set(token)


def get_obo_token() -> str | None:
    return _obo_token.get()


def is_running_on_databricks_apps() -> bool:
    return os.environ.get("DATABRICKS_APP_PORT") is not None


def get_workspace_client() -> WorkspaceClient:
    token = get_obo_token()
    if token:
        host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
        logger.debug("Creating OBO WorkspaceClient for host: %s", host)
        return WorkspaceClient(host=host, token=token, auth_type="pat")
    return WorkspaceClient()
