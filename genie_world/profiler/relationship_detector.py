"""Relationship detector: infers foreign-key relationships from column naming patterns."""

from __future__ import annotations

from genie_world.profiler.models import DetectionMethod, Relationship, TableProfile

# Suffixes that indicate a foreign-key column
_FK_SUFFIXES = ("_id", "_key", "_fk")

# Candidate PK column names on the target table
_PK_CANDIDATES = {"id", "key"}


def _extract_prefix(column_name: str) -> str | None:
    """Return the prefix of a FK column, or None if not a FK column.

    Examples:
        "customer_id" -> "customer"
        "order_key"   -> "order"
        "user_fk"     -> "user"
        "name"        -> None
    """
    for suffix in _FK_SUFFIXES:
        if column_name.endswith(suffix) and len(column_name) > len(suffix):
            return column_name[: -len(suffix)]
    return None


def _candidate_table_names(prefix: str) -> list[str]:
    """Return candidate table names to check for a given prefix.

    Tries plural and singular forms:
        prefix itself (e.g. "customer")
        prefix + "s"  (e.g. "customers")
        prefix + "es" (e.g. "statuses")
        prefix without trailing "s" (e.g. "order" from "orders")
    """
    candidates = [prefix, prefix + "s", prefix + "es"]
    if prefix.endswith("s") and len(prefix) > 1:
        candidates.append(prefix[:-1])
    # Deduplicate while preserving order
    seen: set[str] = set()
    result: list[str] = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            result.append(c)
    return result


def detect_by_naming_patterns(tables: list[TableProfile]) -> list[Relationship]:
    """Detect FK relationships by scanning columns ending in _id, _key, or _fk.

    For each such column in each table, the prefix (e.g. "customer" from
    "customer_id") is extracted and a matching table is looked up (trying
    plural/singular variants). If the target table has a "id" or "key" column
    a Relationship is created with confidence=0.6.

    Self-references (source_table == target_table) are excluded.

    Args:
        tables: List of TableProfile objects to scan.

    Returns:
        A list of inferred Relationship objects.
    """
    # Build lookup: table name (lower) -> (full_name, set of column names (lower))
    table_lookup: dict[str, tuple[str, set[str]]] = {}
    for tbl in tables:
        full_name = f"{tbl.catalog}.{tbl.schema_name}.{tbl.table}"
        col_names = {col.name.lower() for col in tbl.columns}
        table_lookup[tbl.table.lower()] = (full_name, col_names)

    relationships: list[Relationship] = []

    for tbl in tables:
        source_full = f"{tbl.catalog}.{tbl.schema_name}.{tbl.table}"

        for col in tbl.columns:
            prefix = _extract_prefix(col.name.lower())
            if prefix is None:
                continue

            # Try to find matching target table
            target_entry: tuple[str, set[str]] | None = None
            matched_table_name: str | None = None
            for candidate in _candidate_table_names(prefix):
                if candidate in table_lookup:
                    target_entry = table_lookup[candidate]
                    matched_table_name = candidate
                    break

            if target_entry is None:
                continue

            target_full, target_cols = target_entry

            # Skip self-references
            if target_full == source_full:
                continue

            # Check target table has a matching PK column
            pk_col: str | None = None
            for pk_candidate in _PK_CANDIDATES:
                if pk_candidate in target_cols:
                    pk_col = pk_candidate
                    break

            if pk_col is None:
                continue

            relationships.append(
                Relationship(
                    source_table=source_full,
                    source_column=col.name,
                    target_table=target_full,
                    target_column=pk_col,
                    confidence=0.6,
                    detection_method=DetectionMethod.NAMING_PATTERN,
                )
            )

    return relationships


def merge_relationships(*sources: list[Relationship]) -> list[Relationship]:
    """Merge multiple relationship lists, deduplicating by (src_table, src_col, tgt_table, tgt_col).

    When duplicates are found, the one with the highest confidence is kept.

    Args:
        *sources: Variable number of Relationship lists.

    Returns:
        A deduplicated list of Relationship objects.
    """
    best: dict[tuple[str, str, str, str], Relationship] = {}

    for source in sources:
        for rel in source:
            key = (
                rel.source_table,
                rel.source_column,
                rel.target_table,
                rel.target_column,
            )
            existing = best.get(key)
            if existing is None or rel.confidence > existing.confidence:
                best[key] = rel

    return list(best.values())
