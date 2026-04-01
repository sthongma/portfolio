"""
Deduplication Service.
Prevents duplicate file imports by checking SHA-256 hash against the target table.
Returns previous import info (who, when, from where) for skip decisions.
"""

import logging
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


class DeduplicationService:
    """Check for duplicate files in bronze layer."""

    def __init__(self, engine: Engine, dedup_config: Dict[str, Any]):
        self.engine = engine
        self.dedup_config = dedup_config
        self.logger = logging.getLogger(__name__)

    def table_exists(self, schema_name: str, table_name: str) -> bool:
        """Check if table exists in database using INFORMATION_SCHEMA."""
        try:
            query = """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = :schema_name
            AND TABLE_NAME = :table_name
            """
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(query), {"schema_name": schema_name, "table_name": table_name}
                )
                return result.scalar() > 0
        except Exception:
            return False

    def is_file_already_imported(self, schema_name: str, table_name: str, file_hash: str) -> bool:
        """Check if file hash already exists in target table."""
        if not self.dedup_config.get("enabled", False):
            return False

        if not self.table_exists(schema_name, table_name):
            return False

        try:
            hash_column = self.dedup_config.get("hash_column", "_file_sha256")
            query = f"""
            SELECT TOP 1 1
            FROM [{schema_name}].[{table_name}]
            WHERE [{hash_column}] = :file_hash
            """
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"file_hash": file_hash})
                return result.fetchone() is not None
        except Exception as e:
            self.logger.error(f"Error checking for duplicate: {e}")
            return False  # On error, assume not duplicate (safe default)

    def get_file_import_info(
        self, schema_name: str, table_name: str, file_hash: str
    ) -> Optional[Dict[str, Any]]:
        """Get previous import details: who imported, when, from where."""
        if not self.dedup_config.get("enabled", False):
            return None

        try:
            hash_column = self.dedup_config.get("hash_column", "_file_sha256")
            query = f"""
            SELECT TOP 1
                [_source_file_path],
                [_imported_by],
                [_imported_at]
            FROM [{schema_name}].[{table_name}]
            WHERE [{hash_column}] = :file_hash
            ORDER BY [_imported_at] DESC
            """
            result = pd.read_sql(text(query), self.engine, params={"file_hash": file_hash})
            if len(result) > 0:
                return result.iloc[0].to_dict()
            return None
        except Exception as e:
            self.logger.error(f"Error getting file import info: {e}")
            return None

    def should_skip_file(
        self, schema_name: str, table_name: str, file_hash: str
    ) -> tuple[bool, Optional[str]]:
        """
        Check if file should be skipped due to deduplication.

        Returns:
            (should_skip, reason_message)
        """
        if not self.dedup_config.get("skip_duplicate_files", True):
            return False, None

        if self.is_file_already_imported(schema_name, table_name, file_hash):
            info = self.get_file_import_info(schema_name, table_name, file_hash)
            if info:
                reason = (
                    f"File already imported:\n"
                    f"  Previously imported: {info.get('_imported_at', 'unknown')}\n"
                    f"  Imported by: {info.get('_imported_by', 'unknown')}\n"
                    f"  Source path: {info.get('_source_file_path', 'unknown')}"
                )
            else:
                reason = "File with this hash already exists in database"
            return True, reason

        return False, None
