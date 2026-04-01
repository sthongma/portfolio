"""
Metadata Manager.
Generates provenance metadata for every imported file:
  - _source_file_path: Absolute path to source file
  - _file_sha256: SHA-256 hash for file-level deduplication
  - _batch_id: UUID for grouping rows from same import
  - _imported_by: Database or system username
  - _imported_at: Import timestamp
"""

import getpass
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


class MetadataManager:
    """Manage metadata for bronze layer imports."""

    def get_metadata(self, file_path: str, file_hash: str, batch_id: str = None) -> Dict[str, Any]:
        """
        Generate metadata dictionary for a file import.

        Args:
            file_path: Path to source file
            file_hash: SHA-256 hash of file
            batch_id: Optional batch ID (auto-generated UUID if not provided)
        """
        if batch_id is None:
            batch_id = str(uuid.uuid4())

        return {
            "_source_file_path": self.get_absolute_path(file_path),
            "_file_sha256": file_hash,
            "_batch_id": batch_id,
            "_imported_by": self.get_current_user(),
            "_imported_at": datetime.now(),
            # _loaded_at: set by SQL DEFAULT GETDATE()
            # _row_hash: added by transformer if row-level dedup is enabled
        }

    def get_absolute_path(self, file_path: str) -> str:
        """Normalize file path to absolute."""
        return str(Path(file_path).resolve())

    def get_current_user(self) -> str:
        """
        Get current username.
        Priority: DB_USERNAME env var -> OS username -> 'UNKNOWN'
        """
        db_user = os.getenv("DB_USERNAME", "").strip()
        if db_user:
            return db_user
        try:
            return getpass.getuser()
        except Exception:
            return "UNKNOWN"

    def add_metadata_to_dataframe(self, df, file_path: str, file_hash: str, batch_id: str = None):
        """
        Add metadata columns to a pandas DataFrame.
        Broadcasts metadata values to all rows.
        """
        metadata = self.get_metadata(file_path, file_hash, batch_id)
        for col_name, col_value in metadata.items():
            df[col_name] = col_value
        return df
