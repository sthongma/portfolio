"""
File Type Detector.
Auto-detect file type based on column headers using a two-pass matching approach.
Supports Excel (.xlsx/.xls) and CSV files with configurable sheet/header options.
"""

import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


class FileTypeDetector:
    """Detect file type by matching column headers against YAML configurations."""

    def __init__(self, file_types_config: Dict[str, Dict[str, Any]]):
        self.file_types_config = file_types_config

    def detect_file_type(self, file_path: str) -> Optional[Tuple[str, Dict[str, Any]]]:
        """
        Detect file type based on column headers.

        Two-pass approach:
        1. Try with default settings (sheet 0, header row 1)
        2. Try with each config's excel_options if first pass fails

        Detection logic:
        - Columns NOT in optional_columns are required
        - Match succeeds when ALL required columns are present
        - Best match (highest score) wins

        Returns:
            Tuple of (file_type_name, config) or None if no match.
        """
        try:
            default_columns = self._read_file_columns(file_path)
        except Exception as e:
            print(f"Error reading file columns: {e}")
            return None

        best_match = None
        best_match_score = 0.0

        for file_type_name, config in self.file_types_config.items():
            detection_config = config.get("detection", {})
            column_mapping = config.get("column_mapping", {})
            if not column_mapping:
                continue

            # Try config-specific excel_options if available
            excel_options = config.get("excel_options", {})
            if excel_options:
                try:
                    columns = self._read_file_columns(file_path, excel_options)
                except Exception:
                    columns = default_columns
            else:
                columns = default_columns

            # Determine required vs optional columns
            optional_targets = set(detection_config.get("optional_columns", []))
            required_columns = [
                key for key, value in column_mapping.items() if value not in optional_targets
            ]
            match_score = self._calculate_match_score(columns, required_columns)

            if match_score >= 1.0 and match_score > best_match_score:
                best_match = (file_type_name, config)
                best_match_score = match_score

        return best_match

    def detect_all_matching_file_types(self, file_path: str) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Detect ALL matching file types (multi-config support).
        Enables one Excel file with multiple sheets to match different configs.
        """
        try:
            default_columns = self._read_file_columns(file_path)
        except Exception:
            return []

        matches = []
        for file_type_name, config in self.file_types_config.items():
            detection_config = config.get("detection", {})
            column_mapping = config.get("column_mapping", {})
            if not column_mapping:
                continue

            excel_options = config.get("excel_options", {})
            columns = default_columns
            if excel_options:
                try:
                    columns = self._read_file_columns(file_path, excel_options)
                except Exception:
                    pass

            optional_targets = set(detection_config.get("optional_columns", []))
            required_columns = [
                key for key, value in column_mapping.items() if value not in optional_targets
            ]

            if self._calculate_match_score(columns, required_columns) >= 1.0:
                matches.append((file_type_name, config))

        return matches

    def _read_file_columns(
        self, file_path: str, excel_options: Optional[Dict[str, Any]] = None
    ) -> List[str]:
        """Read column headers from Excel or CSV file."""
        file_path = Path(file_path)
        extension = file_path.suffix.lower()

        if extension in [".xlsx", ".xls"]:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
                read_params = {"nrows": 0}
                if excel_options:
                    if "sheet_name" in excel_options:
                        read_params["sheet_name"] = excel_options["sheet_name"]
                    if "header_row" in excel_options:
                        read_params["header"] = excel_options["header_row"] - 1
                df = pd.read_excel(file_path, **read_params)
        elif extension == ".csv":
            df = pd.read_csv(file_path, nrows=0)
        else:
            raise ValueError(f"Unsupported file type: {extension}")

        return df.columns.tolist()

    def _calculate_match_score(self, file_columns: List[str], required_columns: List[str]) -> float:
        """Calculate match score (0.0 to 1.0) between file and required columns."""
        if not required_columns:
            return 0.0
        matched = sum(1 for col in required_columns if col in file_columns)
        return matched / len(required_columns)
