"""
Bronze Pipeline Runner.
Orchestrates pipeline execution — separated from CLI presentation layer.
Routes to appropriate method based on input: config folders, single file, or folder.
"""

import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from src.cli.display_formatter import (
    display_aggregate_summary,
    display_file_details,
    display_file_table,
    display_summary,
)
from src.core.pipeline import BronzePipeline

# Ensure UTF-8 encoding for Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")


class BronzePipelineRunner:
    """
    Orchestrates Bronze pipeline execution.
    Handles business logic separated from CLI presentation.
    """

    def __init__(self, pipeline: BronzePipeline, logger: Optional[logging.Logger] = None):
        self.pipeline = pipeline
        self.logger = logger or logging.getLogger(__name__)

    def run_config_folders(self) -> Dict[str, Any]:
        """Run pipeline on folders defined in app_settings.yaml."""
        input_folders = self.pipeline.app_settings["file_management"].get("input_folders", [])
        file_types = self.pipeline.file_types
        print(f"\n  {len(input_folders)} folders · {len(file_types)} file types\n")

        results = self.pipeline.process_config_folders()

        # Extract all files from folder results
        all_files = []
        for folder_result in results.get("folders", []):
            if folder_result.get("exists"):
                all_files.extend(folder_result.get("files", []))

        if all_files:
            display_file_table(all_files)

        display_aggregate_summary(results)
        return results

    def run_single_file(self, file_path: str) -> Dict[str, Any]:
        """Run pipeline on a single file."""
        file_result = self.pipeline.process_file(str(Path(file_path)))
        display_file_details(file_result)
        return file_result

    def run_folder(self, folder_path: str) -> Dict[str, Any]:
        """Run pipeline on all files in a folder."""
        file_types = self.pipeline.file_types
        print(f"\n  {folder_path} · {len(file_types)} file types\n")

        results = self.pipeline.process_folder(str(folder_path))

        if results.get("files"):
            display_file_table(results["files"])

        display_summary(results)
        return results

    def run(self, path: Optional[str] = None) -> Dict[str, Any]:
        """
        Main orchestrator — routes to appropriate execution method.

        Args:
            path: Optional path to file or folder. If None, uses config folders.
        """
        if path is None:
            results = self.run_config_folders()
        elif Path(path).is_file():
            results = self.run_single_file(str(Path(path)))
        elif Path(path).is_dir():
            results = self.run_folder(str(Path(path)))
        else:
            raise ValueError(f"Invalid path: {path}")

        return results

    def close(self) -> None:
        """Close pipeline and cleanup resources."""
        self.pipeline.close()
