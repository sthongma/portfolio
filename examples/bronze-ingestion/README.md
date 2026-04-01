# Bronze Ingestion Layer — Code Examples

Sanitized examples from a production ETL pipeline that ingests 31 file types from 6+ data sources into SQL Server with metadata tracking, deduplication, and auto file-type detection.

## Files

| File | What it demonstrates |
|------|---------------------|
| `pipeline_runner.py` | Orchestrator pattern: routes execution (config folders / single file / folder) |
| `file_type_config.yaml` | YAML-driven configuration: detection, column mapping, dedup settings |
| `file_detector.py` | Auto file-type detection: two-pass matching, required/optional columns, scoring |
| `metadata_manager.py` | Provenance tracking: SHA-256, batch ID, user detection, DataFrame integration |
| `dedup_service.py` | File-level deduplication: hash lookup, import history, skip logic |
