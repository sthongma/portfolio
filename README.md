# Sahatsawat Thongma

**Analytics Engineer / Data Engineer**

[Email] | [LinkedIn] | Bangkok, Thailand

---

## About

I build end-to-end data platforms for e-commerce and warehouse operations — from raw data ingestion through data warehouse modeling to business dashboards and operational web applications.

My focus is on turning messy, multi-source business data into reliable analytical models that drive real decisions.

---

## Production Systems

### 1. E-Commerce Data Pipeline & Analytics Platform

> Full data platform serving 9 business domains across multiple e-commerce channels and warehouse operations.

**What it does:**
- Ingests raw data from 6+ sources (marketplace platforms, warehouse systems, logistics providers) — handling 31 different file formats
- Transforms data through a 4-layer medallion architecture (Bronze → Staging → Silver → Gold)
- Serves analytical dashboards with real-time KPIs, price alerts, procurement tracking, and AI-powered chat queries
- Runs automated daily orchestration with monitoring and alerting

**Scale:**

| Metric | Count |
|--------|-------|
| dbt models | 193 |
| Data sources | 31 file types from 6+ systems |
| Business domains | 9 (sales, fulfillment, inventory, procurement, logistics, warehouse, pricing, demand, suppliers) |
| Custom macros | 14 |
| Airflow DAGs | 6 |
| API endpoints | 13 routers |
| Test coverage | 95%+ |

**Architecture:**
```
External Files (Excel/CSV)        Marketplace APIs
        |                              |
        v                              v
  [Bronze Layer] ---- SHA-256 dedup, metadata tracking, chunked loading
        |
        v
  [dbt Staging] ---- 38 models: type casting, null standardization
        |
        v
  [dbt Silver] ---- 42 incremental models: row-level dedup via _row_hash
        |
        v
  [dbt Intermediate] ---- 113 views: business logic, joins, calculations
        |
        v
  [dbt Gold] ---- fact tables, dimensions, reports (incremental refresh)
        |
        v
  [FastAPI + React Dashboard] ---- role-based access, price alerts, AI chat
        |
  [Airflow Orchestration] ---- daily scheduled, email alerts, freshness checks
```

**Tech:** dbt, SQL Server, Python, Pandas, Airflow, FastAPI, React/TypeScript, Docker, GitHub Actions

**Code examples:** [`examples/dbt-models/`](examples/dbt-models/) | [`examples/bronze-ingestion/`](examples/bronze-ingestion/) | [`examples/airflow-dags/`](examples/airflow-dags/)

---

### 2. Warehouse Management System (WMS)

> Full-featured WMS used daily by warehouse teams for picking, counting, moving, and receiving operations.

**What it does:**
- Manages inventory across warehouse locations with real-time available/reserved quantity tracking
- Assigns and tracks tasks (pick, count, move, receive) with item-level and location-level completion
- Provides interactive warehouse grid mapping with location slotting (min/max stock levels)
- Supports barcode/QR camera scanning for mobile operations
- Full audit logging of all inventory movements and user actions

**Scale:**

| Metric | Count |
|--------|-------|
| Database models | 15+ |
| Permissions | 16 granular |
| Languages | 2 (Thai/English, 1,000+ keys) |
| Current version | v1.5.94 |

**Tech:** FastAPI, SQLAlchemy, SQL Server, Jinja2, JavaScript (Canvas API), Docker, Azure App Service, GitHub Actions CI/CD

**Code examples:** [`examples/fastapi-backend/`](examples/fastapi-backend/)

---

### 3. Barcode Scanner Network (BSN)

> Real-time multi-user barcode scanning system with workflow enforcement for warehouse operations.

**What it does:**
- Processes barcode scans with validation and duplicate detection across multiple concurrent users
- Enforces workflow sequences through job dependency rules (e.g., item must be scanned at "Release" before "Outbound")
- Triggers custom notifications and audio alerts based on scan events
- Provides full scan history with filtering, Excel export, and audit trail

**Architecture:**
```
Barcode Scanner (Camera/USB)
        |
        v
  [FastAPI Backend] ---- validate → check dependencies → detect duplicates
        |
        v
  [SQL Server] ---- scan_logs, jobs, dependencies, notifications, audit
        |
        v
  [HTMX Frontend] ---- real-time UI updates without full page reload
        |
  [Azure Blob Storage] ---- custom sound files for alerts
```

**Tech:** FastAPI, HTMX, TailwindCSS, SQL Server, Azure Blob Storage, Docker

---

### 4. OCR Invoice Processor

> Automated data extraction from Thai e-commerce platform invoices using Azure AI.

**What it does:**
- Batch processes PDF invoices from multiple platforms using Azure Document Intelligence
- Extracts structured data: invoice number, dates, amounts, vendor/customer info, tax IDs, VAT
- Handles Thai-specific formats (Buddhist Era dates, Thai Baht, UTF-8)
- Caches results using file hash to avoid redundant API calls and reduce costs
- Exports formatted Excel files with branding

**Tech:** Python, Streamlit, Azure Document Intelligence, SQLite, OpenPyXL

---

## Technical Skills

| Area | Technologies |
|------|-------------|
| **Data Modeling & SQL** | SQL Server, T-SQL, dbt (dbt-sqlserver), Medallion Architecture, Kimball methodology, Incremental strategies, Data quality testing |
| **Python** | Pandas, SQLAlchemy, FastAPI, Airflow, Streamlit, Pydantic |
| **Frontend** | React, TypeScript, Jinja2, HTMX, TailwindCSS |
| **Cloud & DevOps** | Docker, GitHub Actions, Azure (App Service, Blob Storage, Document Intelligence) |

---

## Code Examples

This repository contains sanitized code samples from the projects above. They demonstrate patterns and architecture without exposing business-specific logic.

| Directory | What it shows |
|-----------|--------------|
| [`examples/dbt-models/`](examples/dbt-models/) | Medallion architecture: staging → silver → intermediate → gold, with incremental strategy, macros, and schema tests |
| [`examples/bronze-ingestion/`](examples/bronze-ingestion/) | YAML-driven file ingestion pipeline with metadata tracking and deduplication |
| [`examples/fastapi-backend/`](examples/fastapi-backend/) | FastAPI router + query pattern with SQL Server, auth, and pagination |
| [`examples/airflow-dags/`](examples/airflow-dags/) | Orchestrator DAG pattern with sequential dependency execution |
