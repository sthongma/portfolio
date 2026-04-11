# Portfolio — Sahatsawat Thongma

> **End-to-end data engineer · e-commerce platforms**
> Sole data engineer at EP Asia Group, building the full data stack (pipeline → warehouse → dashboards → operational apps) solo with AI pair programming.

---

## 🔗 Live links

| What | URL |
|---|---|
| 🇬🇧 **Resume (EN)** | [sthongma.github.io/portfolio/resume-en.html](https://sthongma.github.io/portfolio/resume-en.html) |
| 🇹🇭 **Resume (TH)** | [sthongma.github.io/portfolio/resume-th.html](https://sthongma.github.io/portfolio/resume-th.html) |
| 📊 **Fulfillment dashboard mock** | [sthongma.github.io/portfolio/fulfillment-dashboard.html](https://sthongma.github.io/portfolio/fulfillment-dashboard.html) |

> **Tip:** The resume is interactive — use the **Resume / Projects** tabs at the top to switch between the compact CV view and the rich project gallery.

---

## 📁 Repository contents

This is my public portfolio repository. It's organized into three parts:

### 1. Self-contained HTML resumes

Single-file HTML resumes with dual-mode layout (compact print-ready CV + rich web portfolio view). No build step, no external dependencies.

- [`resume-en.html`](resume-en.html) — English version
- [`resume-th.html`](resume-th.html) — Thai version (with sans-serif overrides for Thai rendering)

### 2. Mock dashboard

A static HTML mock that replicates the real **Fulfillment Overview** dashboard from the E-Commerce Data Platform I built at EP Asia Group. Same color palette, layout structure, and data shapes as the production UI. All data is fictional.

- [`fulfillment-dashboard.html`](fulfillment-dashboard.html) — live demo

### 3. Sanitized code examples

Real patterns from production systems I've built, with business-specific logic removed. The code is representative, not the actual production source.

| Directory | Pattern demonstrated |
|---|---|
| [`examples/dbt-models/`](examples/dbt-models/) | Medallion architecture (staging → silver → intermediate → gold), incremental strategies, macros, schema tests |
| [`examples/bronze-ingestion/`](examples/bronze-ingestion/) | YAML-driven file ingestion with SHA-256 deduplication and metadata tracking |
| [`examples/fastapi-backend/`](examples/fastapi-backend/) | FastAPI router + query pattern for SQL Server with authentication and pagination |
| [`examples/airflow-dags/`](examples/airflow-dags/) | Orchestrator DAG with sequential dependency execution |

---

## 🧭 Where to start

- **Recruiter / hiring manager?** → Open the [resume](https://sthongma.github.io/portfolio/resume-en.html) and the [dashboard mock](https://sthongma.github.io/portfolio/fulfillment-dashboard.html).
- **Engineer reviewing my work?** → Browse the [`examples/`](examples/) folder for representative code.
- **Just curious?** → The [resume's Projects tab](https://sthongma.github.io/portfolio/resume-en.html) walks through every production system I've built.

---

## 📬 Contact

- **Email:** sahatsawat.thongma@gmail.com
- **LinkedIn:** [linkedin.com/in/sahatsawat-thongma](https://www.linkedin.com/in/sahatsawat-thongma-82bb45396)
- **GitHub:** [github.com/sthongma](https://github.com/sthongma)
- **Location:** Samut Sakhon, Thailand

---

## 📄 About the code samples

Code in [`examples/`](examples/) is provided as reference patterns. Feel free to read, learn from, or adapt. Nothing here is the actual production code — it's been stripped of business logic, credentials, and anything proprietary. The production systems themselves live in private repos.
