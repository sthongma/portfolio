window.resumeI18n = {
    en: {
      documentTitle: "Sahatsawat Thongma — Data Engineer",
      htmlLang: "en",
      name: "Sahatsawat Thongma",
      roleTitle: "End-to-End Data Engineer &nbsp;·&nbsp; E-Commerce Platforms",
      location: "Samut Sakhon, Thailand",
      tabs: ["Resume", "Projects"],
      sectionSummary: "Summary",
      summary: [
        "<strong>Data engineer with hands-on e-commerce operations experience</strong>, combining ERP/accounting, BI, data warehousing, and production web apps to replace manual workflows with reliable automated systems."
      ],
      sectionExperience: "Experience",
      epPeriod: "2025 – Present",
      epRole: "Data Engineer &nbsp;·&nbsp; E-Commerce Data Platform",
      epBullets: [
        "<strong>Built the company's first data warehouse</strong>, automating daily marketplace data prep that previously took 1–3 staff half a day in Excel/Power Query.",
        "Designed a <strong>dbt medallion architecture</strong> with 193 models across 9 business domains, serving executive, procurement, warehouse, and fulfillment reporting.",
        "Built ingestion with Power Automate, Python, and 46 Azure Data Factory pipelines, plus a FastAPI + React config app with AI-assisted column analysis for controlled self-service mappings.",
        "Shipped production apps used daily: WMS, real-time barcode scanner network, and Azure-powered invoice OCR, orchestrated with Airflow/ADF and quality alerts."
      ],
      travelerRoles: [
        "Accountant / ERP &amp; BI Analyst &nbsp;·&nbsp; Graphic Designer, E-Commerce"
      ],
      travelerBullets: [
        "Managed Ecount ERP workflows for sales and finance teams, resolved data issues, and trained new users.",
        "Built Power BI reporting and used Power Query, SQL, and Python to reconcile ERP exports for sales and finance.",
        "Created e-commerce product imagery and maintained product/SKU data across marketplace and ERP systems."
      ],
      projectTaglines: [
        "Full data platform serving 9 business domains across multiple e-commerce channels and warehouse operations.",
        "Full-featured WMS with an <strong>interactive visual warehouse map</strong> — used daily by warehouse teams.",
        "Real-time multi-user barcode scanning system with workflow enforcement.",
        "AI-assisted invoice processing that reads Thai e-commerce PDF invoices and fills structured records automatically."
      ],
      projectBullets: [
        [
          "Ingests data from 6+ sources (Shopee, Lazada, TikTok, JST, DHL, internal WMS/BSN) → 4-layer medallion architecture with dbt.",
          "<strong>Bronze config web app</strong> (FastAPI + React) — self-service UI for file types, validation rules, and AI-assisted column analysis/mapping suggestions.",
          "Serves <strong>FastAPI + React dashboards</strong> with real-time KPIs, price alerts, procurement tracking, and operational drilldowns.",
          "Runs automatically outside business hours with data quality checks and email alerts on failure or stale data."
        ],
        [
          "<strong>Interactive warehouse grid map</strong> (dynamic CSS Grid, server-side) with 4-level color-coded stock states (empty / low / ok / critical) that instantly surface low, incomplete, or misplaced slots — plus a custom annotation system for reserved zones and merged-cell support for pallet-sized locations.",
          "Real-time inventory tracking + 4-type task workflow (pick, count, move, receive) with task chat and full audit logging.",
          "Mobile barcode/QR scanning + bilingual UI (Thai/English, 736 i18n keys).",
          "19 granular permissions, 21 SQLAlchemy models, 62 Jinja2 templates — deployed on Azure App Service with GitHub Actions CI/CD."
        ],
        [
          "Processes concurrent-user scans with validation + duplicate detection, and enforces workflow sequences through job dependency rules (e.g., \"Release\" before \"Outbound\").",
          "Custom notifications + audio alerts, full scan history with filtering and Excel export."
        ],
        [
          "Uses Azure Document Intelligence to read invoice files and automatically populate fields such as invoice number, dates, amounts, vendor/customer, tax IDs, and VAT.",
          "Supports Thai-specific formats (Buddhist Era dates, Thai Baht, UTF-8), caches results by file hash, and exports completed records to branded Excel."
        ]
      ],
      compactProjects: [
        "Generic Excel/CSV-to-SQL Server ETL tool with Tkinter GUI and CLI for non-technical users. Supports Replace and Upsert modes, auto-schema detection, batch processing, column mapping, and metadata tracking. Ships as a Windows installer via PyInstaller.",
        "File sync utility for staging raw data files of any type (Excel, CSV, PDF, images, archives, ...) into the pipeline's landing folders. Features SQLite-backed history tracking for deduplication, per-mapping copy/move modes, depth and date filtering, collision handling, dry-run preview, and progress reporting. Configured through a single CSV so non-developers can update mappings without code changes.",
        "Lightweight Flask service that receives webhook events from DHL (shipment and tracking updates) and persists them to SQL Server via PyODBC — a minimal integration bridge that streams logistics events straight into the warehouse data layer.",
        "Python scraper that collects flash-sale and return data from e-commerce platforms, feeding raw files into the Bronze layer of the main data pipeline."
      ],
      sectionSkills: "Technical Skills",
      sectionEducation: "Education",
      languages: "Thai (native) &nbsp;·&nbsp; English (technical documentation, async written communication)",
      university: "Udon Thani Rajabhat University",
      universityPeriod: "Completed first-year coursework",
      universityDegree: "Mechatronics &amp; Robotics Engineering — completed first-year coursework before leaving to run an online store full-time. Built practical skills through real e-commerce problems: <strong>graphic design → accounting → data analytics → data engineering</strong>."
    },
    th: {
      documentTitle: "สหัสวรรษ ทองมา — Data Engineer · ประวัติส่วนตัว",
      htmlLang: "th",
      name: "สหัสวรรษ ทองมา",
      roleTitle: "Data Engineer (End-to-End) &nbsp;·&nbsp; สาย E-Commerce",
      location: "สมุทรสาคร, ประเทศไทย",
      tabs: ["Resume", "Projects"],
      sectionSummary: "เกี่ยวกับผม",
      summary: [
        "<strong>Data Engineer ที่เข้าใจงาน e-commerce operation จากหน้างานจริง</strong> ผสมประสบการณ์ ERP/accounting, BI, data warehouse และ production web apps เพื่อเปลี่ยนงาน manual ให้เป็นระบบอัตโนมัติที่ใช้งานได้จริง"
      ],
      sectionExperience: "ประสบการณ์ทำงาน",
      epPeriod: "2025 – ปัจจุบัน",
      epRole: "Data Engineer &nbsp;·&nbsp; E-Commerce Data Platform",
      epBullets: [
        "<strong>สร้าง data warehouse ตัวแรกของบริษัท</strong> แทนงานเตรียมข้อมูล marketplace ที่เคยใช้พนักงาน 1–3 คนทำ Excel/Power Query ครึ่งวันทุกวัน",
        "ออกแบบ <strong>dbt medallion architecture</strong> 193 models ครอบคลุม 9 business domains สำหรับ executive, procurement, warehouse และ fulfillment reporting",
        "สร้าง ingestion ด้วย Power Automate, Python และ Azure Data Factory 46 pipelines พร้อม FastAPI + React config app ที่ต่อ AI ช่วยวิเคราะห์คอลัมน์และแนะนำ mapping ได้อย่างควบคุม",
        "Ship production apps ที่ทีมใช้ทุกวัน: WMS, real-time barcode scanner network และ Azure-powered invoice OCR พร้อม orchestration และ quality alerts"
      ],
      travelerRoles: [
        "Accountant / ERP &amp; BI Analyst &nbsp;·&nbsp; Graphic Designer, E-Commerce"
      ],
      travelerBullets: [
        "ดูแล Ecount ERP workflows ให้ทีมขายและการเงิน แก้ปัญหา data และสอน user ใหม่",
        "สร้าง Power BI reporting และใช้ Power Query, SQL, Python reconcile ERP exports สำหรับ sales/finance",
        "ออกแบบรูปสินค้า e-commerce และดูแล product/SKU data ระหว่าง marketplace กับ ERP"
      ],
      projectTaglines: [
        "Full data platform ที่ serve 9 business domains ครอบคลุม e-commerce channels หลายตัวและ warehouse operations",
        "Full-featured WMS พร้อม <strong>interactive visual warehouse map</strong> — ทีมคลังใช้งานจริงทุกวัน",
        "ระบบ barcode scanning แบบ real-time multi-user พร้อม workflow enforcement",
        "ระบบ AI อ่านไฟล์ invoice PDF ของ e-commerce ไทย แล้วลงข้อมูลเป็น structured records ให้อัตโนมัติ"
      ],
      projectBullets: [
        [
          "ดึง raw data จาก 6+ sources (Shopee, Lazada, TikTok, JST, DHL, internal WMS/BSN) → medallion architecture 4 ชั้นด้วย dbt",
          "<strong>Bronze config web app</strong> (FastAPI + React) — self-service UI สำหรับ file types, validation rules และ AI-assisted column analysis/mapping suggestions",
          "Serve <strong>FastAPI + React dashboards</strong> — real-time KPIs, price alerts, procurement tracking และ operational drilldowns",
          "รันอัตโนมัตินอกเวลาทำงานพร้อม data quality checks, email alerts เมื่อ fail หรือ stale"
        ],
        [
          "<strong>Interactive warehouse grid map</strong> (dynamic CSS Grid, server-side) แสดง stock level 4 ระดับ (empty / low / ok / critical) บอกจุดที่ของน้อย, ไม่ครบ, หรือวางไม่ตรง — พร้อม annotation system สำหรับ reserved zones และ merged-cell support",
          "Real-time inventory tracking + task workflow 4 ประเภท (pick, count, move, receive) พร้อม task chat และ full audit logging",
          "Mobile barcode/QR scanning + bilingual UI (ไทย/อังกฤษ, 736 i18n keys)",
          "19 granular permissions, 21 SQLAlchemy models, 62 Jinja2 templates — deploy บน Azure App Service ด้วย GitHub Actions CI/CD"
        ],
        [
          "ประมวลผล scan พร้อม validation + duplicate detection สำหรับ concurrent users และ enforce workflow sequences ด้วย job dependency rules (เช่น \"Release\" ก่อน \"Outbound\")",
          "Custom notifications + audio alerts, เก็บ scan history ครบพร้อม filter และ Excel export"
        ],
        [
          "ใช้ Azure Document Intelligence อ่านไฟล์ invoice แล้วกรอกข้อมูลให้อัตโนมัติ เช่น เลข invoice, วันที่, ยอดเงิน, vendor/customer, เลขผู้เสียภาษี และ VAT",
          "รองรับ format เฉพาะไทย (พ.ศ., เงินบาท, UTF-8), cache ด้วย file hash เพื่อลดการเรียก API ซ้ำ และ export records ที่ลงข้อมูลแล้วเป็น Excel พร้อม branding"
        ]
      ],
      compactProjects: [
        "ETL tool สำหรับโหลด Excel/CSV เข้า SQL Server พร้อม Tkinter GUI และ CLI ให้ user ที่ไม่ใช่ developer ใช้งานได้ รองรับ Replace และ Upsert mode, auto-schema detection, batch processing, column mapping และ metadata tracking แพ็คเป็น Windows installer ด้วย PyInstaller",
        "File sync utility สำหรับ stage raw data files ทุกประเภท (Excel, CSV, PDF, images, archives, ...) เข้า landing folder ของ pipeline — มี SQLite history tracking กัน duplicate, copy/move modes ต่อ mapping, depth + date filtering, collision handling, dry-run preview และ progress bar config ผ่าน CSV ไฟล์เดียวให้ user ที่ไม่ใช่ developer แก้ source / destination mapping ได้",
        "Flask service เบา ๆ ที่รับ webhook event จาก DHL (shipment / tracking updates) และ persist ลง SQL Server ด้วย PyODBC — เป็น integration bridge ระหว่าง DHL notification กับ warehouse data",
        "Python scraper ที่เก็บ flash-sale และ return data จาก e-commerce platforms ส่ง raw files ต่อเข้า Bronze layer ของ main data pipeline"
      ],
      sectionSkills: "ทักษะทางเทคนิค",
      sectionEducation: "การศึกษา",
      languages: "ไทย (เจ้าของภาษา) &nbsp;·&nbsp; English (technical documentation, async written communication)",
      university: "มหาวิทยาลัยราชภัฏอุดรธานี",
      universityPeriod: "เรียนจบ coursework ปี 1",
      universityDegree: "คณะวิศวกรรมเมคคาทรอนิกส์และหุ่นยนต์ — เรียนจบ coursework ปี 1 ก่อนออกมาทำร้านค้า online เต็มตัว และต่อยอดทักษะจากปัญหา e-commerce จริง: <strong>graphic design → accounting → data analytics → data engineering</strong>"
    }
  };
