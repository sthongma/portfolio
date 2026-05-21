window.resumeI18n = {
    en: {
      documentTitle: "Sahatsawat Thongma — Data Engineer",
      htmlLang: "en",
      name: "Sahatsawat Thongma",
      roleTitle: "Data Engineer",
      location: "Samut Sakhon, Thailand",
      tabs: ["Resume", "Projects"],
      sectionSummary: "Summary",
      summary: [
        "<strong>Data engineer building production data platforms.</strong> 3+ years across data, BI, and operations; currently designs and operates a <strong>50 GB warehouse processing 100K+ records/day</strong> for a 100M+ THB/month e-commerce business — <strong>dbt, Airflow, Azure (ADF, App Service), Python, SQL Server, FastAPI/React</strong>. Builds for non-technical business users (warehouse, procurement, exec)."
      ],
      sectionExperience: "Experience",
      epPeriod: "May 2025 – Present",
      epRole: "Data Engineer &nbsp;·&nbsp; E-Commerce Data Platform",
      epBullets: [
        "<strong>Built the company's first data warehouse</strong> — replaced messy Google Sheets/Excel for 25+ staff, saving <strong>25–75 person-hours/day across the team</strong> (1–3 hr/person) and eliminating cross-spreadsheet reconciliation errors.",
        "<strong>Designed the data warehouse structure</strong> — 193 transformation steps across 9 business areas, processing <strong>100K+ records/day</strong> from Shopee, Lazada, TikTok, Facebook. Powers <strong>BI dashboards</strong> for exec, procurement, warehouse, and fulfillment teams.",
        "<strong>Built ETL pipelines</strong> pulling marketplace file exports and internal data (Power Automate portal automation, Python, 46 Azure Data Factory pipelines). Plus a self-service config app so non-developers can add new data sources themselves.",
        "<strong>Replaced the broken warehouse system</strong> with custom apps (WMS, barcode scanner, invoice OCR) — Burmese teammates now learn in under 5 minutes; <strong>1,200+ orders/day</strong> with full audit trail.",
        "<strong>Worked with procurement, warehouse &amp; executive teams</strong> to find work that could be automated — replaced manual processes with <strong>low-code / no-code tools and AI</strong>."
      ],
      travelerRoles: [
        "Accountant / ERP &amp; BI Analyst"
      ],
      travelerBullets: [
        "Set up <strong>Ecount ERP master data from scratch</strong> — defined chart of accounts, customer/vendor master, and SKU mapping standards.",
        "Built <strong>Power BI reports</strong> and used <strong>Power Query</strong> to reconcile ERP data across sales and finance — established consistent monthly reporting.",
        "Established product/SKU master data across marketplaces and ERP as a single source of truth for the catalog."
      ],
      projectTaglines: [
        "Full data platform — 50 GB warehouse with 44M+ rows, serving 9 business domains, 1,200+ orders/day, and 100K+ rows/day across 4 marketplaces.",
        "Custom WMS that replaced the previous third-party system — interactive visual map, foreign staff trained in under 5 min, full stock audit trail.",
        "Real-time multi-user barcode scanning system with workflow enforcement.",
        "AI-assisted invoice processing that reads Thai e-commerce PDF invoices and fills structured records automatically.",
        "Supporting utilities that feed and operate the data platform."
      ],
      projectBullets: [
        [
          "Ingests data from 8 sources (Shopee, Lazada, TikTok, Facebook, OMS, DHL, Flash Express, internal WMS/BSN) → 4-layer medallion architecture with dbt — replaces a daily manual download routine that one person used to do every morning.",
          "<strong>Bronze config web app</strong> (FastAPI + React) — self-service UI for file types, validation rules, and AI-assisted column analysis/mapping suggestions, so non-developers can onboard new sources safely.",
          "Serves <strong>FastAPI + React dashboards</strong> with real-time KPIs, price alerts, procurement tracking, and operational drilldowns — driving daily restock decisions for procurement.",
          "Runs automatically outside business hours via <strong>custom Airflow operators</strong> (Bronze, dbt), with <strong>180+ dbt data tests</strong>, source freshness checks, and Thai-language <strong>email + LINE notifications</strong> on failure or stale data."
        ],
        [
          "<strong>Replaced the previous WMS</strong> — staff (including foreign workers) now learn the system in under 5 minutes versus weeks of struggling with the prior tool, and stock movements are traceable for the first time (full audit trail per item).",
          "<strong>Interactive warehouse grid map</strong> (dynamic CSS Grid, server-side) with 4-level color-coded stock states (empty / low / ok / critical) that instantly surface low, incomplete, or misplaced slots — plus annotations for reserved zones and merged-cell support for pallet-sized locations.",
          "Real-time inventory tracking + 4-type task workflow (pick, count, move, receive) with task chat, mobile barcode/QR scanning, and bilingual UI (Thai/English, 736 i18n keys).",
          "19 granular permissions, 21 SQLAlchemy models, 62 Jinja2 templates — deployed on Azure App Service with GitHub Actions CI/CD."
        ],
        [
          "Processes concurrent-user scans with validation + duplicate detection, and enforces workflow sequences through job dependency rules (e.g., \"Release\" before \"Outbound\") — replaces manual logs kept on paper, Google Sheets, and Excel.",
          "Custom notifications + audio alerts, full scan history with filtering and Excel export."
        ],
        [
          "Uses Azure Document Intelligence to read invoice files and automatically populate fields such as invoice number, dates, amounts, vendor/customer, tax IDs, and VAT.",
          "Supports Thai-specific formats (Buddhist Era dates, Thai Baht, UTF-8), caches results by file hash, and exports completed records to branded Excel."
        ],
        [
          "<strong>App Pipeline</strong> — generic Excel/CSV → SQL Server ETL tool with Tkinter GUI + CLI, shipped as Windows installer (PyInstaller). Replace/Upsert, auto-schema, batch processing.",
          "<strong>Copy Files Utility</strong> — file-sync utility for staging raw files (any type) into landing folders. SQLite-backed dedup, CSV-configured mappings so non-devs update sources without code.",
          "<strong>DHL Webhook Receiver</strong> — Flask service that streams DHL tracking events into SQL Server (PyODBC) — minimal bridge from logistics to warehouse data.",
          "<strong>Web Scraper</strong> — Python scraper collecting flash-sale and return data from marketplaces, feeding the Bronze layer."
        ]
      ],
      sectionSkills: "Technical Skills",
      sectionEducation: "Education",
      university: "Udon Thani Rajabhat University",
      universityPeriod: "Coursework completed (Year 1)",
      universityDegree: "Mechatronics &amp; Robotics Engineering."
    },
    th: {
      documentTitle: "สหัสวรรษ ทองมา — Data Engineer",
      htmlLang: "th",
      name: "สหัสวรรษ ทองมา",
      roleTitle: "Data Engineer",
      location: "สมุทรสาคร, ประเทศไทย",
      tabs: ["Resume", "Projects"],
      sectionSummary: "เกี่ยวกับผม",
      summary: [
        "<strong>Data engineer สร้าง production data platforms</strong> — ประสบการณ์ 3+ ปี ในงาน data, BI และ operations ปัจจุบันออกแบบและดูแล <strong>data warehouse ขนาด 50 GB ประมวลผล 100K+ records/วัน</strong> ให้บริษัท e-commerce ยอดขาย 100M+ THB/เดือน — <strong>dbt, Airflow, Azure (ADF, App Service), Python, SQL Server, FastAPI/React</strong> สร้าง app และ dashboard ให้ user สายธุรกิจ (คลัง จัดซื้อ ผู้บริหาร)"
      ],
      sectionExperience: "ประสบการณ์ทำงาน",
      epPeriod: "พ.ค. 2025 – ปัจจุบัน",
      epRole: "Data Engineer &nbsp;·&nbsp; E-Commerce Data Platform",
      epBullets: [
        "<strong>สร้าง data warehouse ตัวแรกของบริษัท</strong> — แทน Google Sheets/Excel ที่กระจัดกระจาย ให้ทีม 25+ คน ประหยัดเวลารวมทั้งทีม <strong>25–75 ชั่วโมง/วัน</strong> (1–3 ชม./คน) และตัดปัญหา reconcile ตัวเลขข้าม spreadsheet",
        "<strong>ออกแบบโครงสร้าง data warehouse</strong> — 193 transformation steps ครอบคลุม 9 ด้านธุรกิจ ประมวลผล <strong>100K+ records/วัน</strong> จาก Shopee, Lazada, TikTok, Facebook ป้อน <strong>BI dashboards</strong> ให้ผู้บริหาร จัดซื้อ คลัง และ fulfillment",
        "<strong>สร้าง ETL pipelines</strong> ดึงไฟล์ export จาก marketplace และข้อมูลจากระบบภายใน (Power Automate portal automation, Python, Azure Data Factory 46 pipelines) พร้อม self-service config app ให้คนที่ไม่ใช่ developer เพิ่ม data source ใหม่ได้เอง",
        "<strong>เปลี่ยนระบบ WMS ที่ใช้ไม่ได้</strong> เป็นแอปที่เขียนเอง (WMS, barcode scanner, invoice OCR) — พี่พม่าในคลังเรียนไม่ถึง 5 นาทีก็ใช้ได้ รองรับ <strong>1,200+ orders/วัน</strong> พร้อมประวัติของทุกชิ้น",
        "<strong>ทำงานร่วมกับทีมจัดซื้อ คลัง และผู้บริหาร</strong> หา process ที่ automate ได้ แทนงานมือด้วย <strong>low-code / no-code และ AI</strong>"
      ],
      travelerRoles: [
        "Accountant / ERP &amp; BI Analyst"
      ],
      travelerBullets: [
        "<strong>วาง master data ใน Ecount ERP ตั้งแต่ 0</strong> — กำหนด chart of accounts, customer/vendor master และ SKU mapping standards",
        "สร้าง <strong>Power BI reports</strong> และใช้ <strong>Power Query</strong> reconcile ข้อมูล ERP ของ sales/finance — ทำให้ report รายเดือนตรงและ stable",
        "วาง product/SKU master data ระหว่าง marketplaces และ ERP ให้เป็น single source of truth ของ catalog"
      ],
      projectTaglines: [
        "Data platform เต็มระบบ — warehouse ขนาด 50 GB กว่า 44M แถว เสิร์ฟ 9 business domains รับ 1,200+ orders/วัน และ 100K+ rows/วัน จาก 4 marketplaces",
        "WMS ที่เขียนเองมาแทนระบบเดิม — แผนที่คลังกดดูของได้ คนงานต่างชาติเรียนไม่ถึง 5 นาทีก็ใช้ได้ track ของได้ทุกการเคลื่อนไหว",
        "ระบบ barcode scanner หลายคนใช้พร้อมกันแบบ real-time พร้อมบังคับลำดับงาน",
        "ระบบ AI อ่านไฟล์ invoice PDF ของ e-commerce ไทย แล้วลงข้อมูลเป็น structured records ให้อัตโนมัติ",
        "Utility ที่ feed และ operate data platform หลัก"
      ],
      projectBullets: [
        [
          "ดึง raw data จาก 8 sources (Shopee, Lazada, TikTok, Facebook, OMS, DHL, Flash Express, internal WMS/BSN) → medallion architecture 4 ชั้นด้วย dbt — แทนการที่เคยมีคนนั่งโหลดไฟล์ทุกเช้า",
          "<strong>Bronze config web app</strong> (FastAPI + React) — self-service UI สำหรับ file types, validation rules และ AI-assisted column analysis/mapping suggestions ทำให้คนที่ไม่ใช่ developer เพิ่ม source ใหม่ได้เองอย่างปลอดภัย",
          "Serve <strong>FastAPI + React dashboards</strong> — real-time KPIs, price alerts, procurement tracking และ operational drilldowns ที่ฝ่ายจัดซื้อใช้ตัดสินใจซื้อสินค้าทุกวัน",
          "รันอัตโนมัตินอกเวลาทำงานผ่าน <strong>custom Airflow operators</strong> (Bronze, dbt) พร้อม <strong>180+ dbt data tests</strong>, source freshness checks และ <strong>email + LINE notifications</strong> ภาษาไทย แจ้งเตือนเมื่อ pipeline fail หรือข้อมูลค้าง"
        ],
        [
          "<strong>มาแทนระบบ WMS เดิม</strong> — จากที่คนงาน (รวมคนงานต่างชาติ) ใช้ยากและสอนนาน เปลี่ยนเป็นเรียนไม่ถึง 5 นาทีก็ใช้ได้ และ track การเคลื่อนไหวของของทุกชิ้นได้ครบ ซึ่งเดิมทำไม่ได้เลย",
          "<strong>Interactive warehouse grid map</strong> (dynamic CSS Grid, server-side) แสดง stock 4 ระดับ (empty / low / ok / critical) เห็นจุดที่ของน้อย ไม่ครบ หรือวางไม่ตรงทันที — พร้อม annotation สำหรับ reserved zones และ merged-cell support",
          "Real-time inventory tracking + task workflow 4 ประเภท (pick, count, move, receive) พร้อม task chat, mobile barcode/QR scanning และ bilingual UI (ไทย/อังกฤษ, 736 i18n keys)",
          "19 granular permissions, 21 SQLAlchemy models, 62 Jinja2 templates — deploy บน Azure App Service ด้วย GitHub Actions CI/CD"
        ],
        [
          "ประมวลผล scan พร้อม validation + duplicate detection สำหรับ concurrent users และ enforce workflow ด้วย job dependency rules (เช่น \"Release\" ก่อน \"Outbound\") — แทนการจดมือในกระดาษ Google Sheet และ Excel",
          "Custom notifications + audio alerts, เก็บ scan history ครบพร้อม filter และ Excel export"
        ],
        [
          "ใช้ Azure Document Intelligence อ่านไฟล์ invoice แล้วกรอกข้อมูลให้อัตโนมัติ เช่น เลข invoice, วันที่, ยอดเงิน, vendor/customer, เลขผู้เสียภาษี และ VAT",
          "รองรับ format เฉพาะไทย (พ.ศ., เงินบาท, UTF-8), cache ด้วย file hash เพื่อลดการเรียก API ซ้ำ และ export records ที่ลงข้อมูลแล้วเป็น Excel พร้อม branding"
        ],
        [
          "<strong>App Pipeline</strong> — ETL tool โหลด Excel/CSV → SQL Server พร้อม Tkinter GUI + CLI, แพ็คเป็น Windows installer (PyInstaller) รองรับ Replace/Upsert, auto-schema, batch processing",
          "<strong>Copy Files Utility</strong> — file-sync utility สำหรับ stage raw files ทุกประเภทเข้า landing folder พร้อม SQLite dedup, config ผ่าน CSV ไฟล์เดียวให้ user แก้ mapping ได้เอง",
          "<strong>DHL Webhook Receiver</strong> — Flask service ที่ stream DHL tracking events เข้า SQL Server (PyODBC) — bridge ระหว่าง logistics กับ warehouse data",
          "<strong>Web Scraper</strong> — Python scraper เก็บ flash-sale และ return data จาก marketplaces ส่งเข้า Bronze layer"
        ]
      ],
      sectionSkills: "ทักษะทางเทคนิค",
      sectionEducation: "การศึกษา",
      university: "มหาวิทยาลัยราชภัฏอุดรธานี",
      universityPeriod: "เรียนครบรายวิชาชั้นปีที่ 1",
      universityDegree: "วิศวกรรมเมคคาทรอนิกส์และหุ่นยนต์"
    }
  };
